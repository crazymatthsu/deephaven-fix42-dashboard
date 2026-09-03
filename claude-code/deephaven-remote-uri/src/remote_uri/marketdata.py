"""Simulated market data: universe parsing and a seeded random walk -- doc 10 section 6.

Pure stdlib and deliberately free of Deephaven: the walk is ordinary python state
(``{symbol: price}``), and :mod:`remote_uri.marketdata_table` is the only thing that
knows how to render it as a table.  That split is what keeps the quote generator
unit-testable *and* what keeps stateful python out of ``update_view`` -- the table
is a ``function_generated_table`` snapshot, so the engine never calls back into this
class from a formula (doc 10 section 6).

Properties the tests pin:

* **deterministic per seed** -- two walks with the same seed and the same universe
  produce the same quotes step for step, which is what makes the e2e's
  ``OpenNotional`` assertion reproducible;
* **bounded steps** -- each tick moves a price by at most
  :attr:`MarketDataWalk.sigma_bps` basis points, so a demo cannot wander to zero or
  to infinity;
* **bid < mid < ask** for any positive half-spread;
* **unseen symbols self-seed** -- a symbol that shows up in the order flow but is
  not in ``REMOTEURI_MD_SYMBOLS`` is added with its first non-null ``Price``, or
  :data:`DEFAULT_REFERENCE_PRICE` when it has none.
"""

from __future__ import annotations

import math
import random
import threading
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

__all__ = [
    "DEFAULT_REFERENCE_PRICE",
    "DEFAULT_SIGMA_BPS",
    "MIN_PRICE",
    "PRICE_DECIMALS",
    "QuoteRow",
    "parse_universe",
    "MarketDataWalk",
    "seed_from_rows",
]

#: Reference price for a symbol seen in the order flow with no usable ``Price``.
DEFAULT_REFERENCE_PRICE = 100.0

#: Per-tick move bound, in basis points of the current price.
DEFAULT_SIGMA_BPS = 10.0

#: Prices are clamped here so a long walk cannot reach zero (and make a spread vanish).
MIN_PRICE = 0.01

#: Quotes are rounded to this many decimals -- enough that a 5 bp half-spread on a
#: cent-priced symbol still separates bid, mid and ask.
PRICE_DECIMALS = 6

#: One row of :meth:`MarketDataWalk.snapshot_rows`: ``(Symbol, Bid, Ask, Mid)``.
QuoteRow = Tuple[str, float, float, float]


def parse_universe(raw: Optional[str], env_name: str = "REMOTEURI_MD_SYMBOLS") -> Dict[str, float]:
    """Parse a ``SYMBOL:reference price,...`` universe.

    Args:
        raw: The configured text; ``None``/blank yields an empty dict (the caller
            substitutes its default -- this function never invents a universe).
        env_name: Variable named in error messages.

    Returns:
        ``{symbol: reference price}`` in configuration order.

    Raises:
        ValueError: On an entry without a price, a non-numeric or non-positive
            price, or a duplicated symbol. A silently-dropped symbol would mark its
            orders at their limit price and quietly change the exposure numbers.
    """
    text = (raw or "").strip()
    universe: Dict[str, float] = {}
    if not text:
        return universe
    for chunk in text.replace(";", ",").split(","):
        entry = chunk.strip()
        if not entry:
            continue
        symbol, sep, price_text = entry.partition(":")
        symbol = symbol.strip().upper()
        if not symbol:
            raise ValueError(f"{env_name}: entry {entry!r} has no symbol; expected 'AAPL:190'")
        if not sep:
            raise ValueError(
                f"{env_name}: entry {entry!r} has no reference price; expected 'AAPL:190'"
            )
        try:
            price = float(price_text.strip())
        except ValueError:
            raise ValueError(
                f"{env_name}: symbol {symbol!r} has price {price_text.strip()!r}, "
                "which is not a number; expected e.g. 'AAPL:190'"
            ) from None
        if not math.isfinite(price) or price <= 0:
            raise ValueError(
                f"{env_name}: symbol {symbol!r} has price {price}; a reference price "
                "must be finite and greater than zero"
            )
        if symbol in universe:
            raise ValueError(
                f"{env_name}: symbol {symbol!r} is listed more than once; "
                "each symbol needs exactly one reference price"
            )
        universe[symbol] = price
    return universe


class MarketDataWalk:
    """A seeded, bounded random walk over ``{symbol: mid}``.

    Thread-safe by construction: :meth:`step` runs on the update-graph thread (the
    ``function_generated_table`` refresh) while :meth:`ensure` runs on whatever
    thread the ``orders_all`` listener fires on, so every mutation takes the lock.

    A plain class rather than a ``dataclass``: Application Mode may exec the app
    file under a ``__name__`` that is not in ``sys.modules``, which breaks
    ``dataclasses``' type introspection at class-creation time.
    """

    def __init__(
        self,
        universe: Optional[Mapping[str, float]] = None,
        seed: int = 42,
        spread_bps: float = 5.0,
        sigma_bps: float = DEFAULT_SIGMA_BPS,
    ) -> None:
        """Seed the walk.

        Args:
            universe: ``{symbol: reference price}``; the starting mids.
            seed: ``random.Random`` seed -- the whole point of the knob is that a
                given seed replays the same quotes.
            spread_bps: Half-spread around the mid, in basis points.
            sigma_bps: Maximum per-tick move, in basis points of the current price.

        Raises:
            ValueError: If ``spread_bps`` or ``sigma_bps`` is negative or not finite.
        """
        for label, value in (("spread_bps", spread_bps), ("sigma_bps", sigma_bps)):
            if not math.isfinite(float(value)) or float(value) < 0:
                raise ValueError(f"{label}={value!r} must be a finite, non-negative number")
        self.spread_bps = float(spread_bps)
        self.sigma_bps = float(sigma_bps)
        self.seed = int(seed)
        self._rng = random.Random(self.seed)
        self._lock = threading.Lock()
        self._prices: Dict[str, float] = {}
        # Symbols seeded at DEFAULT_REFERENCE_PRICE because their first sighting
        # carried no usable Price; the first real one still gets to replace it.
        self._defaulted: set = set()
        self.steps = 0
        self.added = 0
        for symbol, price in (universe or {}).items():
            self._prices[str(symbol).upper()] = _clamp(float(price))

    # -- accessors ---------------------------------------------------------------

    @property
    def symbols(self) -> Tuple[str, ...]:
        """Every known symbol, sorted -- the row order of a snapshot."""
        with self._lock:
            return tuple(sorted(self._prices))

    @property
    def prices(self) -> Dict[str, float]:
        """A copy of the current mids."""
        with self._lock:
            return dict(self._prices)

    def __len__(self) -> int:
        """Number of symbols currently quoted."""
        with self._lock:
            return len(self._prices)

    def __contains__(self, symbol: object) -> bool:
        """True when ``symbol`` is quoted."""
        with self._lock:
            return str(symbol).upper() in self._prices

    # -- mutation ----------------------------------------------------------------

    def ensure(self, symbol: Any, reference: Any = None) -> bool:
        """Add ``symbol`` to the universe if it is not quoted yet.

        Doc 10 section 4.3: "symbols seen in orders but not listed are added with
        their first non-null ``Price`` (else ``100.0``)". A symbol whose first
        sighting carried no price is seeded at :data:`DEFAULT_REFERENCE_PRICE` and
        *upgraded* the first time a real price turns up, so a market order arriving
        before a limit order does not pin the symbol at 100 forever.

        Args:
            symbol: The symbol from the order flow.
            reference: That order's ``Price``; ``None``/``0`` when it has none.

        Returns:
            True when the universe changed (added or upgraded).
        """
        name = str(symbol or "").strip().upper()
        if not name:
            return False
        price = _positive(reference)
        with self._lock:
            if name not in self._prices:
                self._prices[name] = _clamp(price if price is not None else DEFAULT_REFERENCE_PRICE)
                if price is None:
                    self._defaulted.add(name)
                self.added += 1
                return True
            if price is not None and name in self._defaulted:
                self._prices[name] = _clamp(price)
                self._defaulted.discard(name)
                return True
            return False

    def step(self) -> List[QuoteRow]:
        """Advance every price one bounded tick and return the new snapshot.

        Returns:
            :meth:`snapshot_rows` for the post-step state (sorted by symbol).
        """
        with self._lock:
            self.steps += 1
            for symbol in sorted(self._prices):
                move = self._rng.uniform(-1.0, 1.0) * (self.sigma_bps / 10_000.0)
                self._prices[symbol] = _clamp(self._prices[symbol] * (1.0 + move))
            return self._rows_locked()

    def snapshot_rows(self) -> List[QuoteRow]:
        """Current quotes as ``(Symbol, Bid, Ask, Mid)`` rows, sorted by symbol."""
        with self._lock:
            return self._rows_locked()

    # -- internals ---------------------------------------------------------------

    def _rows_locked(self) -> List[QuoteRow]:
        """Build the snapshot; the caller holds the lock."""
        half = self.spread_bps / 10_000.0
        rows: List[QuoteRow] = []
        for symbol in sorted(self._prices):
            mid = round(self._prices[symbol], PRICE_DECIMALS)
            bid = round(mid * (1.0 - half), PRICE_DECIMALS)
            ask = round(mid * (1.0 + half), PRICE_DECIMALS)
            rows.append((symbol, bid, ask, mid))
        return rows

    def describe(self) -> str:
        """One-line summary for the startup banner."""
        return (
            f"{len(self)} symbols seed={self.seed} spread={self.spread_bps}bp "
            f"sigma={self.sigma_bps}bp"
        )


def _clamp(price: float) -> float:
    """Keep a price finite and above :data:`MIN_PRICE`."""
    if not math.isfinite(price) or price < MIN_PRICE:
        return MIN_PRICE
    return price


def _positive(value: Any) -> Optional[float]:
    """Return ``value`` as a positive finite float, or ``None``."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def seed_from_rows(walk: "MarketDataWalk", rows: Iterable[Mapping[str, Any]]) -> int:
    """Feed ``(Symbol, Price)`` row dicts through :meth:`MarketDataWalk.ensure`.

    Args:
        walk: The walk to extend.
        rows: Row-like mappings carrying ``Symbol`` and (optionally) ``Price``.

    Returns:
        How many of them changed the universe.
    """
    changed = 0
    for row in rows:
        if walk.ensure(row.get("Symbol"), row.get("Price")):
            changed += 1
    return changed
