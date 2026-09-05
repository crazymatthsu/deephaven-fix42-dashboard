"""Deterministic mock market data: one-minute OHLC bars, written as parquet (doc 11 section 3).

Design goals, each pinned by a unit test:

* **Deterministic per (seed, symbol, day).** Regenerating a single day's file yields the
  identical file, whatever else was generated before -- so a partially populated store
  can be topped up and a corrupted file re-created without touching its neighbours.
  Every random stream is seeded from a SHA-256 of ``seed|symbol|day`` (never python's
  salted ``hash``).
* **Continuous across days** without replaying history. A cheap *daily* walk from a fixed
  anchor date gives each day its open and close; the *intraday* path is a Brownian
  bridge between those two, so tomorrow's open sits an overnight gap away from today's
  close and a multi-day chart looks like one instrument, not 20 unrelated ones.
* **Well-formed bars.** ``Low <= min(Open, Close) <= max(Open, Close) <= High``, each bar
  opens at the previous close, volumes are positive with the familiar U-shaped intraday
  profile, and every session is exactly 390 bars (09:30-15:59 New York, weekdays only).

Only :func:`bars_to_arrow` / :func:`write_day_file` / :func:`generate` touch pyarrow; the
bar arithmetic is plain python so the property tests run without it.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import math
import os
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from market_data_demo.layout import (
    ParquetRef,
    normalize_symbol,
    parse_symbols,
    relative_path,
    trading_days,
)

__all__ = [
    "ANCHOR_DATE",
    "BARS_PER_DAY",
    "COLUMNS",
    "DEFAULT_SEED",
    "DEFAULT_SYMBOLS",
    "SESSION_OPEN",
    "SESSION_CLOSE",
    "DayBars",
    "GenerateReport",
    "us_eastern_offset",
    "session_timestamps",
    "daily_open_close",
    "reference_price",
    "resolve_universe",
    "generate_day_bars",
    "bars_to_arrow",
    "write_day_file",
    "generate",
]

#: Regular US equity session, New York time; one bar per minute -> 390 bars.
SESSION_OPEN = dt.time(9, 30)
SESSION_CLOSE = dt.time(16, 0)
BARS_PER_DAY = 390

#: The daily walk starts here for every symbol; earlier days are not generated.
ANCHOR_DATE = dt.date(2020, 1, 1)

#: Seed used when none is given (CLI default and the compose demo).
DEFAULT_SEED = 42

#: Demo universe with reference prices at :data:`ANCHOR_DATE`.
DEFAULT_SYMBOLS: Dict[str, float] = {
    "AAPL": 190.0,
    "MSFT": 420.0,
    "NVDA": 120.0,
    "AMZN": 180.0,
    "GOOGL": 165.0,
    "META": 500.0,
    "TSLA": 250.0,
    "JPM": 200.0,
}

#: Column order of every file (and of every table the reader produces).
COLUMNS = ("Timestamp", "Symbol", "Open", "High", "Low", "Close", "Volume", "VWAP", "TradeCount")

# Model parameters -- tuned for plausible-looking charts, not calibrated to anything.
_DAILY_DRIFT = 0.0003  # mean daily log-return
_DAILY_SIGMA = 0.012  # daily log-return stdev (~19% annualized)
_GAP_SIGMA = 0.004  # overnight gap stdev
_MINUTE_SIGMA = 0.0006  # per-minute bridge noise (~1.2% over a session)
_WICK_SIGMA = 0.0004  # high/low extension beyond the body
_VOLUME_SIGMA = 0.35  # lognormal noise on the volume profile
_BASE_DAILY_NOTIONAL = 400_000_000.0  # shares/day = notional / reference price
_MIN_PRICE = 0.5
_PRICE_DECIMALS = 2


@dataclass
class DayBars:
    """One symbol-day of bars, columnar, in :data:`COLUMNS` order."""

    symbol: str
    day: dt.date
    columns: Dict[str, List[Any]] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.columns.get("Timestamp", ()))

    def row(self, index: int) -> Dict[str, Any]:
        """The ``index``-th bar as a dict (test convenience)."""
        return {name: self.columns[name][index] for name in COLUMNS}


@dataclass
class GenerateReport:
    """What :func:`generate` did."""

    root: str
    symbols: List[str]
    days: List[dt.date]
    written: List[ParquetRef] = field(default_factory=list)
    skipped: List[ParquetRef] = field(default_factory=list)

    @property
    def files(self) -> int:
        return len(self.written) + len(self.skipped)


# --------------------------------------------------------------------------------------
# Time
# --------------------------------------------------------------------------------------


def _nth_sunday(year: int, month: int, n: int) -> dt.date:
    first = dt.date(year, month, 1)
    offset = (6 - first.weekday()) % 7  # days until the first Sunday
    return first + dt.timedelta(days=offset + 7 * (n - 1))


def us_eastern_offset(day: dt.date) -> dt.timedelta:
    """UTC offset of New York on ``day``: -4h in daylight time, -5h otherwise.

    US DST runs from the second Sunday of March to the first Sunday of November (the
    2007 rule). Decided at day granularity, which is exact for a 09:30 session start.
    Implemented directly so the generator needs no tz database on the host.
    """
    dst_start = _nth_sunday(day.year, 3, 2)
    dst_end = _nth_sunday(day.year, 11, 1)
    if dst_start <= day < dst_end:
        return dt.timedelta(hours=-4)
    return dt.timedelta(hours=-5)


def session_timestamps(day: dt.date) -> List[dt.datetime]:
    """The 390 bar-start instants of ``day``'s regular session, as aware UTC datetimes."""
    local_open = dt.datetime.combine(day, SESSION_OPEN)
    utc_open = (local_open - us_eastern_offset(day)).replace(tzinfo=dt.timezone.utc)
    return [utc_open + dt.timedelta(minutes=index) for index in range(BARS_PER_DAY)]


# --------------------------------------------------------------------------------------
# Randomness and the price model
# --------------------------------------------------------------------------------------


def _rng(seed: int, symbol: str, day: dt.date, stream: str) -> random.Random:
    """A ``random.Random`` seeded from a stable digest of its coordinates."""
    key = f"{int(seed)}|{symbol}|{day.isoformat()}|{stream}".encode()
    return random.Random(int.from_bytes(hashlib.sha256(key).digest()[:8], "big"))


def reference_price(symbol: str, universe: Optional[Mapping[str, float]] = None) -> float:
    """The anchor-date price of ``symbol``.

    Known symbols come from ``universe`` (default :data:`DEFAULT_SYMBOLS`); anything else
    gets a stable pseudo-random price in ``[20, 500)`` derived from its name, so an
    arbitrary ticker typed into the CLI produces sensible, reproducible data.
    """
    symbol = normalize_symbol(symbol)
    table = DEFAULT_SYMBOLS if universe is None else universe
    if symbol in table:
        return float(table[symbol])
    digest = hashlib.sha256(f"reference|{symbol}".encode()).digest()
    return 20.0 + (int.from_bytes(digest[:8], "big") % 480_000) / 1000.0


def resolve_universe(symbols: Union[None, str, Iterable[str]]) -> Dict[str, float]:
    """``{SYMBOL: reference price}`` for the requested tickers (default: the demo universe)."""
    names = parse_symbols(symbols)
    if not names:
        return dict(DEFAULT_SYMBOLS)
    return {name: reference_price(name) for name in names}


def daily_open_close(
    symbol: str, day: dt.date, seed: int, reference: Optional[float] = None
) -> tuple:
    """The daily walk: ``(open, close)`` of ``symbol`` on ``day``.

    Walks weekday by weekday from :data:`ANCHOR_DATE`: ``close = open * exp(r)`` with
    ``r ~ N(drift, sigma)`` and ``next open = close * exp(g)`` with ``g ~ N(0, gap)``.
    Roughly 260 iterations per year -- cheap enough to redo for every file, which is
    what makes each file independent of every other.
    """
    symbol = normalize_symbol(symbol)
    price = reference_price(symbol) if reference is None else float(reference)
    if day < ANCHOR_DATE:
        raise ValueError(f"{day} is before the anchor date {ANCHOR_DATE}")
    current = ANCHOR_DATE
    day_open = price
    day_close = price
    while current <= day:
        if current.weekday() < 5:
            rng = _rng(seed, symbol, current, "daily")
            day_open = max(_MIN_PRICE, price)
            day_close = max(_MIN_PRICE, day_open * math.exp(rng.gauss(_DAILY_DRIFT, _DAILY_SIGMA)))
            price = day_close * math.exp(rng.gauss(0.0, _GAP_SIGMA))
        current += dt.timedelta(days=1)
    return (day_open, day_close)


def _u_shape(index: int, count: int) -> float:
    """Intraday volume profile: heavy at the open and close, light at lunch (mean ~1)."""
    x = (index + 0.5) / count  # (0, 1)
    return 0.55 + 1.8 * (x - 0.5) ** 2 * 4 / 2  # 0.55 at midday, ~1.45 at the edges


def _round_price(value: float) -> float:
    return round(max(_MIN_PRICE, value), _PRICE_DECIMALS)


def generate_day_bars(
    symbol: str,
    day: dt.date,
    seed: int = DEFAULT_SEED,
    reference: Optional[float] = None,
) -> DayBars:
    """Generate the 390 one-minute bars of ``symbol`` on ``day`` (a weekday).

    Raises:
        ValueError: On a weekend day -- there is no session to generate.
    """
    symbol = normalize_symbol(symbol)
    if day.weekday() >= 5:
        raise ValueError(f"{day} is not a trading day (weekend)")
    ref = reference_price(symbol) if reference is None else float(reference)
    day_open, day_close = daily_open_close(symbol, day, seed, ref)
    rng = _rng(seed, symbol, day, "intraday")
    timestamps = session_timestamps(day)
    n = BARS_PER_DAY

    # Brownian bridge in log space from log(open) to log(close) over n steps.
    steps = [rng.gauss(0.0, _MINUTE_SIGMA) for _ in range(n)]
    walk = [0.0]
    for step in steps:
        walk.append(walk[-1] + step)
    total = walk[-1]
    log_open, log_close = math.log(day_open), math.log(day_close)
    path = [
        log_open + (log_close - log_open) * (k / n) + (walk[k] - total * (k / n))
        for k in range(n + 1)
    ]
    prices = [_round_price(math.exp(value)) for value in path]

    daily_shares = _BASE_DAILY_NOTIONAL / max(ref, 1.0)
    base_minute_volume = max(100.0, daily_shares / n)

    opens: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    volumes: List[int] = []
    vwaps: List[float] = []
    trades: List[int] = []
    for k in range(n):
        bar_open, bar_close = prices[k], prices[k + 1]
        body_high, body_low = max(bar_open, bar_close), min(bar_open, bar_close)
        high = _round_price(body_high * (1.0 + abs(rng.gauss(0.0, _WICK_SIGMA))))
        low = _round_price(body_low * (1.0 - abs(rng.gauss(0.0, _WICK_SIGMA))))
        high = max(high, body_high)
        low = min(low, body_low)
        volume = int(round(base_minute_volume * _u_shape(k, n) * math.exp(rng.gauss(0.0, _VOLUME_SIGMA))))
        volume = max(volume, 1)
        vwap = round((bar_open + high + low + bar_close) / 4.0, 4)
        vwap = min(max(vwap, low), high)
        trade_count = max(1, int(volume / max(50.0, rng.uniform(60.0, 180.0))))
        opens.append(bar_open)
        highs.append(high)
        lows.append(low)
        closes.append(bar_close)
        volumes.append(volume)
        vwaps.append(vwap)
        trades.append(trade_count)

    columns: Dict[str, List[Any]] = {
        "Timestamp": timestamps,
        "Symbol": [symbol] * n,
        "Open": opens,
        "High": highs,
        "Low": lows,
        "Close": closes,
        "Volume": volumes,
        "VWAP": vwaps,
        "TradeCount": trades,
    }
    return DayBars(symbol=symbol, day=day, columns=columns)


# --------------------------------------------------------------------------------------
# Parquet
# --------------------------------------------------------------------------------------


def arrow_schema() -> Any:
    """The parquet schema every file is written with.

    ``Timestamp`` is a UTC-adjusted microsecond timestamp (``isAdjustedToUTC=true``),
    which Deephaven reads as an ``Instant`` column and every other engine reads as an
    ordinary timestamp; prices are ``double``, counts ``int64``.
    """
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("Timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("Symbol", pa.string(), nullable=False),
            pa.field("Open", pa.float64()),
            pa.field("High", pa.float64()),
            pa.field("Low", pa.float64()),
            pa.field("Close", pa.float64()),
            pa.field("Volume", pa.int64()),
            pa.field("VWAP", pa.float64()),
            pa.field("TradeCount", pa.int64()),
        ]
    )


def bars_to_arrow(bars: DayBars) -> Any:
    """Convert :class:`DayBars` to a ``pyarrow.Table`` with :func:`arrow_schema`."""
    import pyarrow as pa

    schema = arrow_schema()
    arrays = [pa.array(bars.columns[field.name], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def write_day_file(
    root: Union[str, os.PathLike],
    symbol: str,
    day: dt.date,
    seed: int = DEFAULT_SEED,
    reference: Optional[float] = None,
    overwrite: bool = True,
) -> ParquetRef:
    """Generate one symbol-day and write it to ``<root>/YYYY/MM/DD/<SYMBOL>.parquet``.

    The file is written to a temporary sibling and renamed into place, so a reader (or
    a Deephaven server watching the directory) never sees a half-written file.

    Returns:
        The :class:`ParquetRef` of the file (``path`` absolute), whether it was written
        or -- with ``overwrite=False`` -- found already present.
    """
    import pyarrow.parquet as pq

    root_path = Path(root)
    rel = relative_path(day, symbol)
    target = root_path / rel
    ref = ParquetRef(day=day, symbol=normalize_symbol(symbol), path=str(target.resolve()))
    if target.exists() and not overwrite:
        return ref
    target.parent.mkdir(parents=True, exist_ok=True)
    table = bars_to_arrow(generate_day_bars(symbol, day, seed, reference))
    tmp = target.with_name(target.name + ".tmp")
    pq.write_table(table, str(tmp), compression="snappy")
    os.replace(str(tmp), str(target))
    return ref


def generate(
    root: Union[str, os.PathLike],
    symbols: Union[None, str, Iterable[str], Mapping[str, float]] = None,
    start: Optional[dt.date] = None,
    end: Optional[dt.date] = None,
    seed: int = DEFAULT_SEED,
    force: bool = False,
    progress: Optional[Callable[[ParquetRef, bool], None]] = None,
) -> GenerateReport:
    """Populate ``root`` with every weekday in ``[start, end]`` for every symbol.

    Args:
        root: Local directory (created if missing).
        symbols: Tickers or ``{ticker: reference price}``; default the demo universe.
        start: First day; defaults to 30 calendar days before ``end``.
        end: Last day; defaults to yesterday (the "historical" in historical data).
        seed: Generator seed -- same seed, same files.
        force: Rewrite files that already exist (otherwise they are skipped).
        progress: Optional callback ``(ref, written)`` per file.
    """
    if isinstance(symbols, Mapping):
        universe: Dict[str, float] = {normalize_symbol(k): float(v) for k, v in symbols.items()}
    else:
        universe = resolve_universe(symbols)
    end_day = end or (dt.date.today() - dt.timedelta(days=1))
    start_day = start or (end_day - dt.timedelta(days=30))
    if end_day < start_day:
        raise ValueError(f"end {end_day} is before start {start_day}")
    days = trading_days(start_day, end_day)
    report = GenerateReport(root=str(Path(root).resolve()), symbols=sorted(universe), days=days)
    for day in days:
        for symbol, ref_price in universe.items():
            target = Path(root) / relative_path(day, symbol)
            existed = target.exists()
            ref = write_day_file(root, symbol, day, seed=seed, reference=ref_price, overwrite=force or not existed)
            written = force or not existed
            (report.written if written else report.skipped).append(ref)
            if progress is not None:
                progress(ref, written)
    return report
