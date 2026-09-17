"""The mock venue: acks, fills, rejects, IOC cancels and a random-walk quote board.

Pure python and deterministic for a seed. :meth:`MockVenue.step` does one round over
the orders a venue holds and one quote tick; ``simulator.py`` on the server calls it on
a timer, the tests call it directly and assert on the :class:`StepReport` it returns.

Fill rules (doc 13 §4 "venue only"):

* ``ROUTED`` -> ``ACK`` (``WORKING``), except at ``DARK`` where a small share is rejected.
* ``WORKING`` / ``PARTIAL``: a fill with a per-venue probability (``MKT`` almost always).
  A ``LMT`` order fills only when the market is at or through its limit -- a BUY needs
  ``ask <= limit`` and fills at the ask, a SELL/SHORT needs ``bid >= limit`` and fills at
  the bid -- never through the limit.
* ``IOC``: one attempt after the ack, then the remainder is cancelled by the venue.
* Fill size: a round lot between 15 % and 60 % of the leaves, or everything when fewer
  than 200 shares remain.
"""

from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Mapping, Optional, Tuple

from basket_oms_demo.core import OmsCore, OmsError, Order, Quote

__all__ = ["StepReport", "MockVenue", "FILL_PROBABILITY", "DEFAULT_FILL_PROBABILITY"]

#: Probability per step that a WORKING/PARTIAL order gets (another) fill, by venue.
FILL_PROBABILITY: Mapping[str, float] = {
    "ALGO-VWAP": 0.35,
    "ALGO-TWAP": 0.35,
    "ALGO-POV": 0.4,
    "DARK": 0.3,
}
DEFAULT_FILL_PROBABILITY = 0.55
MARKET_FILL_PROBABILITY = 0.9
DARK_REJECT_PROBABILITY = 0.08
#: Per-step log-return sigma of the quote random walk, and the band around the reference.
QUOTE_SIGMA = 0.0008
QUOTE_BAND = 0.03


@dataclass
class StepReport:
    acks: List[str] = field(default_factory=list)
    rejects: List[str] = field(default_factory=list)
    fills: List[Tuple[str, int, float]] = field(default_factory=list)
    cancels: List[str] = field(default_factory=list)
    quotes: int = 0

    @property
    def actions(self) -> int:
        return len(self.acks) + len(self.rejects) + len(self.fills) + len(self.cancels)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class MockVenue:
    """See the module docstring.

    Args:
        core: the order core the venue acts on (through its public venue-side methods).
        ref_prices: symbol -> reference price for the quote board; unknown symbols get 100.
        seed: for the venue's own RNG (independent of the seeder's).
        clock: for the quotes' ``Updated``; defaults to the core's clock.
    """

    def __init__(
        self,
        core: OmsCore,
        ref_prices: Mapping[str, float],
        seed: int = 42,
        clock: Optional[Callable[[], dt.datetime]] = None,
    ) -> None:
        self.core = core
        self.rng = random.Random(seed)
        self.clock = clock or core.clock
        self.quotes: Dict[str, Quote] = {}
        self.ioc_tried: set = set()
        for symbol, ref in ref_prices.items():
            self.quotes[symbol] = self._make_quote(symbol, float(ref), float(ref))

    # -- quotes -------------------------------------------------------------------------

    def _make_quote(self, symbol: str, last: float, ref: float) -> Quote:
        spread = max(0.01, round(last * 0.0004, 2))
        bid = round(last - spread / 2.0, 2)
        ask = round(bid + spread, 2)
        chg = round((last / ref - 1.0) * 100.0, 3) if ref else 0.0
        return Quote(symbol, bid, ask, round(last, 2), ref, chg, self.clock())

    def quote(self, symbol: str) -> Quote:
        """The current quote; an unknown symbol is added at a reference price of 100."""
        symbol = symbol.upper()
        if symbol not in self.quotes:
            self.quotes[symbol] = self._make_quote(symbol, 100.0, 100.0)
        return self.quotes[symbol]

    def tick_quotes(self) -> int:
        """One random-walk step for every quote; publishes each through the core."""
        for symbol in sorted(self.quotes):
            old = self.quotes[symbol]
            last = old.last * (1.0 + self.rng.gauss(0.0, QUOTE_SIGMA))
            low, high = old.ref_px * (1 - QUOTE_BAND), old.ref_px * (1 + QUOTE_BAND)
            last = min(max(last, low), high)
            quote = self._make_quote(symbol, last, old.ref_px)
            self.quotes[symbol] = quote
            self.core.publish_quote(quote)
        return len(self.quotes)

    def publish_quotes(self) -> None:
        for symbol in sorted(self.quotes):
            self.core.publish_quote(self.quotes[symbol])

    # -- fills --------------------------------------------------------------------------

    def _fill_price(self, order: Order) -> Optional[float]:
        """The price this order would fill at right now, or ``None`` if it cannot."""
        quote = self.quote(order.symbol)
        buying = order.side == "BUY"
        if order.ord_type == "MKT":
            return quote.ask if buying else quote.bid
        limit = order.limit_px or 0.0
        if buying:
            return quote.ask if quote.ask <= limit else None
        return quote.bid if quote.bid >= limit else None

    def _fill_qty(self, order: Order) -> int:
        if order.leaves <= 200:
            return order.leaves
        lot = int(round(order.leaves * self.rng.uniform(0.15, 0.6) / 100.0)) * 100
        return max(100, min(order.leaves, lot))

    def _fill_probability(self, order: Order) -> float:
        if order.ord_type == "MKT":
            return MARKET_FILL_PROBABILITY
        return FILL_PROBABILITY.get(order.venue or "", DEFAULT_FILL_PROBABILITY)

    def step(self) -> StepReport:
        """One round: quotes tick, ROUTED orders ack (or reject), working orders may fill."""
        report = StepReport()
        report.quotes = self.tick_quotes()
        for order in self.core.live_orders():
            try:
                if order.status == "ROUTED":
                    if order.venue == "DARK" and self.rng.random() < DARK_REJECT_PROBABILITY:
                        self.core.reject(order.order_id, "DARK: no liquidity")
                        report.rejects.append(order.order_id)
                    else:
                        self.core.ack(order.order_id)
                        report.acks.append(order.order_id)
                    continue
                # WORKING / PARTIAL
                ioc = order.tif == "IOC"
                if ioc and order.order_id in self.ioc_tried:
                    continue
                if ioc or self.rng.random() < self._fill_probability(order):
                    px = self._fill_price(order)
                    if px is not None:
                        qty = self._fill_qty(order)
                        filled, _ = self.core.fill(order.order_id, qty, px)
                        report.fills.append((order.order_id, qty, px))
                        order = filled
                if ioc:
                    self.ioc_tried.add(order.order_id)
                    if order.leaves > 0 and order.status in ("WORKING", "PARTIAL"):
                        self.core.venue_cancel(order.order_id, "IOC: remainder cancelled")
                        report.cancels.append(order.order_id)
            except OmsError:
                # The trader acted on the order between the snapshot and this step
                # (cancelled it, say). Nothing to do: the next step sees the new state.
                continue
        return report
