"""The symbol universe and the seeded demo baskets (plan step 2). Pure python.

The seed tells a story on first open: a staged basket to route, a working pairs basket
that keeps filling, an index-arb basket in every state at once, a finished basket, and a
basket with a split parent so the tree panel is not empty. Every state is reached
through the core's own mutations (route / ack / fill / cancel / split), so the audit
tape and the execution tape are consistent with the orders they describe.
"""

from __future__ import annotations

import random
from typing import Dict, List, Mapping, Sequence, Tuple

from basket_oms_demo.core import Basket, OmsCore

__all__ = ["UNIVERSE", "ref_prices", "seed_demo", "STORY_BASKETS"]

#: (symbol, reference price) -- large caps, prices roughly where they traded in 2026.
UNIVERSE: Tuple[Tuple[str, float], ...] = (
    ("AAPL", 189.50),
    ("MSFT", 415.20),
    ("NVDA", 118.70),
    ("AMZN", 178.30),
    ("GOOGL", 165.40),
    ("META", 512.80),
    ("TSLA", 248.90),
    ("AMD", 156.80),
    ("NFLX", 645.20),
    ("JPM", 205.10),
    ("BAC", 41.30),
    ("GS", 468.50),
    ("XOM", 113.60),
    ("CVX", 152.40),
    ("COP", 108.90),
    ("WMT", 68.20),
    ("PG", 166.50),
    ("KO", 62.70),
    ("PEP", 172.10),
    ("JNJ", 158.30),
    ("UNH", 508.60),
    ("V", 275.40),
    ("MA", 458.90),
    ("HD", 356.70),
)


def ref_prices() -> Dict[str, float]:
    return {symbol: px for symbol, px in UNIVERSE}


def _px(ref: Mapping[str, float], symbol: str, offset_pct: float) -> str:
    return f"{ref[symbol] * (1 + offset_pct / 100.0):.2f}"


def _fill_px(rng: random.Random, ref: Mapping[str, float], symbol: str) -> float:
    return round(ref[symbol] * (1 + rng.uniform(-0.15, 0.15) / 100.0), 2)


# -- the five story baskets ------------------------------------------------------------


def _tech_rebalance(core: OmsCore, rng: random.Random, ref: Mapping[str, float]) -> Basket:
    """All NEW: the one the demo routes first."""
    lines = [
        f"AAPL BUY 5000 LMT {_px(ref, 'AAPL', -0.30)}",
        "MSFT BUY 2000 MKT",
        f"NVDA SELL 8000 LMT {_px(ref, 'NVDA', +0.25)}",
        "META BUY 1200 MKT",
        f"GOOGL SELL 3000 LMT {_px(ref, 'GOOGL', +0.40)} GTC",
        "AMZN BUY 2500 MKT",
    ]
    return core.create_basket("Tech rebalance -- Sep", lines, strategy="DMA", note="staged for the open")


def _pairs(core: OmsCore, rng: random.Random, ref: Mapping[str, float]) -> Basket:
    """Two legs at the VWAP algo, both partially filled and still working."""
    basket = core.create_basket(
        "Pairs NVDA / AMD",
        [f"NVDA BUY 6000 LMT {_px(ref, 'NVDA', +0.20)}", f"AMD SELL 9000 LMT {_px(ref, 'AMD', -0.20)}"],
        strategy="VWAP",
        note="long NVDA / short AMD, 40% done",
    )
    for order, pct in zip(core.orders_in(basket.basket_id), (0.35, 0.42)):
        core.route(order.order_id, "ALGO-VWAP")
        core.ack(order.order_id)
        done = 0
        target = int(order.qty * pct)
        while done < target:
            lot = min(target - done, rng.choice((300, 500, 700, 1000)))
            core.fill(order.order_id, lot, _fill_px(rng, ref, order.symbol))
            done += lot
    return basket


def _index_arb(core: OmsCore, rng: random.Random, ref: Mapping[str, float]) -> Basket:
    """Eight names in every state: filled, partial, working, routed-not-acked, unrouted."""
    lines = [
        "JPM BUY 3000 MKT",
        f"BAC BUY 15000 LMT {_px(ref, 'BAC', -0.10)}",
        "XOM SELL 4000 MKT",
        f"CVX SELL 2500 LMT {_px(ref, 'CVX', +0.15)}",
        "WMT BUY 6000 MKT IOC",
        f"PG BUY 1800 LMT {_px(ref, 'PG', -0.05)} GTC",
        "KO SELL 7000 MKT",
        f"PEP SELL 2200 LMT {_px(ref, 'PEP', +0.30)}",
    ]
    basket = core.create_basket("Index arb -- S&P slice", lines, strategy="TWAP", note="TWAP to the close")
    orders = core.orders_in(basket.basket_id)
    plan = [
        ("NYSE", 1.0),  # JPM filled
        ("ALGO-TWAP", 0.55),  # BAC partial
        ("ARCA", 1.0),  # XOM filled
        ("ALGO-TWAP", 0.20),  # CVX partial
        ("BATS", 0.0),  # WMT working, no fill yet
        ("NASDAQ", None),  # PG routed, not acked
        (None, None),  # KO unrouted
        ("DARK", 0.0),  # PEP working at the dark pool
    ]
    for order, (venue, pct) in zip(orders, plan):
        if venue is None:
            continue
        core.route(order.order_id, venue)
        if pct is None:
            continue
        core.ack(order.order_id)
        target = int(order.qty * pct)
        done = 0
        while done < target:
            lot = min(target - done, rng.choice((200, 500, 1000)))
            core.fill(order.order_id, lot, _fill_px(rng, ref, order.symbol))
            done += lot
    return basket


def _energy_unwind(core: OmsCore, rng: random.Random, ref: Mapping[str, float]) -> Basket:
    """Finished: two fills and a cancel -> DONE."""
    basket = core.create_basket(
        "Energy unwind",
        ["XOM SELL 2000 MKT", "CVX SELL 1500 MKT", f"COP SELL 3000 LMT {_px(ref, 'COP', +0.50)}"],
        strategy="DMA",
        note="done before lunch",
    )
    orders = core.orders_in(basket.basket_id)
    for order in orders[:2]:
        core.route(order.order_id, "NYSE")
        core.ack(order.order_id)
        core.fill(order.order_id, order.qty, _fill_px(rng, ref, order.symbol))
    core.route(orders[2].order_id, "ARCA")
    core.ack(orders[2].order_id)
    core.fill(orders[2].order_id, 500, _fill_px(rng, ref, "COP"))
    core.cancel(orders[2].order_id, reason="limit too far, pulled")
    return basket


def _split_demo(core: OmsCore, rng: random.Random, ref: Mapping[str, float]) -> Basket:
    """A block split into three slices, one slice already routed."""
    basket = core.create_basket(
        "Block -- UNH / V",
        [f"UNH BUY 30000 LMT {_px(ref, 'UNH', -0.20)} GTC", "V BUY 4000 MKT"],
        strategy="POV",
        note="work the UNH block in slices",
    )
    parent = core.orders_in(basket.basket_id)[0]
    children = core.split(parent.order_id, slices=3)
    core.route(children[0].order_id, "ALGO-VWAP")
    core.ack(children[0].order_id)
    core.fill(children[0].order_id, 2500, _fill_px(rng, ref, "UNH"))
    return basket


STORY_BASKETS = (_tech_rebalance, _pairs, _index_arb, _energy_unwind, _split_demo)


def _random_basket(core: OmsCore, rng: random.Random, ref: Mapping[str, float], index: int) -> Basket:
    symbols = rng.sample([s for s, _ in UNIVERSE], k=rng.randint(3, 6))
    lines: List[str] = []
    for symbol in symbols:
        side = rng.choice(("BUY", "BUY", "SELL"))
        qty = rng.choice((500, 1000, 1500, 2000, 2500, 4000))
        if rng.random() < 0.5:
            offset = rng.uniform(-0.4, 0.4)
            lines.append(f"{symbol} {side} {qty} LMT {_px(ref, symbol, offset)}")
        else:
            lines.append(f"{symbol} {side} {qty} MKT")
    basket = core.create_basket(f"Program {index}", lines, strategy=rng.choice(("DMA", "VWAP", "TWAP", "POV")))
    if rng.random() < 0.5:
        for order in core.orders_in(basket.basket_id):
            if rng.random() < 0.7:
                core.route(order.order_id, rng.choice(core.venues))
    return basket


def seed_demo(core: OmsCore, n_baskets: int = 5, seed: int = 42) -> List[Basket]:
    """Create ``n_baskets`` baskets: the story baskets first, random ones after."""
    rng = random.Random(seed)
    ref = ref_prices()
    baskets: List[Basket] = []
    for index in range(max(0, int(n_baskets))):
        if index < len(STORY_BASKETS):
            baskets.append(STORY_BASKETS[index](core, rng, ref))
        else:
            baskets.append(_random_basket(core, rng, ref, index + 1))
    return baskets
