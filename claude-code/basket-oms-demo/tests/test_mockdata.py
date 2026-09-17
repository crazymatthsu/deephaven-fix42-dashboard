"""The seeded demo: deterministic, consistent, and it tells the intended story."""

from __future__ import annotations

from basket_oms_demo.core import OmsCore
from basket_oms_demo.mockdata import UNIVERSE, ref_prices, seed_demo


def _seeded(n=5, seed=42):
    core = OmsCore(trader="seed")
    baskets = seed_demo(core, n_baskets=n, seed=seed)
    return core, baskets


def test_universe_has_unique_symbols_with_positive_prices():
    symbols = [s for s, _ in UNIVERSE]
    assert len(symbols) == len(set(symbols)) >= 20
    assert all(px > 0 for px in ref_prices().values())


def test_story_baskets_states():
    core, baskets = _seeded()
    by_name = {b.name: b for b in baskets}
    assert [b.basket_id for b in baskets] == ["B-0001", "B-0002", "B-0003", "B-0004", "B-0005"]
    assert by_name["Tech rebalance -- Sep"].status == "STAGED"
    assert by_name["Pairs NVDA / AMD"].status == "WORKING"
    assert by_name["Index arb -- S&P slice"].status == "WORKING"
    assert by_name["Energy unwind"].status == "DONE"
    assert by_name["Block -- UNH / V"].status == "WORKING"
    # The index-arb basket covers every state the context menu cares about.
    statuses = {o.status for o in core.orders_in(by_name["Index arb -- S&P slice"].basket_id)}
    assert {"FILLED", "PARTIAL", "WORKING", "ROUTED", "NEW"} <= statuses
    # The split demo has a SPLIT parent with three children, one of them working.
    block = core.orders_in(by_name["Block -- UNH / V"].basket_id)
    parents = [o for o in block if o.status == "SPLIT"]
    children = [o for o in block if o.parent_id]
    assert len(parents) == 1 and len(children) == 3 and sum(c.qty for c in children) == parents[0].qty
    assert any(c.status == "PARTIAL" for c in children)


def test_invariants_hold_for_every_seeded_order():
    core, _ = _seeded(n=9, seed=7)
    for order in core.orders.values():
        if order.status == "SPLIT":
            assert order.leaves == 0
        elif order.status in ("CANCELLED", "REJECTED"):
            assert order.leaves == 0 and order.filled <= order.qty
        else:
            assert order.filled + order.leaves == order.qty
        if order.ord_type == "MKT":
            assert order.limit_px is None
        else:
            assert order.limit_px and order.limit_px > 0
        if order.filled:
            assert order.avg_px and order.avg_px > 0
    assert core.counts()["baskets"] == 9
    assert all(b.name.startswith("Program") for b in list(core.baskets.values())[5:])


def test_seed_is_deterministic():
    a, _ = _seeded(n=8, seed=3)
    b, _ = _seeded(n=8, seed=3)
    assert [(o.order_id, o.symbol, o.qty, o.status, o.filled, o.avg_px) for o in a.orders.values()] == [
        (o.order_id, o.symbol, o.qty, o.status, o.filled, o.avg_px) for o in b.orders.values()
    ]
    c, _ = _seeded(n=8, seed=4)
    assert [o.symbol for o in a.orders.values()] != [o.symbol for o in c.orders.values()]


def test_zero_baskets_is_allowed():
    core, baskets = _seeded(n=0)
    assert baskets == [] and core.counts()["orders"] == 0
