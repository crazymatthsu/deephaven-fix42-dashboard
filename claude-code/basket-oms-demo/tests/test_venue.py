"""The mock venue: acks, limit discipline, IOC, rejects, determinism."""

from __future__ import annotations

import random

import pytest

from basket_oms_demo.core import OmsCore
from basket_oms_demo.venue import DARK_REJECT_PROBABILITY, MockVenue, StepReport


@pytest.fixture
def venue(core, listener):
    v = MockVenue(core, {"AAPL": 100.0, "MSFT": 400.0}, seed=1)
    return v


def _run(venue, steps):
    reports = [venue.step() for _ in range(steps)]
    return reports


def test_quotes_tick_and_publish(venue, listener):
    assert venue.quote("AAPL").bid < venue.quote("AAPL").ask
    n = venue.tick_quotes()
    assert n == 2 and len(listener.quotes) == 2
    q = venue.quote("AAPL")
    assert 97.0 <= q.last <= 103.0 and q.bid < q.ask and q.ref_px == 100.0
    assert venue.quote("NEWSYM").ref_px == 100.0  # unknown symbols appear on demand


def test_routed_orders_are_acked_then_market_orders_fill_completely(core, venue):
    core.create_basket("mkt", ["AAPL BUY 1000", "MSFT SELL 3000"])
    core.route_basket("B-0001", "NYSE")
    first = venue.step()
    assert sorted(first.acks) == ["O-000001", "O-000002"] and not first.fills
    reports = _run(venue, 40)
    assert core.order("O-000001").status == "FILLED" and core.order("O-000002").status == "FILLED"
    fills = [f for r in reports for f in r.fills]
    assert sum(q for oid, q, _ in fills if oid == "O-000001") == 1000
    # Every fill is a round lot or the remainder; BUYs pay the ask side (>= bid).
    assert all(q % 100 == 0 or q <= 200 for _, q, _ in fills)
    assert core.order("O-000001").avg_px == pytest.approx(100.0, abs=3.5)
    assert core.basket("B-0001").status == "DONE"


def test_limit_orders_never_fill_through_the_limit(core, venue):
    core.create_basket("lmt", ["AAPL BUY 5000 LMT 99.50", "MSFT SELL 5000 LMT 401.00"])
    core.route_basket("B-0001", "ARCA")
    reports = _run(venue, 300)
    for r in reports:
        for oid, qty, px in r.fills:
            if oid == "O-000001":
                assert px <= 99.50
            else:
                assert px >= 401.00
    buy = core.order("O-000001")
    assert buy.filled + buy.leaves == buy.qty
    # With a 0.08 % per-step walk both limits (0.5 % / 0.25 % away) get touched within 300 steps.
    assert any(r.fills for r in reports)


def test_far_limit_never_fills(core, venue):
    core.create_basket("far", ["AAPL BUY 1000 LMT 50.00"])
    core.route("O-000001", "NYSE")
    reports = _run(venue, 100)
    assert not any(r.fills for r in reports)
    assert core.order("O-000001").status == "WORKING"


def test_ioc_one_shot_then_venue_cancel(core, venue, listener):
    core.create_basket("ioc", ["AAPL BUY 10000 IOC"])
    core.route("O-000001", "BATS")
    venue.step()  # ack
    report = venue.step()  # one attempt, remainder cancelled
    order = core.order("O-000001")
    assert report.fills and report.cancels == ["O-000001"]
    assert order.status == "CANCELLED" and 0 < order.filled < 10000
    assert listener.events[-1].detail.startswith("IOC")
    assert not venue.step().actions


def test_dark_pool_rejects_some(core):
    rejects = 0
    for seed in range(40):
        c = OmsCore()
        v = MockVenue(c, {"AAPL": 100.0}, seed=seed)
        c.create_basket("d", ["AAPL BUY 100"])
        c.route("O-000001", "DARK")
        rejects += len(v.step().rejects)
    assert 0 < rejects < 40 * DARK_REJECT_PROBABILITY * 3


def test_deterministic_for_a_seed():
    def run(seed):
        c = OmsCore()
        v = MockVenue(c, {"AAPL": 100.0, "MSFT": 400.0}, seed=seed)
        c.create_basket("x", ["AAPL BUY 5000", "MSFT SELL 2000 LMT 399.9"])
        c.route_basket("B-0001", "ALGO-VWAP")
        return [(r.acks, r.fills, r.cancels) for r in (v.step() for _ in range(30))]

    assert run(5) == run(5)
    assert run(5) != run(6)


def test_trader_cancel_between_snapshot_and_step_is_tolerated(core, venue):
    core.create_basket("race", ["AAPL BUY 1000"])
    core.route("O-000001", "NYSE")
    venue.step()
    # Simulate the race: the snapshot sees WORKING, the trader cancels before the fill lands.
    snapshot = venue.core.live_orders
    venue.core.live_orders = lambda: (core.cancel("O-000001"), snapshot())[1] or [core.order("O-000001")]  # type: ignore[assignment]
    report = venue.step()
    assert isinstance(report, StepReport) and not report.fills
    assert core.order("O-000001").status == "CANCELLED"


def test_step_report_actions_count():
    r = StepReport(acks=["a"], fills=[("a", 1, 1.0)], cancels=["a"], rejects=[])
    assert r.actions == 3
