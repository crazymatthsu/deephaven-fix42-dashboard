"""The order core: parser, split arithmetic, every transition, roll-ups, the listener."""

from __future__ import annotations

import pytest

from basket_oms_demo.core import (
    OmsCore,
    OmsError,
    Order,
    OrderLine,
    allowed_actions,
    basket_rollup,
    parse_basket_lines,
    parse_line,
    split_quantities,
    weighted_avg_px,
)

# -- parser ---------------------------------------------------------------------------


def test_parse_line_defaults_to_market_day():
    line = parse_line("aapl buy 1000")
    assert line == OrderLine("AAPL", "BUY", 1000, "MKT", None, "DAY")


def test_parse_line_limit_and_tif_in_any_order():
    assert parse_line("MSFT SELL 500 GTC LMT 415.20").limit_px == 415.20
    assert parse_line("MSFT SELL 500 LMT 415.20 GTC").tif == "GTC"
    assert parse_line("msft, s, 500, ioc").tif == "IOC"
    assert parse_line("NVDA ss 100").side == "SHORT"


@pytest.mark.parametrize(
    "text, fragment",
    [
        ("AAPL BUY", "expected"),
        ("AAPL HOLD 100", "side"),
        ("AAPL BUY ten", "whole number"),
        ("AAPL BUY 0", "> 0"),
        ("AAPL BUY 100 LMT", "needs a price"),
        ("AAPL BUY 100 LMT abc", "must be a number"),
        ("AAPL BUY 100 LMT -1", "> 0"),
        ("AAPL BUY 100 FOK", "unexpected token"),
    ],
)
def test_parse_line_errors(text, fragment):
    with pytest.raises(OmsError) as info:
        parse_line(text)
    assert fragment in str(info.value)


def test_parse_basket_lines_skips_blanks_and_comments_and_reports_errors():
    text = "# my basket\nAAPL BUY 100\n\nMSFT SELL nope\n  NVDA BUY 50 LMT 118  # limit\n"
    lines, errors = parse_basket_lines(text)
    assert [l.symbol for l in lines] == ["AAPL", "NVDA"]
    assert errors == [(4, "MSFT SELL nope", "quantity must be a whole number (got 'nope')")]


def test_parse_basket_lines_passes_orderlines_through():
    given = OrderLine("JPM", "BUY", 10)
    lines, errors = parse_basket_lines([given, "BAC SELL 20"])
    assert lines[0] is given and lines[1].symbol == "BAC" and not errors


# -- split arithmetic ---------------------------------------------------------------


def test_split_quantities_sizes_differ_by_at_most_one_and_sum():
    assert split_quantities(1000, 3) == [334, 333, 333]
    assert split_quantities(7, 7) == [1] * 7
    assert sum(split_quantities(12345, 4)) == 12345


def test_split_quantities_rejects():
    with pytest.raises(OmsError):
        split_quantities(10, 1)
    with pytest.raises(OmsError):
        split_quantities(2, 3)


def test_weighted_avg_px():
    assert weighted_avg_px(None, 0, 10.0, 100) == 10.0
    assert weighted_avg_px(10.0, 100, 12.0, 100) == 11.0


# -- create / lifecycle ---------------------------------------------------------------


def test_create_basket_assigns_ids_and_publishes(core, listener, basket):
    assert basket.basket_id == "B-0001"
    orders = core.orders_in(basket.basket_id)
    assert [o.order_id for o in orders] == ["O-000001", "O-000002", "O-000003"]
    assert all(o.status == "NEW" and o.leaves == o.qty and o.venue is None for o in orders)
    assert orders[1].ord_type == "MKT" and orders[1].limit_px is None
    assert basket.status == "STAGED" and basket.strategy == "VWAP" and basket.trader == "alice"
    assert [e.type for e in listener.events] == ["CREATE", "CREATE", "CREATE", "BASKET"]
    assert listener.baskets[-1].status == "STAGED"


def test_create_basket_rejects_bad_input(core):
    with pytest.raises(OmsError, match="line 2"):
        core.create_basket("x", ["AAPL BUY 1", "bad"])
    with pytest.raises(OmsError, match="name"):
        core.create_basket("  ", ["AAPL BUY 1"])
    with pytest.raises(OmsError, match="strategy"):
        core.create_basket("x", ["AAPL BUY 1"], strategy="YOLO")
    with pytest.raises(OmsError, match="no order lines"):
        core.create_basket("x", "# nothing\n")
    assert core.counts()["baskets"] == 0


def test_route_ack_fill_to_filled(core, listener, basket):
    order = core.route("O-000001", "nyse")
    assert order.status == "ROUTED" and order.venue == "NYSE"
    assert core.basket("B-0001").status == "WORKING"
    core.ack("O-000001")
    assert core.order("O-000001").status == "WORKING"
    o, x = core.fill("O-000001", 400, 189.40)
    assert (o.status, o.filled, o.leaves, o.avg_px) == ("PARTIAL", 400, 600, 189.40)
    assert x.exec_id == "X-000001" and x.last_qty == 400 and x.venue == "NYSE"
    o, x2 = core.fill("O-000001", 5000, 189.50)  # clipped to leaves
    assert (o.status, o.filled, o.leaves) == ("FILLED", 1000, 0)
    assert o.avg_px == pytest.approx((400 * 189.40 + 600 * 189.50) / 1000)
    assert o.last_qty == 600 and x2.last_qty == 600
    types = [e.type for e in listener.events]
    assert types.count("FILL") == 2 and types.count("ROUTE") == 1 and types.count("ACK") == 1
    assert core.rollup("B-0001").filled == 1000


def test_route_rejects_unknown_venue_and_non_new(core, basket):
    with pytest.raises(OmsError, match="unknown venue"):
        core.route("O-000001", "MOON")
    core.route("O-000001", "ARCA")
    with pytest.raises(OmsError, match="only NEW"):
        core.route("O-000001", "ARCA")


def test_fill_requires_working(core, basket):
    core.route("O-000001", "NYSE")
    with pytest.raises(OmsError, match="only WORKING/PARTIAL"):
        core.fill("O-000001", 10, 189.0)


def test_reject_at_venue(core, basket):
    core.route("O-000002", "DARK")
    order = core.reject("O-000002", "dark pool declined")
    assert order.status == "REJECTED" and order.leaves == 0 and order.note == "dark pool declined"
    assert "reject" not in allowed_actions(order)


def test_modify_qty_px_tif(core, listener, basket):
    order = core.modify("O-000001", qty=800, limit_px=190.0, tif="GTC")
    assert (order.qty, order.leaves, order.limit_px, order.tif) == (800, 800, 190.0, "GTC")
    assert order.note == "Qty 1,000 -> 800; Px 189.50 -> 190.00; TIF DAY -> GTC"
    assert listener.events[-1].type == "MODIFY"


def test_modify_validation(core, basket):
    with pytest.raises(OmsError, match="nothing to modify"):
        core.modify("O-000001")
    with pytest.raises(OmsError, match="nothing to modify"):
        core.modify("O-000001", qty=1000)
    with pytest.raises(OmsError, match="only a LMT"):
        core.modify("O-000002", limit_px=1.0)
    with pytest.raises(OmsError, match="TIF"):
        core.modify("O-000001", tif="FOK")
    with pytest.raises(OmsError, match="> 0"):
        core.modify("O-000001", qty=0)


def test_modify_below_filled_rejected_and_down_to_filled_completes(core, basket):
    core.route("O-000001", "NYSE")
    core.ack("O-000001")
    core.fill("O-000001", 300, 189.0)
    with pytest.raises(OmsError, match="below the filled quantity"):
        core.modify("O-000001", qty=200)
    order = core.modify("O-000001", qty=300)
    assert order.status == "FILLED" and order.leaves == 0


def test_cancel_and_double_cancel(core, basket):
    order = core.cancel("O-000003")
    assert order.status == "CANCELLED" and order.leaves == 0
    with pytest.raises(OmsError, match="cannot be cancelled"):
        core.cancel("O-000003")
    core.route("O-000001", "NYSE")
    core.ack("O-000001")
    core.fill("O-000001", 100, 189.0)
    assert core.cancel("O-000001").status == "CANCELLED"
    assert core.order("O-000001").filled == 100


def test_split_by_slices(core, listener, basket):
    children = core.split("O-000003", slices=3)
    assert [c.qty for c in children] == [834, 833, 833]
    assert all(c.parent_id == "O-000003" and c.status == "NEW" and c.basket_id == "B-0001" for c in children)
    assert all(c.limit_px == 118.25 and c.tif == "GTC" and c.ord_type == "LMT" for c in children)
    parent = core.order("O-000003")
    assert parent.status == "SPLIT" and parent.leaves == 0
    assert allowed_actions(parent) == frozenset()
    assert listener.events[-1].type == "SPLIT"
    roll = core.rollup("B-0001")
    assert roll.orders == 5 and roll.qty == 1000 + 500 + 2500 and roll.status == "STAGED"


def test_split_by_qtys_and_validation(core, basket):
    with pytest.raises(OmsError, match="either"):
        core.split("O-000001")
    with pytest.raises(OmsError, match="either"):
        core.split("O-000001", slices=2, qtys=[500, 500])
    with pytest.raises(OmsError, match="sum to"):
        core.split("O-000001", qtys=[500, 400])
    with pytest.raises(OmsError, match="at least 2"):
        core.split("O-000001", qtys=[1000])
    with pytest.raises(OmsError, match="> 0"):
        core.split("O-000001", qtys=[1000, 0])
    children = core.split("O-000001", qtys=[600, 400])
    assert [c.qty for c in children] == [600, 400]
    core.route("O-000002", "NYSE")
    with pytest.raises(OmsError, match="only unrouted NEW"):
        core.split("O-000002", slices=2)


def test_allowed_actions_by_status():
    def order(status, qty=10):
        return Order("O", "B", "AAPL", "BUY", qty, "MKT", None, "DAY", "t", None, None, status=status)  # type: ignore[arg-type]

    assert allowed_actions(order("NEW")) == {"modify", "cancel", "route", "split"}
    assert allowed_actions(order("NEW", qty=1)) == {"modify", "cancel", "route"}
    assert allowed_actions(order("ROUTED")) == {"modify", "cancel"}
    assert allowed_actions(order("PARTIAL")) == {"modify", "cancel"}
    for terminal in ("FILLED", "CANCELLED", "REJECTED", "SPLIT"):
        assert allowed_actions(order(terminal)) == frozenset()


def test_route_and_cancel_basket(core, basket):
    core.cancel("O-000002")
    routed = core.route_basket("B-0001", "ALGO-VWAP")
    assert [o.order_id for o in routed] == ["O-000001", "O-000003"]
    assert core.basket("B-0001").status == "WORKING"
    cancelled = core.cancel_basket("B-0001")
    assert [o.order_id for o in cancelled] == ["O-000001", "O-000003"]
    assert core.basket("B-0001").status == "CANCELLED"
    assert core.route_basket("B-0001", "NYSE") == []


def test_basket_rollup_statuses(core, basket):
    assert core.rollup("B-0001").status == "STAGED"
    core.route("O-000001", "NYSE")
    assert core.rollup("B-0001").status == "WORKING"
    core.ack("O-000001")
    core.fill("O-000001", 1000, 189.0)
    core.cancel("O-000002")
    assert core.rollup("B-0001").status == "WORKING"  # O-000003 still NEW
    core.cancel("O-000003")
    roll = core.rollup("B-0001")
    assert roll.status == "DONE" and roll.done == 3 and roll.live == 0 and roll.pct_filled == 25.0
    assert basket_rollup([]).status == "STAGED"


def test_add_orders_and_venue_cancel(core, listener, basket):
    added = core.add_orders("B-0001", "JPM BUY 100 IOC")
    assert added[0].order_id == "O-000004" and added[0].tif == "IOC"
    core.route("O-000004", "BATS")
    core.ack("O-000004")
    core.fill("O-000004", 40, 205.0)
    order = core.venue_cancel("O-000004", "IOC remainder")
    assert order.status == "CANCELLED" and order.filled == 40
    assert listener.events[-1].user == "venue"


def test_unknown_ids(core):
    with pytest.raises(OmsError, match="unknown basket"):
        core.add_orders("B-9999", "AAPL BUY 1")
    with pytest.raises(OmsError, match="unknown order"):
        core.cancel("O-9")


def test_republish_replays_everything(core, listener, basket):
    core.route("O-000001", "NYSE")
    before = (len(listener.baskets), len(listener.orders), len(listener.events))
    core.republish()
    assert len(listener.baskets) == before[0] + 1
    assert len(listener.orders) == before[1] + 3
    assert len(listener.events) == before[2] + len(core.events)


def test_counts(core, basket):
    core.route("O-000001", "NYSE")
    counts = core.counts()
    assert counts["baskets"] == 1 and counts["orders"] == 3
    assert counts["orders_NEW"] == 2 and counts["orders_ROUTED"] == 1
