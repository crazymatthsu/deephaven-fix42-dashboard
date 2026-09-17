"""Embedded-server test: the table bridge and the venue inside an in-process Deephaven.

Skipped unless ``OMS_DH_TEST=1`` **and** ``deephaven_server`` is importable (it is not a
dependency of the module; install ``deephaven-server`` into the venv to run this, and on
an arm64 Mac point ``JAVA_HOME`` at an arm64 JDK 21). Everything else in the suite is
Deephaven-free.
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("OMS_DH_TEST") != "1", reason="set OMS_DH_TEST=1 to run the embedded Deephaven test")

deephaven_server = pytest.importorskip("deephaven_server")


@pytest.fixture(scope="module")
def server():
    srv = deephaven_server.Server(port=0, jvm_args=["-Xmx1g"])
    srv.start()
    yield srv


@pytest.fixture
def runtime(server):
    from deephaven.execution_context import get_exec_ctx

    from basket_oms_demo.app import Runtime
    from basket_oms_demo.config import Config

    with get_exec_ctx():
        rt = Runtime(Config(mock_baskets=5, sim_enabled=False, seed=42))
        rt.seed()
        yield rt


def _rows(table, cols):
    import deephaven.pandas as dhpd

    frame = dhpd.to_pandas(table.view(list(cols)))
    return frame.to_dict("records")


def test_seed_lands_in_the_tables(runtime):
    b = runtime.bridge
    assert b.baskets.size == 5 and b.orders.size == 24 and b.quotes.size == 24
    assert b.events.size == len(runtime.core.events) and b.executions.size == len(runtime.core.executions)
    views = {r["BasketId"]: r for r in _rows(b.baskets_view, ["BasketId", "Status", "Orders", "Live", "Done", "Qty", "Filled", "PctFilled"])}
    assert views["B-0001"]["Status"] == "STAGED" and views["B-0001"]["Orders"] == 6 and views["B-0001"]["Filled"] == 0
    assert views["B-0004"]["Status"] == "DONE" and views["B-0004"]["Done"] == 3
    # The SPLIT parent is excluded from the roll-up: 3 children + 1 sibling.
    assert views["B-0005"]["Orders"] == 4 and views["B-0005"]["Qty"] == 34000
    # The views are read-only projections (no InputTable attribute leaks through).
    assert "InputTable" not in str(b.orders_view.j_table.getAttributes())
    assert "InputTable" not in str(b.baskets_view.j_table.getAttributes())


def test_actions_and_venue_tick_through_the_bridge(runtime):
    from deephaven.execution_context import get_exec_ctx

    core, b, venue = runtime.core, runtime.bridge, runtime.venue
    with get_exec_ctx():
        basket_id = core.create_basket("embedded", ["AAPL BUY 1000", "MSFT BUY 400 LMT 1.00"]).basket_id
        orders = core.orders_in(basket_id)
        core.route_basket(basket_id, "NYSE")
        for _ in range(60):
            venue.step()
            if core.order(orders[0].order_id).status == "FILLED":
                break
    rows = {r["OrderId"]: r for r in _rows(b.orders_view, ["OrderId", "Status", "Filled", "Leaves", "Venue", "Bid", "Ask", "PctFilled"])}
    aapl, msft = rows[orders[0].order_id], rows[orders[1].order_id]
    assert aapl["Status"] == "FILLED" and aapl["Filled"] == 1000 and aapl["Leaves"] == 0 and aapl["PctFilled"] == 100.0
    assert msft["Status"] == "WORKING" and msft["Filled"] == 0  # a BUY limit of 1.00 never meets the ask
    assert aapl["Venue"] == "NYSE" and aapl["Bid"] < aapl["Ask"]
    view = {r["BasketId"]: r for r in _rows(b.baskets_view, ["BasketId", "Status", "Live", "Done", "Filled"])}[basket_id]
    assert view["Status"] == "WORKING" and view["Done"] == 1 and view["Live"] == 1 and view["Filled"] == 1000
    execs = _rows(b.executions.where(f"OrderId == `{orders[0].order_id}`"), ["LastQty"])
    assert sum(r["LastQty"] for r in execs) == 1000
    tree_rows = _rows(b.orders.where(f"BasketId == `B-0005`"), ["OrderId", "ParentId"])
    assert sum(1 for r in tree_rows if r["ParentId"] == "O-000020") == 3
