"""The mock book: deterministic, native-shaped, internally consistent."""

from __future__ import annotations

from order_tree_recon import mockdata


def test_deterministic():
    a = mockdata.generate(seed=7, families=10)
    b = mockdata.generate(seed=7, families=10)
    assert (a.raptor, a.algo, a.router) == (b.raptor, b.algo, b.router)
    c = mockdata.generate(seed=8, families=10)
    assert (a.raptor, a.algo, a.router) != (c.raptor, c.algo, c.router)


def test_catalog_only():
    data = mockdata.generate(seed=1, families=0)
    assert set(data.scenario_roots) == set(mockdata.SCENARIOS)
    assert len(data.roots) == len(mockdata.SCENARIOS)


def test_native_shapes(book):
    assert set(book.raptor[0]) == {"OrderId", "ClOrdID", "Moniker", "Symbol", "Side", "OrderQty", "CumQty",
                                   "LeavesQty", "AvgPx", "OrdStatus"}
    assert set(book.algo[0]) == {"AlgoOrderId", "ParentAlgoOrderId", "RaptorOrderId", "Strategy", "Symbol",
                                 "Side", "Qty", "Filled", "Open", "AvgPrice", "State"}
    assert set(book.router[0]) == {"RouterOrderId", "ParentRouterOrderId", "UpstreamOrderId", "Venue", "Symbol",
                                   "Side", "Qty", "CumQty", "LeavesQty", "AvgPx", "Status"}


def test_ids_unique_per_system(book):
    for rows, column in ((book.raptor, "OrderId"), (book.algo, "AlgoOrderId"), (book.router, "RouterOrderId")):
        ids = [r[column] for r in rows]
        assert len(ids) == len(set(ids))


def test_raptor_speaks_fix_sides_and_owns_the_moniker(book):
    assert {r["Side"] for r in book.raptor} <= set(mockdata.FIX_TO_SIDE)
    assert all(r["Moniker"] for r in book.raptor)
    assert "Moniker" not in book.algo[0] and "Moniker" not in book.router[0]


def test_role_columns(book):
    for a in book.algo:
        is_parent = a["ParentAlgoOrderId"] == ""
        assert bool(a["RaptorOrderId"]) == is_parent
    for o in book.router:
        is_parent = o["ParentRouterOrderId"] == ""
        assert bool(o["UpstreamOrderId"]) == is_parent


def test_leaves_follow_status(book):
    for rows, qty, cum, leaves, status in (
        (book.raptor, "OrderQty", "CumQty", "LeavesQty", "OrdStatus"),
        (book.algo, "Qty", "Filled", "Open", "State"),
        (book.router, "Qty", "CumQty", "LeavesQty", "Status"),
    ):
        for r in rows:
            if r[status] == "CANCELED":
                assert r[leaves] == 0
            else:
                assert r[leaves] == max(r[qty] - r[cum], 0)
            assert (r[cum] > 0) == (r[ "AvgPx" if "AvgPx" in r else "AvgPrice"] is not None)


def test_canonical_rows_cover_every_native_row(book):
    rows = mockdata.canonical_rows(book)
    assert len(rows) == len(book.raptor) + len(book.algo) + len(book.router)
    assert {r["Side"] for r in rows} <= {"BUY", "SELL", "SELL_SHORT"}
    levels = {r["Level"] for r in rows}
    assert levels == {"CLIENT", "ALGO_PARENT", "ALGO_CHILD", "OR_PARENT", "OR_CHILD"}


def test_split_qty():
    assert mockdata.split_qty(1000, 3) == [400.0, 300.0, 300.0]
    assert mockdata.split_qty(250, 2) == [150.0, 100.0]
    assert sum(mockdata.split_qty(3700, 4)) == 3700
    assert mockdata.split_qty(100, 5) == [100.0]
