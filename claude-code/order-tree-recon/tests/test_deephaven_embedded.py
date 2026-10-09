"""The Deephaven DAG against the python reference, on the real engine.

Skipped unless BOTH hold: ``deephaven_server`` is importable (``pip install
deephaven-server==42.4`` -- ~600 MB with its JVM jars, needs JDK 17+) and
``OTR_DH_TEST=1`` is set, so the default unit run stays light. What it proves:

* the adapters, linking, root walk and per-edge recon produce **exactly** the
  reference's rows -- every ``otr_recon`` column of every node, every ladder and
  exposure row;
* the tree, the query API, the app entrypoint and the ``deephaven.ui`` dashboard build;
* the DAG is incremental: one new fill on a venue order (a keyed input table here,
  your live order feed in production) re-derives the break and the bucket exposure
  without rebuilding anything.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("OTR_DH_TEST") != "1", reason="set OTR_DH_TEST=1 to run the embedded Deephaven test"
)

deephaven_server = pytest.importorskip("deephaven_server")

from order_tree_recon import config, mockdata, reference  # noqa: E402


@pytest.fixture(scope="module")
def server():
    port = int(os.environ.get("OTR_DH_TEST_PORT", "10096"))
    srv = deephaven_server.Server(port=port, jvm_args=["-Xmx1g"])
    srv.start()
    return srv


def _rows(table) -> List[Dict[str, Any]]:
    """``iter_dict`` with Deephaven's null sentinels mapped to ``None``."""
    from deephaven.constants import NULL_DOUBLE, NULL_INT, NULL_LONG

    sentinels = {NULL_DOUBLE, NULL_LONG, NULL_INT}

    def norm(value):
        if isinstance(value, bool) or value is None:
            return value
        return None if value in sentinels else value

    return [{k: norm(v) for k, v in r.items()} for r in table.iter_dict()]


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is None and b is None
        return abs(float(a) - float(b)) < 1e-9
    if a in (None, "") and b in (None, ""):
        return True
    return a == b


@pytest.fixture(scope="module")
def book():
    return mockdata.generate(seed=42, families=24)


@pytest.fixture(scope="module")
def built(server, book):
    from order_tree_recon import dag, sources

    native = sources.native_tables(book)
    levels = sources.adapters(native["raptor_orders"], native["algo_orders"], native["router_orders"])
    return dag.build_order_tree(config.default_topology(), levels, 1e-6)


@pytest.fixture(scope="module")
def ref(book):
    return reference.reconcile(config.default_topology(), mockdata.canonical_rows(book), 1e-6)


def test_every_table_is_built(built):
    from order_tree_recon import dag

    assert tuple(built) == dag.TABLE_NAMES


def test_recon_matches_reference_row_for_row(built, ref):
    from order_tree_recon import dag

    engine = {r["NodeKey"]: r for r in _rows(built["otr_recon"])}
    assert set(engine) == set(ref)
    diffs = [
        (key, column, engine[key][column], ref[key][column])
        for key in ref
        for column in dag.RECON_COLUMNS
        if not _same(engine[key][column], ref[key][column])
    ]
    assert diffs == []


def test_ladder_and_exposure_match_reference(built, ref):
    ladder = {tuple(r[c] for c in reference.LADDER_BY): r for r in _rows(built["otr_level_ladder"])}
    expected = reference.level_ladder(ref)
    assert len(ladder) == len(expected)
    for row in expected:
        got = ladder[tuple(row[c] for c in reference.LADDER_BY)]
        for column in ("Orders",) + reference.LADDER_SUMS + ("CumVsClient", "LeavesVsClient"):
            assert _same(float(got[column]), float(row[column])), (row, column)

    summary = {tuple(r[c] for c in reference.SUMMARY_BY): r for r in _rows(built["otr_exposure"])}
    expected = reference.exposure_summary(ref)
    assert len(summary) == len(expected)
    for row in expected:
        got = summary[tuple(row[c] for c in reference.SUMMARY_BY)]
        for column in ("Orders",) + reference.SUMMARY_SUMS + ("NetUnbookedQty", "StreetOpenVsClient"):
            assert _same(float(got[column]), float(row[column])), (row, column)


def test_tree_and_flat_tree(built, ref):
    from deephaven.table import TreeTable

    assert isinstance(built["otr_tree"], TreeTable)
    flat = _rows(built["otr_tree_flat"].view(["NodeKey", "TreePath", "Tree", "Depth"]))
    assert len(flat) == len(ref)
    root_keys = {ref[r["NodeKey"]]["RootKey"] for r in flat}
    assert len(root_keys) == sum(1 for n in ref.values() if n["Depth"] == 0)
    deepest = max(flat, key=lambda r: r["Depth"])
    assert deepest["Tree"].startswith("  " * deepest["Depth"] + "+- ")


def test_query_api(built, book, ref):
    from order_tree_recon.query_api import make_query_api

    api = make_query_api(built)
    result = api["order_tree"]("DUNE", "MSFT", "BUY", include_unlinked=False)
    assert [r["BreakKind"] for r in _rows(result["breaks"])] == ["CHILD_STILL_OPEN"]
    exposure = _rows(result["exposure"])
    assert len(exposure) == 1 and exposure[0]["StreetOpenVsClient"] == 100.0
    assert len(_rows(result["ladder"])) == 5

    # from the still-open venue order back to the whole tree
    system, order_id = book.scenario_roots["cancel_not_propagated"]
    root_key = f"{system}|{order_id}"
    venue = next(n for n in ref.values() if n["RootKey"] == root_key and n["IsStreet"] and n["LeavesQty"] > 0)
    tree = _rows(api["find_tree"](venue["OrderId"]))
    assert {r["RootKey"] for r in tree} == {root_key}
    assert len(tree) == sum(1 for n in ref.values() if n["RootKey"] == root_key)
    assert _rows(api["find_tree"]("no-such-id")) == []

    nvda = [b for b in reference.exposure_summary(ref) if b["RootSymbol"] == "NVDA"]
    assert len(_rows(api["exposure"](symbol="NVDA"))) == len(nvda)  # incl. the unattributed orphan bucket
    assert "" not in {r["RootMoniker"] for r in _rows(api["exposure"]("ACME", "NVDA"))}


def _wait_for(predicate, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.2)
    return predicate()


def test_incremental_fill_on_a_live_router_table(server, book):
    """One venue fill that never reaches its router parent -> UNBOOKED_FILL, live."""
    from deephaven import input_table
    from order_tree_recon import dag, sources

    native = sources.native_tables(book)
    router = input_table(init_table=native["router_orders"], key_cols="RouterOrderId")
    levels = sources.adapters(native["raptor_orders"], native["algo_orders"], router)
    tables = dag.build_order_tree(config.default_topology(), levels, 1e-6)

    ref = reference.reconcile(config.default_topology(), mockdata.canonical_rows(book))
    system, order_id = book.scenario_roots["working"]
    venue = next(n for n in ref.values()
                 if n["RootKey"] == f"{system}|{order_id}" and n["IsStreet"] and n["LeavesQty"] >= 100)
    parent_key = venue["ParentKey"]

    def kind_of(key):
        rows = _rows(tables["otr_recon"].where(f"NodeKey == `{key}`").view(["BreakKind"]))
        return rows[0]["BreakKind"] if rows else None

    assert kind_of(parent_key) == "NONE"
    native_row = next(r for r in book.router if r["RouterOrderId"] == venue["OrderId"])
    filled = dict(native_row, CumQty=native_row["CumQty"] + 100, LeavesQty=native_row["LeavesQty"] - 100)
    router.add(sources._table([filled], sources._ROUTER_COLUMNS))

    assert _wait_for(lambda: kind_of(parent_key) == "UNBOOKED_FILL")
    bucket = _rows(tables["otr_exposure"].where(["RootMoniker == `ACME`", "RootSymbol == `MSFT`", "RootSide == `BUY`"]))
    assert bucket[0]["NetUnbookedQty"] == 100.0


def test_app_entrypoint_builds_everything(server):
    from order_tree_recon import app

    built = app.build({"OTR_MOCK_FAMILIES": "4", "OTR_SEED": "3"})
    for name in ("raptor_orders", "algo_orders", "router_orders", "otr_recon", "otr_tree",
                 "order_tree", "find_tree", "exposure", app.DASHBOARD_NAME):
        assert name in built, name
