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
  without rebuilding anything;
* the FIX path: the engine's fold of raw messages equals :func:`fix.fold_all`, the tree
  it builds equals the reference, an appended ExecutionReport ticks through to a break,
  and ``OTR_SOURCE=fix-files`` builds the app from ``fix-samples/``.
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


# --------------------------------------------------------------------------------------
# The FIX path (fix_dag)
# --------------------------------------------------------------------------------------

SAMPLES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fix-samples")


@pytest.fixture(scope="module")
def fix_book(server, book):
    from order_tree_recon import fix, fix_dag, fix_mock

    topology = config.default_topology()
    cfg = fix.load_fix_config("", topology)
    tapes = fix_mock.tapes(book)
    # A null, an empty line, garbage and a report without an order id: all skipped.
    noise = [None, "", "garbage", fix_mock.fix_message([("35", "8"), ("37", ""), ("14", "abc")])]
    messages = {system: fix_dag.fix_messages_table(lines + noise) for system, lines in tapes.items()}
    levels, ids = fix_dag.fix_level_tables(topology, cfg, messages)
    rows, py_ids = fix.fold_all(cfg, tapes)
    return {"cfg": cfg, "tapes": tapes, "levels": levels, "ids": ids, "rows": rows, "py_ids": py_ids}


def test_fix_fold_matches_python_fold(fix_book):
    def key(r):
        return (r["Level"], r["OrderId"])

    engine = [dict(r, Level=level) for level, table in fix_book["levels"].items() for r in _rows(table)]
    assert sorted(engine, key=key) == sorted(fix_book["rows"], key=key)
    assert {(r["System"], r["Id"], r["OrderId"]) for r in _rows(fix_book["ids"])} == set(fix_book["py_ids"])


def test_fix_path_recon_matches_reference(fix_book, book):
    from order_tree_recon import dag, fix_mock
    from order_tree_recon.query_api import make_query_api

    topology = config.default_topology()
    tables = dag.build_order_tree(topology, fix_book["levels"], 1e-6, extra_ids=fix_book["ids"])
    ref = reference.reconcile(topology, fix_book["rows"], 1e-6, extra_ids=fix_book["py_ids"])
    engine = {r["NodeKey"]: r for r in _rows(tables["otr_recon"])}
    assert set(engine) == set(ref)
    diffs = [
        (key, column, engine[key][column], ref[key][column])
        for key in ref
        for column in dag.RECON_COLUMNS
        if not _same(engine[key][column], ref[key][column])
    ]
    assert diffs == []

    # The router parent of the amended DMA order linked through Raptor's id history, and
    # find_tree resolves the amended-away ClOrdID too.
    _, client_id = book.scenario_roots[fix_mock.AMENDED_SCENARIO]
    client = next(r for r in fix_book["rows"] if r["OrderId"] == client_id)
    original = client["AltId"][: -len("-R1")]
    tree = _rows(make_query_api(tables)["find_tree"](original))
    assert {r["RootKey"] for r in tree} == {f"RAPTOR|{client_id}"}
    assert all(r["LinkState"] in ("ROOT", "LINKED") for r in tree)


def test_fix_messages_tick_through_to_a_break(fix_book, book):
    """An appended venue fill the router parent never books -> UNBOOKED_FILL, live."""
    from deephaven import input_table
    from order_tree_recon import fix_dag, fix_mock, mockdata as md

    topology = config.default_topology()
    router = input_table(init_table=fix_dag.fix_messages_table(fix_book["tapes"]["OR"]))  # append-only
    messages = {
        "RAPTOR": fix_dag.fix_messages_table(fix_book["tapes"]["RAPTOR"]),
        "ALGO": fix_dag.fix_messages_table(fix_book["tapes"]["ALGO"]),
        "OR": router,
    }
    tables = fix_dag.build_from_fix(topology, fix_book["cfg"], messages, 1e-6)

    system, order_id = book.scenario_roots["working"]
    root = next(r for r in book.roots if r.order_id == order_id)
    venue = next(o for o in root.walk() if o.level == "OR_CHILD" and o.leaves >= 100)
    parent_key = f"OR|{venue.link_id}"

    def kind_of(key):
        rows = _rows(tables["otr_recon"].where(f"NodeKey == `{key}`").view(["BreakKind"]))
        return rows[0]["BreakKind"] if rows else None

    assert kind_of(parent_key) == "NONE"
    fill = fix_mock.fix_message([
        ("35", "8"), ("37", venue.order_id), ("150", "1"), ("39", "1"), ("55", venue.symbol),
        ("54", md.SIDE_TO_FIX[venue.side]), ("38", str(int(venue.qty))), ("14", str(int(venue.cum + 100))),
        ("151", str(int(venue.leaves - 100))), ("6", repr(venue.px)),
    ])
    router.add(fix_dag.fix_messages_table([fill]))

    assert _wait_for(lambda: kind_of(parent_key) == "UNBOOKED_FILL")
    bucket = _rows(tables["otr_exposure"].where(["RootMoniker == `ACME`", "RootSymbol == `MSFT`", "RootSide == `BUY`"]))
    assert bucket[0]["NetUnbookedQty"] == 100.0


def test_app_entrypoint_fix_files(server, book, ref):
    from order_tree_recon import app

    built = app.build({"OTR_SOURCE": "fix-files", "OTR_FIX_DIR": SAMPLES_DIR})
    for name in ("fix_raptor_messages", "fix_algo_messages", "fix_or_messages", "otr_recon", "otr_tree",
                 "order_tree", app.DASHBOARD_NAME):
        assert name in built, name
    assert built["otr_nodes"].size == len(mockdata.canonical_rows(book))
    assert built["otr_breaks"].size == sum(n["Breaks"] for n in ref.values())


def test_fix_files_with_a_system_missing(server, tmp_path):
    """Only Raptor's log present: the other levels are empty, everything still builds."""
    import shutil

    from order_tree_recon import app

    shutil.copy(os.path.join(SAMPLES_DIR, "raptor.log"), tmp_path / "raptor.log")
    built = app.build({"OTR_SOURCE": "fix-files", "OTR_FIX_DIR": str(tmp_path)})
    nodes = _rows(built["otr_recon"].view(["Level", "LinkState"]))
    assert nodes and {(n["Level"], n["LinkState"]) for n in nodes} == {("CLIENT", "ROOT")}
    assert built["fix_algo_messages"].size == 0
    assert app.DASHBOARD_NAME in built
