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
* the FIX order-state path: the engine's SOW mirror and fold equal the python twins
  (:func:`fixstate.sow_current`, :func:`fixstate.fold_events`) -- deletes and a reconnect
  snapshot included -- and the tree equals the reference; the AMPS source, fed real
  ``AMPS.Message`` objects through a stub client, ticks the tree live (a fill, an ``oof``,
  a reconnect); ``OTR_SOURCE=fix-files`` / ``amps`` build the app.
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
# FIX order states from AMPS SOW topics (fixstate_dag, amps_sow)
# --------------------------------------------------------------------------------------

SAMPLES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "fix-samples")


def _sorted(rows):
    return sorted(rows, key=lambda r: (r["Level"], r["OrderId"]))


def _engine_levels(levels):
    return _sorted(dict(r, Level=level) for level, table in levels.items() for r in _rows(table))


@pytest.fixture(scope="module")
def fix_book(server, book):
    from order_tree_recon import fixstate, fixstate_dag, fixstate_mock

    topology = config.default_topology()
    cfg = fixstate.load_fix_config("", topology)
    events = fixstate_mock.events(book)
    # Noise on both sides: an event with no key, garbage, a malformed number.
    events["RAPTOR"] = events["RAPTOR"] + [
        fixstate.event("raptor.orders", "", 10_000),
        fixstate.event("raptor.orders", "not fix at all", 10_001),
        fixstate.event("raptor.orders", "35=8|37=ZZ-NOISE|11=Z|55=X|54=Q|38=1,000|14=abc|151= 5 |6=inf|39=0|", 10_002),
    ]
    levels, ids, current = fixstate_dag.fix_level_tables(
        topology, cfg, {system: fixstate_dag.event_table(rows) for system, rows in events.items()})
    rows, py_ids = fixstate.fold_events(cfg, events)
    return {"cfg": cfg, "events": events, "levels": levels, "ids": ids, "current": current,
            "rows": rows, "py_ids": py_ids}


def test_fix_fold_matches_python_twin(fix_book):
    assert _engine_levels(fix_book["levels"]) == _sorted(fix_book["rows"])
    assert {(r["System"], r["Id"], r["OrderId"]) for r in _rows(fix_book["ids"])} == set(fix_book["py_ids"])
    raptor_sow = _rows(fix_book["current"]["RAPTOR"].view(["T37"]))
    assert len(raptor_sow) == sum(1 for r in fix_book["rows"] if r["Level"] == "CLIENT")


def test_fix_path_recon_matches_reference(fix_book, book):
    from order_tree_recon import dag, fixstate_mock
    from order_tree_recon.query_api import make_query_api

    topology = config.default_topology()
    tables = dag.build_order_tree(topology, fix_book["levels"], 1e-6, extra_ids=fix_book["ids"])
    ref = reference.reconcile(topology, fix_book["rows"], 1e-6, extra_ids=fix_book["py_ids"])
    engine = {r["NodeKey"]: r for r in _rows(tables["otr_recon"])}
    assert set(engine) == set(ref)
    diffs = [(key, column, engine[key][column], ref[key][column])
             for key in ref for column in dag.RECON_COLUMNS if not _same(engine[key][column], ref[key][column])]
    assert diffs == []
    # find_tree resolves the amended-away ClOrdID (Raptor's OrigClOrdID / id history).
    _, client_id = book.scenario_roots[fixstate_mock.AMENDED_SCENARIO]
    tree = _rows(make_query_api(tables)["find_tree"]("BLUEFIN-00003"))
    assert {r["RootKey"] for r in tree} == {f"RAPTOR|{client_id}"}
    assert all(r["LinkState"] in ("ROOT", "LINKED") for r in tree)


def test_sow_mirror_deletes_and_reconnect_match_twin(server):
    from order_tree_recon import fixstate, fixstate_dag

    cfg = fixstate.load_fix_config("", config.default_topology())
    E = fixstate.event
    a = "35=8|37=A|11=A|55=X|54=1|38=100|14=0|151=100|39=0|"
    b = "35=8|37=B|11=B|55=X|54=1|38=100|14=0|151=100|39=0|"
    c = "35=8|37=C|11=C|55=X|54=1|38=100|14=0|151=100|39=0|"
    events = [E("t1", a, 1, sow_key="ka"), E("t1", b, 2, sow_key="kb"), E("t1", c, 3, sow_key="kc"),
              E("t1", "", 4, action="SNAPSHOT_END"), E("t1", b.replace("14=0", "14=50"), 5, sow_key="kb"),
              E("t1", "", 6, action="DELETE", sow_key="kc"), E("t2", c, 7)]
    stages = [events, events + [E("t1", b, 8, epoch=2, sow_key="kb")],
              events + [E("t1", b, 8, epoch=2, sow_key="kb"), E("t1", "", 9, action="SNAPSHOT_END", epoch=2)]]
    for stage in stages:
        mirror = fixstate_dag.sow_mirror(fixstate_dag.parsed_events(cfg, "RAPTOR", fixstate_dag.event_table(stage)))
        engine = sorted((r["Topic"], r["T37"], r["T14"]) for r in _rows(mirror.view(["Topic", "T37", "T14"])))
        twin = sorted((e["Topic"], *fixstate.fix_values(e["RawFix"], ["37", "14"])) for e in fixstate.sow_current(stage))
        assert engine == twin
    assert [t[1] for t in twin] == ["B", "C"]  # A was not re-sent by snapshot 2; C lives on in t2


def _amps_message(command, data="", key=""):
    """A real ``AMPS.Message`` when the client is installed, else a stand-in with the same getters."""
    try:
        import AMPS
    except ImportError:  # pragma: no cover - the stand-in keeps the test runnable without the client
        class Stub:
            def get_command(self): return command
            def get_data(self): return data
            def get_sow_key(self): return key
        return Stub()
    message = AMPS.Message()
    message.set_command(command)
    message.set_data(data.replace("|", "\x01"))
    message.set_sow_key(key)
    return message


def test_amps_source_ticks_the_tree_live(server):
    """sow_and_subscribe through a stub client: snapshot, a fill, an oof, a reconnect snapshot."""
    from order_tree_recon import amps_sow, dag, fixstate, fixstate_dag

    topology = config.default_topology()
    cfg = fixstate.load_fix_config("", topology)
    handlers = {}

    class Client:
        def add_connection_state_listener(self, f): pass
        def connect_and_logon(self): pass
        def execute_async(self, command, handler):
            handlers[command] = handler
            return command
        def unsubscribe(self, sub_id): pass
        def close(self): pass

    source = amps_sow.start_source(cfg, amps_sow.AmpsSettings(["tcp://stub:9007/amps/fix"]), background=False,
                                   client_factory=lambda s: Client(), command_factory=lambda t, f, s: t)
    levels, ids, current = fixstate_dag.fix_level_tables(topology, cfg, source.tables)
    tables = dag.build_order_tree(topology, levels, 1e-6, extra_ids=ids)

    def nodes():
        return sorted((r["NodeKey"], r["CumQty"], r["LinkState"]) for r in
                      _rows(tables["otr_recon"].view(["NodeKey", "CumQty", "LinkState"])))

    client = "35=8|37=R1|11=C1|1=ACME|55=AAPL|54=1|38=100|14=0|151=100|6=0|39=0|"
    other = "35=8|37=R2|11=C2|1=ACME|55=MSFT|54=1|38=100|14=0|151=100|6=0|39=0|"
    parent = "35=8|37=P1|11=C1|55=AAPL|54=1|38=100|14=0|151=100|6=0|39=0|100=SOR|"
    raptor, router = handlers["raptor.orders"], handlers["router.parent.orders"]
    for handler, msgs in ((raptor, [("sow", client, "r1"), ("sow", other, "r2")]), (router, [("sow", parent, "p1")])):
        handler(_amps_message("group_begin"))
        for command, data, key in msgs:
            handler(_amps_message(command, data, key))
        handler(_amps_message("group_end"))
    expected = [("OR|P1", 0.0, "LINKED"), ("RAPTOR|R1", 0.0, "ROOT"), ("RAPTOR|R2", 0.0, "ROOT")]
    assert _wait_for(lambda: nodes() == expected), nodes()

    raptor(_amps_message("publish", client.replace("14=0", "14=40").replace("151=100", "151=60"), "r1"))
    raptor(_amps_message("oof", other, "r2"))
    expected = [("OR|P1", 0.0, "LINKED"), ("RAPTOR|R1", 40.0, "ROOT")]
    assert _wait_for(lambda: nodes() == expected), nodes()

    # Reconnect: the router's new snapshot no longer has P1 (deleted while disconnected).
    router(_amps_message("group_begin"))
    router(_amps_message("group_end"))
    assert _wait_for(lambda: nodes() == [("RAPTOR|R1", 40.0, "ROOT")]), nodes()
    status = {r["Topic"]: r for r in source.status()}
    assert (status["router.parent.orders"]["Epoch"], status["raptor.orders"]["DELETE"]) == (2, 1)
    source.stop()


def test_app_entrypoint_fix_files(server, book, ref):
    from order_tree_recon import app

    built = app.build({"OTR_SOURCE": "fix-files", "OTR_FIX_DIR": SAMPLES_DIR})
    for name in ("fix_raptor_sow", "fix_algo_sow", "fix_or_sow", "otr_recon", "otr_tree", "order_tree",
                 app.DASHBOARD_NAME):
        assert name in built, name
    assert built["otr_nodes"].size == len(mockdata.canonical_rows(book))
    assert built["otr_breaks"].size == sum(n["Breaks"] for n in ref.values())


def test_app_entrypoint_amps_comes_up_while_amps_is_unreachable(server):
    from order_tree_recon import app

    built = app.build({"OTR_SOURCE": "amps", "OTR_AMPS_URI": "tcp://127.0.0.1:1/amps/fix", "OTR_FIX_DIR": SAMPLES_DIR})
    try:
        for name in ("amps_source", "fix_raptor_sow", "otr_recon", app.DASHBOARD_NAME):
            assert name in built, name
        assert built["otr_nodes"].size == 0
        assert [s[1] for s in built["amps_source"].subscriptions()][:1] == ["raptor.orders"]
    finally:
        built["amps_source"].stop()


def test_fix_files_with_a_system_missing(server, tmp_path):
    import shutil

    from order_tree_recon import app

    shutil.copy(os.path.join(SAMPLES_DIR, "raptor.orders.fix"), tmp_path / "raptor.orders.fix")
    built = app.build({"OTR_SOURCE": "fix-files", "OTR_FIX_DIR": str(tmp_path)})
    nodes = _rows(built["otr_recon"].view(["Level", "LinkState"]))
    assert nodes and {(n["Level"], n["LinkState"]) for n in nodes} == {("CLIENT", "ROOT")}
    assert app.DASHBOARD_NAME in built
