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
* the order-state path: the engine's fold of the message tables (delivered out of order,
  text or typed columns) equals :func:`states.fold_all`, the tree it builds equals the
  reference, an appended state message ticks through to a break, and
  ``OTR_SOURCE=state-files`` builds the app from ``state-samples/``.
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
# The order-state path (states_dag)
# --------------------------------------------------------------------------------------

SAMPLES_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "state-samples")


def _sorted(rows):
    return sorted(rows, key=lambda r: (r["Level"], r["OrderId"]))


def _engine_levels(levels):
    return _sorted(dict(r, Level=level) for level, table in levels.items() for r in _rows(table))


@pytest.fixture(scope="module")
def state_book(server):
    import random

    from order_tree_recon import states, states_dag

    topology = config.default_topology()
    cfg = states.load_state_config("", topology)
    messages = {s.system: states.read_state_files(s.files, SAMPLES_DIR) for s in cfg.systems}
    # Noise on both sides: a message without an order id (skipped) and an order whose
    # numbers are malformed (null, never an error) and whose side code is unknown.
    inputs = {}
    for system, (msgs, columns, _, _) in messages.items():
        inputs[system] = list(msgs) + [{c: "" for c in columns}]
    inputs["RAPTOR"].append(dict({c: "" for c in messages["RAPTOR"][1]}, OrderId="ZZ-NOISE", Side="Q",
                                 OrderQty="1,000", CumQty="abc", LeavesQty=" 5 ", AvgPx="inf", OrdStatus="NEW"))
    rows, py_ids = states.fold_all(cfg, inputs)
    # The engine gets the sequenced systems shuffled: the sequence field restores the order.
    rng = random.Random(11)
    tables = {}
    for system, msgs in inputs.items():
        msgs = list(msgs)
        if system in ("ALGO", "OR"):
            rng.shuffle(msgs)
        tables[system] = states_dag.state_table(msgs, messages[system][1])
    levels, ids = states_dag.state_level_tables(topology, cfg, tables)
    return {"cfg": cfg, "messages": messages, "tables": tables, "levels": levels, "ids": ids,
            "rows": rows, "py_ids": py_ids}


def test_state_fold_matches_python_fold(state_book):
    assert _engine_levels(state_book["levels"]) == _sorted(state_book["rows"])
    assert {(r["System"], r["Id"], r["OrderId"]) for r in _rows(state_book["ids"])} == set(state_book["py_ids"])


def test_state_fold_accepts_typed_columns(state_book):
    """A Kafka json_spec table: numeric quantities, a numeric sequence, null prices."""
    from deephaven import new_table
    from deephaven.column import double_col, long_col, string_col
    from deephaven.constants import NULL_DOUBLE

    from order_tree_recon import states_dag

    msgs, _, _, _ = state_book["messages"]["ALGO"]
    text_columns = ("AlgoOrderId", "ParentAlgoOrderId", "RaptorOrderId", "Strategy", "Symbol", "Side", "State")
    typed = new_table(
        [string_col(c, [m[c] for m in msgs]) for c in text_columns]
        + [long_col(c, [int(m[c]) for m in msgs]) for c in ("Version", "Qty", "Filled", "Open")]
        + [double_col("AvgPrice", [float(m["AvgPrice"]) if m["AvgPrice"] else NULL_DOUBLE for m in msgs])]
    )
    topology = config.default_topology()
    levels, _ = states_dag.state_level_tables(topology, state_book["cfg"], {"ALGO": typed})
    algo = {k: v for k, v in levels.items() if k.startswith("ALGO")}
    expected = [r for r in state_book["rows"] if r["Level"].startswith("ALGO")]
    assert _engine_levels(algo) == _sorted(expected)


def test_state_path_recon_matches_reference(state_book, book):
    from order_tree_recon import dag, states_mock
    from order_tree_recon.query_api import make_query_api

    topology = config.default_topology()
    tables = dag.build_order_tree(topology, state_book["levels"], 1e-6, extra_ids=state_book["ids"])
    ref = reference.reconcile(topology, state_book["rows"], 1e-6, extra_ids=state_book["py_ids"])
    engine = {r["NodeKey"]: r for r in _rows(tables["otr_recon"])}
    assert set(engine) == set(ref)
    diffs = [
        (key, column, engine[key][column], ref[key][column])
        for key in ref
        for column in dag.RECON_COLUMNS
        if not _same(engine[key][column], ref[key][column])
    ]
    assert diffs == []

    # find_tree resolves the amended-away ClOrdID through the id history.
    _, client_id = book.scenario_roots[states_mock.AMENDED_SCENARIO]
    client = next(r for r in state_book["rows"] if r["OrderId"] == client_id)
    tree = _rows(make_query_api(tables)["find_tree"](client["AltId"][: -len("-R1")]))
    assert {r["RootKey"] for r in tree} == {f"RAPTOR|{client_id}"}
    assert all(r["LinkState"] in ("ROOT", "LINKED") for r in tree)


def test_state_messages_tick_through_to_a_break(state_book, book):
    """An appended venue state the router parent never books -> UNBOOKED_FILL, live."""
    from deephaven import input_table

    from order_tree_recon import states_dag

    topology = config.default_topology()
    router_msgs, router_columns, _, _ = state_book["messages"]["OR"]
    router = input_table(init_table=states_dag.state_table(router_msgs, router_columns))  # append-only
    messages = dict(state_book["tables"], OR=router)
    tables = states_dag.build_from_states(topology, state_book["cfg"], messages, 1e-6)

    _, order_id = book.scenario_roots["working"]
    root = next(r for r in book.roots if r.order_id == order_id)
    venue = next(o for o in root.walk() if o.level == "OR_CHILD" and o.leaves >= 100)
    parent_key = f"OR|{venue.link_id}"

    def kind_of(key):
        rows = _rows(tables["otr_recon"].where(f"NodeKey == `{key}`").view(["BreakKind"]))
        return rows[0]["BreakKind"] if rows else None

    assert kind_of(parent_key) == "NONE"
    latest = [m for m in router_msgs if m["RouterOrderId"] == venue.order_id][-1]
    filled = dict(latest, UpdateTime="2026-10-09T23:59:59.999", CumQty=str(int(venue.cum + 100)),
                  LeavesQty=str(int(venue.leaves - 100)), Status="PARTIALLY_FILLED")
    router.add(states_dag.state_table([filled], router_columns))

    assert _wait_for(lambda: kind_of(parent_key) == "UNBOOKED_FILL")
    bucket = _rows(tables["otr_exposure"].where(["RootMoniker == `ACME`", "RootSymbol == `MSFT`", "RootSide == `BUY`"]))
    assert bucket[0]["NetUnbookedQty"] == 100.0


def test_app_entrypoint_state_files(server, book, ref):
    from order_tree_recon import app

    built = app.build({"OTR_SOURCE": "state-files", "OTR_STATE_DIR": SAMPLES_DIR})
    for name in ("state_raptor_updates", "state_algo_updates", "state_or_updates", "otr_recon", "otr_tree",
                 "order_tree", app.DASHBOARD_NAME):
        assert name in built, name
    assert built["otr_nodes"].size == len(mockdata.canonical_rows(book))
    assert built["otr_breaks"].size == sum(n["Breaks"] for n in ref.values())


def test_state_files_with_a_system_missing(server, tmp_path):
    """Only Raptor's file present: the other levels are empty, everything still builds."""
    import shutil

    from order_tree_recon import app

    shutil.copy(os.path.join(SAMPLES_DIR, "raptor.csv"), tmp_path / "raptor.csv")
    built = app.build({"OTR_SOURCE": "state-files", "OTR_STATE_DIR": str(tmp_path)})
    nodes = _rows(built["otr_recon"].view(["Level", "LinkState"]))
    assert nodes and {(n["Level"], n["LinkState"]) for n in nodes} == {("CLIENT", "ROOT")}
    assert built["state_algo_updates"].size == 0
    assert app.DASHBOARD_NAME in built


def test_missing_column_is_a_clear_error(server, state_book):
    from order_tree_recon import states_dag

    table = state_book["tables"]["OR"].drop_columns(["UpstreamOrderId"])
    with pytest.raises(ValueError, match="UpstreamOrderId"):
        states_dag.state_level_tables(config.default_topology(), state_book["cfg"], {"OR": table})
