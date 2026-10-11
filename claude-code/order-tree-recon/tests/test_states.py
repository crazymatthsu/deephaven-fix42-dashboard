"""The order-state layer on a bare host python: config, values, files, fold, samples, check.

The Deephaven side (states_dag) is pinned against the same fold in test_deephaven_embedded.py.
"""

from __future__ import annotations

import io
import json
import os
import random
import shutil
from typing import Dict, List

import pytest

from conftest import row, tree_of
from order_tree_recon import config, mockdata, reference, states, states_check, states_mock

MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLES_DIR = os.path.join(MODULE_DIR, "state-samples")


def _cfg(**overrides) -> dict:
    cfg = json.loads(json.dumps(states.DEFAULT_STATE_CONFIG))
    for system in cfg["systems"]:
        if system["system"] in overrides:
            system.update(overrides[system["system"]])
    return cfg


@pytest.fixture(scope="module")
def default_config() -> states.StateConfig:
    return states.load_state_config("", config.default_topology())


@pytest.fixture(scope="module")
def sample_messages(default_config) -> Dict[str, List[Dict[str, str]]]:
    return {s.system: states.read_state_files(s.files, SAMPLES_DIR)[0] for s in default_config.systems}


@pytest.fixture(scope="module")
def folded(default_config, sample_messages):
    return states.fold_all(default_config, sample_messages)


# --------------------------------------------------------------------------------------
# Values
# --------------------------------------------------------------------------------------


def test_text_and_number():
    assert states.text(None) == "" and states.text("  a b\t\r\n") == "a b" and states.text(12) == "12"
    assert [states.number(v) for v in ("100", " 2.5 ", "-1e3", ".5", "5.")] == [100.0, 2.5, -1000.0, 0.5, 5.0]
    assert [states.number(v) for v in ("", "1,000", "1_000", "n/a", "inf", "nan", None)] == [None] * 7


def test_legal_names():
    assert [states.legal_name(n) for n in ("OrderId", "Order Id", "cum-qty", "1st", " x ")] == [
        "OrderId", "Order_Id", "cum_qty", "_1st", "x",
    ]


# --------------------------------------------------------------------------------------
# The mapping config
# --------------------------------------------------------------------------------------


def test_default_config(topology, default_config):
    assert [s.system for s in default_config.systems] == ["RAPTOR", "ALGO", "OR"]
    assert sorted(r.level for s in default_config.systems for r in s.levels) == sorted(topology.names)
    raptor = default_config["RAPTOR"]
    assert raptor.id_fields == ("OrderId", "ClOrdID")
    assert dict(raptor.side_codes)["5"] == "SELL_SHORT"
    assert raptor.source("Status") == "OrdStatus" and raptor.source("Destination") is None
    assert default_config["ALGO"].field_names[:2] == ("AlgoOrderId", "Symbol")


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"ALGO": {"system": "NOPE"}}, "not in the topology"),
        ({"ALGO": {"order_id": "algo order id"}}, "field name"),
        ({"ALGO": {"fields": {"Qty": "Qty"}}}, "not a canonical column"),
        ({"ALGO": {"levels": [{"level": "ALGO_PARENT", "link": "RaptorOrderId"}]}}, "not mapped"),
        ({"RAPTOR": {"levels": [{"level": "CLIENT", "link": "X"}]}}, "root level"),
        ({"OR": {"levels": [{"level": "OR_CHILD", "when": {"field": "P"}, "link": "P"},
                            {"level": "OR_PARENT"}]}}, "needs a link"),
        ({"OR": {"levels": [{"level": "OR_PARENT", "link": "U"},
                            {"level": "OR_CHILD", "when": {"field": "P"}, "link": "P"}]}}, "must be the last"),
        ({"OR": {"levels": [{"level": "OR_CHILD", "when": {"field": "P", "equals": "a`b"}, "link": "P"},
                            {"level": "OR_PARENT", "link": "U"}]}}, "without ` or"),
        ({"OR": {"levels": [{"level": "ALGO_CHILD", "link": "P"}]}}, "belongs to system ALGO"),
        ({"OR": {"side_codes": "bloomberg"}}, "\"fix\" or an object"),
        ({"OR": {"files": "router.csv"}}, "list of glob patterns"),
        ({"OR": {"typo": 1}}, "unknown key"),
    ],
)
def test_config_validation(topology, overrides, message):
    with pytest.raises(ValueError, match=message):
        states.parse_state_config(_cfg(**overrides), topology)


def test_config_duplicates_and_load_errors(tmp_path, topology):
    cfg = _cfg()
    cfg["systems"].append(json.loads(json.dumps(cfg["systems"][0])))
    with pytest.raises(ValueError, match="already mapped"):
        states.parse_state_config(cfg, topology)
    good = tmp_path / "state_config.json"
    good.write_bytes(b"\xef\xbb\xbf" + json.dumps(states.DEFAULT_STATE_CONFIG).encode())
    assert states.load_state_config(str(good), topology) == states.load_state_config(None, topology)
    bad = tmp_path / "bad.json"
    bad.write_text("{nope")
    with pytest.raises(ValueError, match="not valid JSON"):
        states.load_state_config(str(bad), topology)


# --------------------------------------------------------------------------------------
# Files
# --------------------------------------------------------------------------------------


def test_read_csv_jsonl_and_json(tmp_path):
    (tmp_path / "a.csv").write_bytes(b"\xef\xbb\xbfOrder Id,Qty\r\nX1,100\r\n\r\nX2\r\nX3,1,extra\r\n")
    (tmp_path / "b.jsonl").write_text('{"Order Id": "X4", "Qty": 2.0, "Px": null, "Tags": {"a": 1}, "Live": true}\n'
                                      'not json\n[1, 2]\n\n')
    (tmp_path / "c.json").write_text('[{"Order Id": "X5", "Qty": 7.25}]')
    rows, columns, files, bad = states.read_state_files(["*.csv", "*.jsonl", "*.json"], str(tmp_path))
    assert [os.path.basename(f) for f in files] == ["a.csv", "b.jsonl", "c.json"]
    assert columns == ["Order_Id", "Qty", "Px", "Tags", "Live"]
    assert [r["Order_Id"] for r in rows] == ["X1", "X2", "X4", "X5"]
    assert rows[1] == {"Order_Id": "X2", "Qty": "", "Px": "", "Tags": "", "Live": ""}  # short CSV row
    assert rows[2] == {"Order_Id": "X4", "Qty": "2", "Px": "", "Tags": '{"a":1}', "Live": "true"}
    assert rows[3]["Qty"] == "7.25"
    assert bad == 3  # the CSV row with an extra cell, the non-JSON line, the array line
    (tmp_path / "d.xml").write_text("<x/>")
    with pytest.raises(ValueError, match="unsupported file type"):
        states.read_state_files(["*.xml"], str(tmp_path))


# --------------------------------------------------------------------------------------
# The fold
# --------------------------------------------------------------------------------------


def test_fold_latest_state_codes_levels_and_id_history(topology):
    cfg = states.parse_state_config(_cfg(ALGO={"status_codes": {"W": "WORKING"}}), topology)
    msgs = [
        {"Version": "9", "AlgoOrderId": "A-1", "ParentAlgoOrderId": "A-0", "Symbol": "AAPL", "Side": "BUY",
         "Qty": "500", "Filled": "0", "Open": "500", "AvgPrice": "", "State": "W", "Strategy": "VWAP"},
        {"Version": "12", "AlgoOrderId": "A-1", "ParentAlgoOrderId": "A-0", "Symbol": "AAPL", "Side": "BUY",
         "Qty": "500", "Filled": "200", "Open": "300", "AvgPrice": "10.5", "State": "W", "Strategy": "VWAP"},
        {"Version": "10", "AlgoOrderId": "A-1", "ParentAlgoOrderId": "A-0", "Symbol": "AAPL", "Side": "BUY",
         "Qty": "500", "Filled": "100", "Open": "400", "AvgPrice": "10", "State": "W", "Strategy": "VWAP"},
        {"Version": "1", "AlgoOrderId": " ", "Qty": "1"},                         # no order id: skipped
        {"Version": "3", "AlgoOrderId": "A-0", "RaptorOrderId": "R-1", "Qty": "n/a", "State": "NEW"},
    ]
    rows, ids = states.fold_system(cfg["ALGO"], msgs)
    by_id = {r["OrderId"]: r for r in rows}
    assert by_id["A-1"] == {
        "Level": "ALGO_CHILD", "OrderId": "A-1", "AltId": "", "LinkId": "A-0", "Moniker": "", "Symbol": "AAPL",
        "Side": "BUY", "OrderQty": 500.0, "CumQty": 200.0, "LeavesQty": 300.0, "AvgPx": 10.5,
        "Status": "WORKING", "Destination": "VWAP",
    }  # Version 12 beats 10 numerically, though "10" > "12" is false and "9" > "12" is true as text
    assert (by_id["A-0"]["Level"], by_id["A-0"]["LinkId"], by_id["A-0"]["OrderQty"]) == ("ALGO_PARENT", "R-1", None)
    assert ids == [("ALGO", "A-1", "A-1"), ("ALGO", "A-0", "A-0")]


def test_fold_sequence_ties_and_text_timestamps(topology, default_config):
    router = default_config["OR"]
    base = {"RouterOrderId": "O-1", "UpstreamOrderId": "U", "Qty": "100", "LeavesQty": "100"}
    msgs = [dict(base, UpdateTime="2026-10-09T13:30:01.000", CumQty="20"),
            dict(base, UpdateTime="2026-10-09T13:30:00.500", CumQty="10"),
            dict(base, UpdateTime="2026-10-09T13:30:01.000", CumQty="30")]  # a tie: the later message wins
    rows, _ = states.fold_system(router, msgs)
    assert rows[0]["CumQty"] == 30.0


def test_state_path_reconciles_like_the_table_path(topology, book, nodes, folded):
    rows, ids = folded
    state_nodes = reference.reconcile(topology, rows, extra_ids=ids)
    assert set(state_nodes) == set(nodes)
    _, amended = book.scenario_roots[states_mock.AMENDED_SCENARIO]
    diffs = [
        (key, column, state_nodes[key][column], nodes[key][column])
        for key in nodes
        for column in nodes[key]
        if state_nodes[key][column] != nodes[key][column]
    ]
    client = next(r for r in rows if r["OrderId"] == amended)
    assert diffs == [(f"RAPTOR|{amended}", "AltId", client["AltId"], client["AltId"][: -len("-R1")])]


def test_state_path_shows_every_scenario_break(topology, book, folded):
    rows, ids = folded
    state_nodes = reference.reconcile(topology, rows, extra_ids=ids)
    for scenario, expected in mockdata.EXPECTED_BREAKS.items():
        system, order_id = book.scenario_roots[scenario]
        got = sorted((n["Level"], n["BreakKind"]) for n in tree_of(state_nodes, f"{system}|{order_id}")
                     if n["BreakKind"] != "NONE")
        assert got == expected, scenario


def test_amended_id_links_only_through_id_history(topology, book, folded):
    rows, ids = folded
    _, client_id = book.scenario_roots[states_mock.AMENDED_SCENARIO]
    router = next(r for r in rows if r["Level"] == "OR_PARENT" and r["LinkId"].startswith("BLUEFIN-00003"))
    key = f"OR|{router['OrderId']}"
    assert reference.reconcile(topology, rows)[key]["LinkState"] == "ORPHAN"
    linked = reference.reconcile(topology, rows, extra_ids=ids)[key]
    assert (linked["LinkState"], linked["ParentKey"]) == ("LINKED", f"RAPTOR|{client_id}")


def test_out_of_order_delivery_is_harmless_with_a_sequence(default_config, sample_messages, folded):
    shuffled = {system: list(msgs) for system, msgs in sample_messages.items()}
    rng = random.Random(7)
    for system in ("ALGO", "OR"):  # the two systems with a sequence field
        rng.shuffle(shuffled[system])
    rows, _ = states.fold_all(default_config, shuffled)

    def key(r):
        return (r["Level"], r["OrderId"])

    assert sorted(rows, key=key) == sorted(folded[0], key=key)


def test_extra_ids_never_beat_current_ids(topology):
    rows = [row("CLIENT", "R1", AltId="C1"), row("CLIENT", "R2", AltId="C2"), row("OR_PARENT", "O1", "C2")]
    nodes = reference.reconcile(topology, rows, extra_ids=[("RAPTOR", "C2", "R1"), ("RAPTOR", "C9", "NOPE")])
    assert nodes["OR|O1"]["ParentKey"] == "RAPTOR|R2"


# --------------------------------------------------------------------------------------
# The committed samples
# --------------------------------------------------------------------------------------


def test_committed_samples_match_the_generator(tmp_path):
    states_mock.write_samples(str(tmp_path))
    for name in list(states_mock.SAMPLE_FILES.values()) + ["state_config.json"]:
        with open(os.path.join(SAMPLES_DIR, name), "rb") as committed, open(tmp_path / name, "rb") as fresh:
            assert committed.read() == fresh.read(), f"state-samples/{name} is stale: regenerate (see its README)"


def test_sample_config_file_is_the_default():
    with open(os.path.join(SAMPLES_DIR, "state_config.json"), encoding="utf-8") as handle:
        assert json.load(handle) == states.DEFAULT_STATE_CONFIG


# --------------------------------------------------------------------------------------
# states_check: the host-side dry run
# --------------------------------------------------------------------------------------


def test_states_check_on_the_samples():
    out = io.StringIO()
    assert states_check.check(SAMPLES_DIR, out=out) == 0
    text = out.getvalue()
    assert "raptor.csv: 88 messages" in text
    assert "links: ROOT 34, LINKED 305, ORPHAN 1, NO_LINK 0" in text
    assert "UpstreamOrderId = 'ALG-99999-01', not an id of any ALGO / RAPTOR order" in text
    assert "MISSING" not in text


def test_states_check_reports_wrong_fields_and_bad_inputs(tmp_path):
    for name in states_mock.SAMPLE_FILES.values():
        shutil.copy(os.path.join(SAMPLES_DIR, name), tmp_path / name)
    cfg = _cfg(ALGO={"levels": [{"level": "ALGO_CHILD", "when": {"field": "ParentAlgoOrderId"}, "link": "ParentAlgoOrderId"},
                                {"level": "ALGO_PARENT", "link": "RaptorOrderID"}]})  # wrong case
    (tmp_path / "state_config.json").write_text(json.dumps(cfg))
    out = io.StringIO()
    assert states_check.check(str(tmp_path), out=out) == 0
    text = out.getvalue()
    assert "MISSING: mapped field(s) ['RaptorOrderID'] are in no file" in text
    assert "RaptorOrderID is empty in its latest state" in text

    (tmp_path / "broken.json").write_text('{"systems": []}')
    out = io.StringIO()
    assert states_check.check(str(tmp_path), str(tmp_path / "broken.json"), out=out) == 1
    assert "INVALID" in out.getvalue()

    empty = tmp_path / "empty"
    empty.mkdir()
    out = io.StringIO()
    assert states_check.check(str(empty), out=out) == 1
    assert "NO FILES MATCHED" in out.getvalue()


# --------------------------------------------------------------------------------------
# OTR_SOURCE / OTR_STATE_DIR / OTR_STATE_CONFIG
# --------------------------------------------------------------------------------------


def test_source_settings(tmp_path):
    assert config.source({}) == "mock"
    assert config.source({"OTR_SOURCE": " STATE-FILES "}) == "state-files"
    with pytest.raises(ValueError, match="OTR_SOURCE"):
        config.source({"OTR_SOURCE": "kafka"})
    with pytest.raises(ValueError, match="not a directory"):
        config.state_dir({"OTR_STATE_DIR": str(tmp_path / "missing")})
    env = {"OTR_STATE_DIR": str(tmp_path)}
    assert config.state_config_path(env) == ""
    (tmp_path / "state_config.json").write_text("{}")
    assert config.state_config_path(env) == str(tmp_path / "state_config.json")
    (tmp_path / "mine.json").write_text("{}")
    assert config.state_config_path(dict(env, OTR_STATE_CONFIG="mine.json")) == str(tmp_path / "mine.json")
    with pytest.raises(ValueError, match="OTR_STATE_CONFIG"):
        config.state_config_path(dict(env, OTR_STATE_CONFIG="nope.json"))
