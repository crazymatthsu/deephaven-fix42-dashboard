"""The FIX layer on a bare host python: parser, mapping config, files, fold, sample tapes.

The Deephaven side (fix_dag) is pinned against the same fold in test_deephaven_embedded.py.
"""

from __future__ import annotations

import json
import os
from typing import Dict, List

import pytest

from order_tree_recon import config, fix, fix_mock, mockdata, reference

from conftest import tree_of

MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLES_DIR = os.path.join(MODULE_DIR, "fix-samples")


def msg(*fields: str, delimiter: str = "|") -> str:
    """A message from ``"tag=value"`` strings, with a valid header and trailer."""
    return fix_mock.fix_message([tuple(f.split("=", 1)) for f in fields], delimiter)  # type: ignore[misc]


@pytest.fixture(scope="module")
def default_config() -> fix.FixConfig:
    return fix.load_fix_config("", config.default_topology())


@pytest.fixture(scope="module")
def tapes(book) -> Dict[str, List[str]]:
    return fix_mock.tapes(book)


@pytest.fixture(scope="module")
def folded(default_config, tapes):
    return fix.fold_all(default_config, tapes)


# --------------------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("delimiter", ["|", "\x01", "^A"])
def test_parse_fix_delimiters(delimiter):
    raw = msg("35=8", "37=X1", "55=AAPL").replace("|", delimiter)
    assert fix.parse_fix(raw)["37"] == "X1"
    assert fix.parse_fix(raw)["55"] == "AAPL"


def test_parse_fix_log_prefix_crlf_and_first_occurrence_wins():
    raw = "20261009-13:30:00.007 : " + msg("35=8", "37=A", "37=B", "58=x=y") + "\r\n"
    fields = fix.parse_fix(raw)
    assert fields["8"] == "FIX.4.2"
    assert fields["37"] == "A"
    assert fields["58"] == "x=y"


def test_parse_fix_garbage_is_empty_not_an_error():
    assert fix.parse_fix("") == {}
    assert fix.parse_fix(None) == {}  # type: ignore[arg-type]
    assert fix.parse_fix("hello world") == {}


def test_pick_values_normalizes_codes_and_bad_numbers():
    raw = msg("35=8", "54=5", "39=4", "14=1_000", "151=inf", "38=500", "6=12.5", "55=JPM")
    assert fix.pick_values(raw, ["54", "39", "14", "151", "38", "6", "55", "999"]) == [
        "SELL_SHORT", "CANCELED", "", "", "500", "12.5", "JPM", "",
    ]
    assert fix.pick_values(msg("54=Z", "39=Q"), ["54", "39"]) == ["Z", "Q"]  # unknown codes pass through


def test_mock_messages_have_valid_length_and_checksum(tapes):
    lines = [line for tape in tapes.values() for line in tape]
    assert lines and all(fix_mock.checksum_ok(line) for line in lines)
    assert not fix_mock.checksum_ok(lines[0].replace("DROPCOPY", "DROPCOPX"))


# --------------------------------------------------------------------------------------
# The mapping config
# --------------------------------------------------------------------------------------


def _cfg(**overrides) -> dict:
    cfg = json.loads(json.dumps(fix.DEFAULT_FIX_CONFIG))
    for system in cfg["systems"]:
        if system["system"] in overrides:
            system.update(overrides[system["system"]])
    return cfg


def test_default_config_maps_every_level(topology, default_config):
    assert [s.system for s in default_config.systems] == ["RAPTOR", "ALGO", "OR"]
    mapped = [rule.level for s in default_config.systems for rule in s.levels]
    assert sorted(mapped) == sorted(topology.names)
    algo = default_config["ALGO"]
    assert algo.destination_tag == "847"
    assert algo.sticky_tags == ("1", "20001", "11")
    assert algo.tags[:2] == ("35", "37")


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"ALGO": {"system": "NOPE"}}, "not in the topology"),
        ({"ALGO": {"levels": [{"level": "ALGO_PARENT", "link_tag": "11"}]}}, "not mapped"),
        ({"RAPTOR": {"levels": [{"level": "CLIENT", "link_tag": "11"}]}}, "root level"),
        ({"OR": {"levels": [{"level": "OR_CHILD", "when": {"tag": "20001"}, "link_tag": "20001"},
                            {"level": "OR_PARENT"}]}}, "needs a link_tag"),
        ({"OR": {"levels": [{"level": "OR_PARENT", "link_tag": "11"},
                            {"level": "OR_CHILD", "when": {"tag": "20001"}, "link_tag": "20001"}]}}, "must be the last"),
        ({"OR": {"levels": [{"level": "OR_CHILD", "when": {"tag": "20001", "equals": "a`b"}, "link_tag": "20001"},
                            {"level": "OR_PARENT", "link_tag": "11"}]}}, "plain value"),
        ({"OR": {"levels": [{"level": "ALGO_CHILD", "link_tag": "11"}]}}, "belongs to system ALGO"),
        ({"OR": {"order_id_tag": "ClOrdID"}}, "FIX tag number"),
        ({"OR": {"files": "router.log"}}, "list of glob patterns"),
        ({"OR": {"typo": 1}}, "unknown key"),
    ],
)
def test_config_validation(topology, overrides, message):
    with pytest.raises(ValueError, match=message):
        fix.parse_fix_config(_cfg(**overrides), topology)


def test_config_duplicate_level_and_system(topology):
    cfg = _cfg()
    cfg["systems"].append(json.loads(json.dumps(cfg["systems"][0])))
    with pytest.raises(ValueError, match="already mapped"):
        fix.parse_fix_config(cfg, topology)
    with pytest.raises(ValueError, match="at least one system"):
        fix.parse_fix_config({"systems": []}, topology)


def test_config_int_tags_and_nulls_are_accepted(topology):
    cfg = _cfg(RAPTOR={"moniker_tag": 1, "destination_tag": None})
    raptor = fix.parse_fix_config(cfg, topology)["RAPTOR"]
    assert raptor.moniker_tag == "1" and raptor.destination_tag is None


def test_load_fix_config_bom_and_bad_json(tmp_path, topology):
    good = tmp_path / "fix_config.json"
    good.write_bytes(b"\xef\xbb\xbf" + json.dumps(fix.DEFAULT_FIX_CONFIG).encode())
    assert fix.load_fix_config(str(good), topology) == fix.load_fix_config(None, topology)
    bad = tmp_path / "bad.json"
    bad.write_text("{nope")
    with pytest.raises(ValueError, match="not valid JSON"):
        fix.load_fix_config(str(bad), topology)


# --------------------------------------------------------------------------------------
# Files
# --------------------------------------------------------------------------------------


def test_read_fix_files_globs_and_skips_non_fix_lines(tmp_path):
    (tmp_path / "algo-1.log").write_text("session start\n" + msg("35=8", "37=A1") + "\n\n")
    (tmp_path / "algo-2.log").write_bytes(b"\xef\xbb\xbf" + (msg("35=8", "37=A2") + "\r\n").encode())
    (tmp_path / "other.txt").write_text(msg("35=8", "37=Z") + "\n")
    messages, files = fix.read_fix_files(["algo*.log", "algo-1.log"], str(tmp_path))
    assert [os.path.basename(f) for f in files] == ["algo-1.log", "algo-2.log"]
    assert [fix.parse_fix(m)["37"] for m in messages] == ["A1", "A2"]


# --------------------------------------------------------------------------------------
# The fold
# --------------------------------------------------------------------------------------


def test_fold_latest_state_sticky_links_and_id_history(default_config):
    algo = default_config["ALGO"]
    tape = [
        msg("35=D", "11=C1", "55=AAPL"),                                       # not an ExecutionReport
        msg("35=8", "37=A-1", "11=C1", "39=0", "38=500", "14=0", "151=500", "55=AAPL", "54=1",
            "20001=A-0", "847=VWAP"),
        msg("35=8", "37=A-1", "11=C2", "41=C1", "39=0", "38=500", "14=0", "151=500", "55=AAPL", "54=1"),
        msg("35=8", "37=A-1", "11=C2", "39=1", "38=500", "14=200", "151=300", "6=10.5", "55=AAPL", "54=1",
            "20001=WRONG"),                                                  # a later link never wins
        msg("35=8", "37=", "11=C9", "39=2"),                                  # no order id
        msg("35=8", "37=A-1", "14=bad", "39=1"),                              # a bad number is ignored
    ]
    rows, ids = fix.fold_system(algo, tape)
    assert rows == [{
        "Level": "ALGO_CHILD", "OrderId": "A-1", "AltId": "C2", "LinkId": "A-0", "Moniker": "",
        "Symbol": "AAPL", "Side": "BUY", "OrderQty": 500.0, "CumQty": 200.0, "LeavesQty": 300.0,
        "AvgPx": 10.5, "Status": "PARTIALLY_FILLED", "Destination": "VWAP",
    }]
    assert ids == [("ALGO", "A-1", "A-1"), ("ALGO", "C1", "A-1"), ("ALGO", "C2", "A-1")]


def test_fold_level_rules_equals_and_unmatched_orders(topology):
    cfg = fix.parse_fix_config(_cfg(OR={"levels": [
        {"level": "OR_CHILD", "when": {"tag": "9001", "equals": "CHILD"}, "link_tag": "20001"},
        {"level": "OR_PARENT", "when": {"tag": "9001", "equals": "PARENT"}, "link_tag": "11"},
    ]}), topology)
    tape = [
        msg("35=8", "37=P", "11=UP", "9001=PARENT", "38=100", "14=0", "151=100"),
        msg("35=8", "37=K", "11=V", "9001=CHILD", "20001=P", "38=100", "14=0", "151=100"),
        msg("35=8", "37=X", "11=Q", "9001=OTHER"),                            # matches no level: dropped
    ]
    rows, ids = fix.fold_system(cfg["OR"], tape)
    assert {(r["OrderId"], r["Level"], r["LinkId"]) for r in rows} == {("P", "OR_PARENT", "UP"), ("K", "OR_CHILD", "P")}
    assert ("OR", "Q", "X") in ids  # ids are still collected; reconcile ignores unknown orders


def test_fix_path_reconciles_exactly_like_the_table_path(topology, book, nodes, folded):
    rows, ids = folded
    fix_nodes = reference.reconcile(topology, rows, extra_ids=ids)
    assert set(fix_nodes) == set(nodes)
    diffs = [
        (key, column, fix_nodes[key][column], nodes[key][column])
        for key in nodes
        for column in nodes[key]
        if column != "AltId" and fix_nodes[key][column] != nodes[key][column]
    ]
    assert diffs == []


def test_fix_path_shows_every_scenario_break(topology, book, folded):
    rows, ids = folded
    fix_nodes = reference.reconcile(topology, rows, extra_ids=ids)
    for scenario, expected in mockdata.EXPECTED_BREAKS.items():
        system, order_id = book.scenario_roots[scenario]
        got = sorted((n["Level"], n["BreakKind"]) for n in tree_of(fix_nodes, f"{system}|{order_id}")
                     if n["BreakKind"] != "NONE")
        assert got == expected, scenario


def test_amended_clordid_links_only_through_id_history(topology, book, folded):
    rows, ids = folded
    _, client_id = book.scenario_roots[fix_mock.AMENDED_SCENARIO]
    client = next(r for r in rows if r["OrderId"] == client_id)
    router = next(r for r in rows if r["Level"] == "OR_PARENT" and r["LinkId"] + "-R1" == client["AltId"])
    key = f"OR|{router['OrderId']}"
    assert reference.reconcile(topology, rows)[key]["LinkState"] == "ORPHAN"
    linked = reference.reconcile(topology, rows, extra_ids=ids)[key]
    assert (linked["LinkState"], linked["ParentKey"]) == ("LINKED", f"RAPTOR|{client_id}")


def test_extra_ids_never_beat_current_ids(topology):
    from conftest import row

    rows = [row("CLIENT", "R1", AltId="C1"), row("CLIENT", "R2", AltId="C2"), row("OR_PARENT", "O1", "C2")]
    # R1 once carried C2 too (e.g. a reused ClOrdID): R2's current C2 still wins.
    nodes = reference.reconcile(topology, rows, extra_ids=[("RAPTOR", "C2", "R1"), ("RAPTOR", "C9", "NOPE")])
    assert nodes["OR|O1"]["ParentKey"] == "RAPTOR|R2"


# --------------------------------------------------------------------------------------
# The committed samples
# --------------------------------------------------------------------------------------


def test_committed_samples_match_the_generator(tmp_path):
    fix_mock.write_samples(str(tmp_path))
    for name in list(fix_mock.TAPE_FILES.values()) + ["fix_config.json"]:
        with open(os.path.join(SAMPLES_DIR, name), "rb") as committed, open(tmp_path / name, "rb") as fresh:
            assert committed.read() == fresh.read(), f"fix-samples/{name} is stale: regenerate (see fix-samples/README.md)"


def test_samples_folder_is_what_fix_files_mode_reads(topology, nodes):
    cfg = fix.load_fix_config(os.path.join(SAMPLES_DIR, "fix_config.json"), topology)
    tapes = {s.system: fix.read_fix_files(s.files, SAMPLES_DIR)[0] for s in cfg.systems}
    rows, ids = fix.fold_all(cfg, tapes)
    assert set(reference.reconcile(topology, rows, extra_ids=ids)) == set(nodes)


# --------------------------------------------------------------------------------------
# OTR_SOURCE / OTR_FIX_DIR / OTR_FIX_CONFIG
# --------------------------------------------------------------------------------------


def test_source_settings(tmp_path):
    assert config.source({}) == "mock"
    assert config.source({"OTR_SOURCE": " FIX-FILES "}) == "fix-files"
    with pytest.raises(ValueError, match="OTR_SOURCE"):
        config.source({"OTR_SOURCE": "kafka"})
    with pytest.raises(ValueError, match="not a directory"):
        config.fix_dir({"OTR_FIX_DIR": str(tmp_path / "missing")})
    env = {"OTR_FIX_DIR": str(tmp_path)}
    assert config.fix_config_path(env) == ""
    (tmp_path / "fix_config.json").write_text("{}")
    assert config.fix_config_path(env) == str(tmp_path / "fix_config.json")
    (tmp_path / "mine.json").write_text("{}")
    assert config.fix_config_path(dict(env, OTR_FIX_CONFIG="mine.json")) == str(tmp_path / "mine.json")
    with pytest.raises(ValueError, match="OTR_FIX_CONFIG"):
        config.fix_config_path(dict(env, OTR_FIX_CONFIG="nope.json"))


# --------------------------------------------------------------------------------------
# fix_check: the host-side dry run
# --------------------------------------------------------------------------------------


def test_fix_check_on_the_samples():
    import io

    from order_tree_recon import fix_check

    out = io.StringIO()
    assert fix_check.check(SAMPLES_DIR, out=out) == 0
    text = out.getvalue()
    assert "raptor.log: 123 messages, 88 ExecutionReports" in text
    assert "links: ROOT 34, LINKED 305, ORPHAN 1, NO_LINK 0" in text
    assert "link tag 11 = 'ALG-99999-01', not an id of any ALGO / RAPTOR order" in text


def test_fix_check_reports_a_wrong_link_tag_and_bad_inputs(tmp_path):
    import io
    import shutil

    from order_tree_recon import fix_check

    for name in fix_mock.TAPE_FILES.values():
        shutil.copy(os.path.join(SAMPLES_DIR, name), tmp_path / name)
    cfg = _cfg(ALGO={"levels": [{"level": "ALGO_CHILD", "when": {"tag": "20001"}, "link_tag": "20001"},
                                {"level": "ALGO_PARENT", "link_tag": "9999"}]})
    (tmp_path / "fix_config.json").write_text(json.dumps(cfg))
    out = io.StringIO()
    assert fix_check.check(str(tmp_path), out=out) == 0
    assert "link tag 9999 is on none of its ExecutionReports" in out.getvalue()

    (tmp_path / "broken.json").write_text('{"systems": []}')
    out = io.StringIO()
    assert fix_check.check(str(tmp_path), str(tmp_path / "broken.json"), out=out) == 1
    assert "INVALID" in out.getvalue()

    empty = tmp_path / "empty"
    empty.mkdir()
    out = io.StringIO()
    assert fix_check.check(str(empty), out=out) == 1
    assert "NO FILES MATCHED" in out.getvalue()
