"""FIX order states from AMPS SOW topics, on a bare host python (no Deephaven, no AMPS).

Parser, tag mapping, the SOW mirror (deletes, reconnect snapshots), the fold against the table
path, the committed samples, the dry-run checker, and the AMPS source's settings, message
classification and subscribe calls with a stub client. The Deephaven side is pinned against
the same twins in test_deephaven_embedded.py.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import threading
import types
from typing import Any, Dict, List

import pytest

from conftest import row, tree_of
from order_tree_recon import amps_sow, config, fixstate, fixstate_check, fixstate_mock, mockdata, reference, states
from order_tree_recon.fixstate import DELETE, SNAPSHOT_END, UPSERT

MODULE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAMPLES_DIR = os.path.join(MODULE_DIR, "fix-samples")


def _cfg(**overrides) -> dict:
    cfg = json.loads(json.dumps(fixstate.DEFAULT_FIX_CONFIG))
    for system in cfg["systems"]:
        if system["system"] in overrides:
            system.update(overrides[system["system"]])
    return cfg


@pytest.fixture(scope="module")
def fix_config() -> fixstate.FixStateConfig:
    return fixstate.load_fix_config("", config.default_topology())


@pytest.fixture(scope="module")
def mock_events(book) -> Dict[str, List[Dict[str, Any]]]:
    return fixstate_mock.events(book)


@pytest.fixture(scope="module")
def folded(fix_config, mock_events):
    return fixstate.fold_events(fix_config, mock_events)


# --------------------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("raw", [
    "35=8\x0137=A1\x0155=AAPL\x01",                                   # an AMPS fix body: no 8/9/10
    "35=8|37=A1|55=AAPL|",
    "35=8^A37=A1^A55=AAPL",
    "8=FIX.4.2\x019=20\x0135=8\x0137=A1\x0155=AAPL\x0110=123\x01",   # framed
    "20261009-13:30:00.007 : 35=8|37=A1|55=AAPL|",                    # a log prefix
    "2026-10-09 13:30:00 INFO 8=FIX.4.2|35=8|37=A1|55=AAPL|\r\n",
])
def test_parse_fix_forms(raw):
    fields = fixstate.parse_fix(raw)
    assert (fields["35"], fields["37"], fields["55"]) == ("8", "A1", "AAPL")


def test_parse_fix_first_wins_and_garbage():
    assert fixstate.parse_fix("37=A|37=B|58=x=y")["37"] == "A"
    assert fixstate.parse_fix("37=A|58=x=y")["58"] == "x=y"
    assert fixstate.parse_fix(None) == {} and fixstate.parse_fix("hello") == {}
    assert fixstate.fix_values("37=A|55=X", ["55", "999"]) == ["X", ""]


# --------------------------------------------------------------------------------------
# The mapping
# --------------------------------------------------------------------------------------


def test_default_config(topology, fix_config):
    assert fix_config.topics == {"RAPTOR": ("SOW/RAPTOR/ORDERS",), "ALGO": ("SOW/ALGO/ORDERS",),
                                 "OR": ("SOW/OR/ORDERS",)}
    algo = fix_config.state["ALGO"]
    assert algo.order_id == "T37" and algo.alt_id == "T11" and algo.arrival == "Arrival"
    assert algo.source("Destination") == "T847" and algo.source("Symbol") == "T55"
    assert algo.avg_px_needs_fill and dict(algo.side_codes)["1"] == "BUY"
    assert fix_config.tags("ALGO") == ("37", "11", "41", "1", "55", "54", "38", "14", "151", "6", "39", "847", "26009")
    assert sorted(r.level for s in fix_config.systems for r in s.levels) == sorted(topology.names)


@pytest.mark.parametrize("overrides, message", [
    ({"ALGO": {"system": "NOPE"}}, "not in the topology"),
    ({"ALGO": {"order_id_tag": "OrderID"}}, "FIX tag number"),
    ({"ALGO": {"tags": {"Qty": "38"}}}, "not a canonical column"),
    ({"ALGO": {"tags": {"Symbol": "abc"}}}, "FIX tag number"),
    ({"ALGO": {"levels": [{"level": "ALGO_PARENT", "link_tag": "11"}]}}, "not mapped"),
    ({"RAPTOR": {"levels": [{"level": "CLIENT", "link_tag": "11"}]}}, "root level"),
    ({"OR": {"levels": [{"level": "OR_CHILD", "when": {"tag": "26009"}, "link_tag": "26009"},
                        {"level": "OR_PARENT"}]}}, "needs a parent link"),
    ({"OR": {"levels": [{"level": "OR_PARENT", "link_tag": "11"},
                        {"level": "OR_CHILD", "when": {"tag": "26009"}, "link_tag": "26009"}]}}, "must be the last"),
    ({"OR": {"levels": [{"level": "OR_CHILD", "when": {"tag": "9", "equals": "a`b"}, "link_tag": "26009"},
                        {"level": "OR_PARENT", "link_tag": "11"}]}}, "without ` or"),
    ({"OR": {"levels": [{"level": "OR_CHILD", "when": {"field": "26009"}, "link_tag": "26009"},
                        {"level": "OR_PARENT", "link_tag": "11"}]}}, "when must be"),
    ({"OR": {"topics": ["SOW/RAPTOR/ORDERS"]}}, "also belong to another system"),
    ({"OR": {"topics": "router.orders"}}, "list of non-empty strings"),
    ({"OR": {"topics": ["bad topic"]}}, "not a valid name"),
    ({"OR": {"filter": ""}}, "content filter"),
    ({"OR": {"side_codes": "bloomberg"}}, "\"fix\" or an object"),
    ({"OR": {"typo": 1}}, "unknown key"),
])
def test_config_validation(topology, overrides, message):
    with pytest.raises(ValueError, match=message):
        fixstate.parse_fix_config(_cfg(**overrides), topology)


def test_config_overrides_nulls_and_int_tags(topology):
    cfg = fixstate.parse_fix_config(_cfg(RAPTOR={"order_id_tag": 37, "alt_id_tag": None, "tags": {"Destination": None},
                                                 "sequence_tag": "60", "filter": "/39 != '8'"}), topology)
    raptor = cfg.state["RAPTOR"]
    assert raptor.order_id == "T37" and raptor.alt_id is None and raptor.source("Destination") is None
    assert raptor.sequence == "T60" and cfg.filters["RAPTOR"] == "/39 != '8'"


def test_load_config_bom_and_bad_json(tmp_path, topology):
    good = tmp_path / "fix_config.json"
    good.write_bytes(b"\xef\xbb\xbf" + json.dumps(fixstate.DEFAULT_FIX_CONFIG).encode())
    assert fixstate.load_fix_config(str(good), topology) == fixstate.load_fix_config(None, topology)
    bad = tmp_path / "bad.json"
    bad.write_text("{nope")
    with pytest.raises(ValueError, match="not valid JSON"):
        fixstate.load_fix_config(str(bad), topology)


# --------------------------------------------------------------------------------------
# The generic fold: ordering, numbers, AvgPx
# --------------------------------------------------------------------------------------


def test_latest_state_ordering(topology):
    cfg = fixstate.parse_fix_config(_cfg(OR={"sequence_tag": "34"}), topology)
    system = cfg.state["OR"]
    msgs = [("9", 1, "10"), ("12", 2, "30"), ("10", 3, "20"), ("12", 4, "40")]  # (seq, arrival, cum)
    rows = [{"T37": "O-1", "T11": "U", "T34": seq, "T38": "100", "T14": cum, "T151": "0", "Arrival": str(arr)}
            for seq, arr, cum in msgs]
    out, _ = states.fold_system(system, rows)
    assert out[0]["CumQty"] == 40.0  # seq 12 beats 9 and 10 numerically; the tie goes to the later arrival
    no_seq = fixstate.parse_fix_config(_cfg(), topology).state["OR"]
    shuffled = [rows[3], rows[0], rows[2], rows[1]]
    out, _ = states.fold_system(no_seq, shuffled)
    assert out[0]["CumQty"] == 40.0  # no sequence: the highest arrival, whatever the row order


def test_numbers_and_avg_px_need_a_fill(topology, fix_config):
    system = fix_config.state["RAPTOR"]
    base = {"T37": "R", "T11": "C", "T55": "X", "T54": "5", "T39": "Z", "Arrival": "1"}
    out, _ = states.fold_system(system, [dict(base, T38="1,000", T14="0", T151=" 5 ", T6="0")])
    r = out[0]
    assert (r["OrderQty"], r["CumQty"], r["LeavesQty"], r["AvgPx"]) == (None, 0.0, 5.0, None)
    assert (r["Side"], r["Status"]) == ("SELL_SHORT", "Z")  # unknown codes pass through
    out, _ = states.fold_system(system, [dict(base, T38="100", T14="100", T151="0", T6="12.5")])
    assert out[0]["AvgPx"] == 12.5


# --------------------------------------------------------------------------------------
# The SOW mirror
# --------------------------------------------------------------------------------------

A = "35=8|37=A|11=A|55=X|54=1|38=100|14=0|151=100|39=0|"
B = "35=8|37=B|11=B|55=X|54=1|38=100|14=0|151=100|39=0|"
C = "35=8|37=C|11=C|55=X|54=1|38=100|14=0|151=100|39=0|"


def _ids(current):
    return [fixstate.parse_fix(e["RawFix"])["37"] for e in current]


def test_mirror_latest_wins_and_delete_removes():
    E = fixstate.event
    events = [E("t", A, 1, sow_key="ka"), E("t", B, 2, sow_key="kb"), E("t", C, 3, sow_key="kc"),
              E("t", "", 4, action=SNAPSHOT_END), E("t", B.replace("14=0", "14=50"), 5, sow_key="kb"),
              E("t", "", 6, action=DELETE, sow_key="kc")]
    current = fixstate.sow_current(events)
    assert _ids(current) == ["A", "B"]
    assert fixstate.parse_fix(current[1]["RawFix"])["14"] == "50"


def test_mirror_reconnect_snapshot_drops_what_was_deleted_meanwhile():
    E = fixstate.event
    first = [E("t", A, 1, sow_key="ka"), E("t", B, 2, sow_key="kb"), E("t", "", 3, action=SNAPSHOT_END)]
    # Reconnected: snapshot 2 re-sends only B (A was deleted while we were disconnected).
    resent = first + [E("t", B, 4, epoch=2, sow_key="kb")]
    assert _ids(fixstate.sow_current(resent)) == ["A", "B"]  # until the snapshot completes
    done = resent + [E("t", "", 5, action=SNAPSHOT_END, epoch=2)]
    assert _ids(fixstate.sow_current(done)) == ["B"]
    # A snapshot end on another topic does not touch this one.
    assert _ids(fixstate.sow_current(resent + [E("other", "", 5, action=SNAPSHOT_END, epoch=9)])) == ["A", "B"]


def test_mirror_keys_on_the_order_id_without_a_sow_key():
    E = fixstate.event
    events = [E("t", A, 1), E("t", A.replace("14=0", "14=10"), 2), E("t", "garbage", 3)]
    current = fixstate.sow_current(events)
    assert len(current) == 1 and fixstate.parse_fix(current[0]["RawFix"])["14"] == "10"


# --------------------------------------------------------------------------------------
# The fold against the table path
# --------------------------------------------------------------------------------------


def test_fix_path_reconciles_like_the_table_path(topology, book, nodes, folded):
    rows, ids = folded
    fix_nodes = reference.reconcile(topology, rows, extra_ids=ids)
    assert set(fix_nodes) == set(nodes)
    diffs = [(k, c) for k in nodes for c in nodes[k] if c != "AltId" and fix_nodes[k][c] != nodes[k][c]]
    assert diffs == []
    # AltId is the latest ClOrdID on the FIX path: Raptor's differs only for the amended order.
    _, amended = book.scenario_roots[fixstate_mock.AMENDED_SCENARIO]
    raptor_alt = [k for k in nodes if k.startswith("RAPTOR") and fix_nodes[k]["AltId"] != nodes[k]["AltId"]]
    assert raptor_alt == [f"RAPTOR|{amended}"]


def test_fix_path_shows_every_scenario_break(topology, book, folded):
    rows, ids = folded
    fix_nodes = reference.reconcile(topology, rows, extra_ids=ids)
    for scenario, expected in mockdata.EXPECTED_BREAKS.items():
        system, order_id = book.scenario_roots[scenario]
        got = sorted((n["Level"], n["BreakKind"]) for n in tree_of(fix_nodes, f"{system}|{order_id}")
                     if n["BreakKind"] != "NONE")
        assert got == expected, scenario


def test_amended_clordid_links_through_origclordid_or_history(topology, book, mock_events):
    _, client = book.scenario_roots[fixstate_mock.AMENDED_SCENARIO]
    router = next(e for e in mock_events["OR"] if "|11=BLUEFIN-00003|" in e["RawFix"])
    key = f"OR|{fixstate.parse_fix(router['RawFix'])['37']}"

    def link(id_tags, history):
        cfg = fixstate.parse_fix_config(_cfg(RAPTOR={"id_tags": id_tags}), topology)
        rows, ids = fixstate.fold_events(cfg, mock_events)
        if not history:  # only the ids of each order's current state
            current = {s: fixstate.sow_current(mock_events[s]) for s in mock_events}
            ids = [i for s in cfg.systems for i in states.fold_system(s, fixstate.state_rows(current[s.system], cfg.tags(s.system)))[1]]
        return reference.reconcile(topology, rows, extra_ids=ids)[key]

    assert link(["37", "11", "41"], history=False)["ParentKey"] == f"RAPTOR|{client}"  # 41 on the latest state
    assert link(["37", "11"], history=True)["ParentKey"] == f"RAPTOR|{client}"        # the id history
    assert link(["37", "11"], history=False)["LinkState"] == "ORPHAN"


def test_extra_ids_never_beat_current_ids(topology):
    rows = [row("CLIENT", "R1", AltId="C1"), row("CLIENT", "R2", AltId="C2"), row("OR_PARENT", "O1", "C2")]
    nodes = reference.reconcile(topology, rows, extra_ids=[("RAPTOR", "C2", "R1"), ("RAPTOR", "C9", "NOPE")])
    assert nodes["OR|O1"]["ParentKey"] == "RAPTOR|R2"


# --------------------------------------------------------------------------------------
# Files and the committed samples
# --------------------------------------------------------------------------------------


def test_file_events(tmp_path, topology, fix_config):
    (tmp_path / "SOW.RAPTOR.ORDERS.fix").write_bytes(
        b"\xef\xbb\xbf# a SOW dump\n" + (A + "\r\n\n" + B.replace("|", "\x01") + "\n").encode())
    events, files = fixstate.file_events(fix_config, str(tmp_path))
    assert [os.path.basename(f) for f in files["RAPTOR"]] == ["SOW.RAPTOR.ORDERS.fix"]
    # a file name can't hold SOW/RAPTOR/ORDERS's slashes: the topic label is the name sans .fix
    assert [(e["Topic"], e["Arrival"], e["Action"]) for e in events["RAPTOR"]] == [
        ("SOW.RAPTOR.ORDERS", 1, UPSERT), ("SOW.RAPTOR.ORDERS", 2, UPSERT)]
    assert events["ALGO"] == [] and files["ALGO"] == []


def test_committed_samples_match_the_generator(tmp_path):
    fixstate_mock.write_samples(str(tmp_path))
    names = sorted({fixstate_mock.sample_file(t) for t in fixstate_mock.TOPIC_OF_LEVEL.values()}) + ["fix_config.json"]
    for name in names:
        with open(os.path.join(SAMPLES_DIR, name), "rb") as committed, open(tmp_path / name, "rb") as fresh:
            assert committed.read() == fresh.read(), f"fix-samples/{name} is stale: regenerate (see its README)"
    with open(os.path.join(SAMPLES_DIR, "fix_config.json"), encoding="utf-8") as handle:
        assert json.load(handle) == fixstate.DEFAULT_FIX_CONFIG


def test_samples_read_from_files_fold_like_the_mock_events(fix_config, mock_events, folded):
    events, _ = fixstate.file_events(fix_config, SAMPLES_DIR)

    def key(r):
        return (r["Level"], r["OrderId"])

    assert sorted(fixstate.fold_events(fix_config, events)[0], key=key) == sorted(folded[0], key=key)


def test_publish_sends_every_message_soh_delimited(book):
    class Client:
        def __init__(self):
            self.sent, self.calls = [], []

        def connect(self, uri): self.calls.append(("connect", uri))
        def logon(self): self.calls.append("logon")
        def publish(self, topic, data): self.sent.append((topic, data))
        def publish_flush(self): self.calls.append("flush")
        def close(self): self.calls.append("close")

    client = Client()
    count = fixstate_mock.publish("tcp://amps:9007/amps/fix", book, client=client)
    assert count == len(client.sent) == sum(len(v) for v in fixstate_mock.messages(book).values())
    assert client.calls == [("connect", "tcp://amps:9007/amps/fix"), "logon", "flush", "close"]
    assert "|" not in client.sent[0][1] and fixstate.parse_fix(client.sent[0][1])["37"] == "RPT-00001"


# --------------------------------------------------------------------------------------
# fixstate_check
# --------------------------------------------------------------------------------------


def test_check_on_the_sample_files():
    out = io.StringIO()
    assert fixstate_check.check(SAMPLES_DIR, out=out) == 0
    text = out.getvalue()
    assert "SOW.ALGO.ORDERS: 243" in text
    assert "links: ROOT 34, LINKED 305, ORPHAN 1, NO_LINK 0" in text
    assert "tag 11 = 'ALG-99999-01', not an id of any ALGO / RAPTOR order" in text
    assert "MISSING" not in text


def test_check_flags_a_wrong_link_tag(tmp_path):
    for name in os.listdir(SAMPLES_DIR):
        if name.endswith(".fix"):
            shutil.copy(os.path.join(SAMPLES_DIR, name), tmp_path / name)
    cfg = _cfg(ALGO={"levels": [{"level": "ALGO_CHILD", "when": {"tag": "26009"}, "link_tag": "26009"},
                                {"level": "ALGO_PARENT", "link_tag": "9999"}]})
    (tmp_path / "fix_config.json").write_text(json.dumps(cfg))
    out = io.StringIO()
    assert fixstate_check.check(str(tmp_path), out=out) == 0
    text = out.getvalue()
    assert "MISSING: tag 9999 (parent link of ALGO_PARENT) is in none of the 243 messages" in text
    assert "tag 9999 is not in its latest state" in text


def test_check_bad_inputs(tmp_path):
    (tmp_path / "broken.json").write_text('{"systems": []}')
    out = io.StringIO()
    assert fixstate_check.check(str(tmp_path), str(tmp_path / "broken.json"), out=out) == 1
    assert "INVALID" in out.getvalue()
    out = io.StringIO()
    assert fixstate_check.check(str(tmp_path), out=out) == 1
    assert "NO FILES MATCHED" in out.getvalue()


def test_check_against_amps_uses_a_sow_query(book):
    seen = {}

    def fake_amps(cfg, uri):
        seen["uri"] = uri
        return fixstate_mock.events(book)

    out = io.StringIO()
    assert fixstate_check.check(amps_uri="tcp://amps:9007/amps/fix", out=out, amps_events=fake_amps) == 0
    assert seen["uri"] == "tcp://amps:9007/amps/fix"
    assert "source: AMPS sow query" in out.getvalue() and "LINKED 305" in out.getvalue()

    def failing(cfg, uri):
        raise ConnectionError("refused")

    out = io.StringIO()
    assert fixstate_check.check(amps_uri="tcp://nowhere:1/amps/fix", out=out, amps_events=failing) == 1
    assert "AMPS: FAILED -- ConnectionError: refused" in out.getvalue()


def test_amps_sow_events_runs_a_sow_command_per_topic(monkeypatch, fix_config):
    class Command:
        def __init__(self, name): self.name, self.topic, self.filter = name, None, None
        def set_topic(self, t): self.topic = t; return self
        def set_batch_size(self, n): return self
        def set_timeout(self, t): return self
        def set_filter(self, f): self.filter = f; return self

    class Message:
        def __init__(self, command, data="", key=""): self.c, self.d, self.k = command, data, key
        def get_command(self): return self.c
        def get_data(self): return self.d
        def get_sow_key(self): return self.k

    class Client:
        def __init__(self, name): self.commands = []
        def connect(self, uri): pass
        def logon(self): pass
        def close(self): pass
        def execute(self, command):
            self.commands.append(command)
            return [Message("group_begin"), Message("sow", A.replace("|", "\x01"), "k1"), Message("group_end")]

    monkeypatch.setitem(sys.modules, "AMPS", types.SimpleNamespace(Command=Command))
    holder = {}
    events = fixstate_check.amps_sow_events(fix_config, "tcp://x", client_factory=lambda n: holder.setdefault("c", Client(n)))
    assert [c.topic for c in holder["c"].commands] == ["SOW/RAPTOR/ORDERS", "SOW/ALGO/ORDERS", "SOW/OR/ORDERS"]
    assert all(c.name == "sow" for c in holder["c"].commands)
    assert [(e["Topic"], e["SowKey"]) for e in events["ALGO"]] == [("SOW/ALGO/ORDERS", "k1")]


# --------------------------------------------------------------------------------------
# amps_sow: settings, classification, subscribe calls (stub client, no Deephaven)
# --------------------------------------------------------------------------------------


class _Msg:
    def __init__(self, command, data="", key=""):
        self.command, self.data, self.key = command, data, key

    def get_command(self): return self.command
    def get_data(self): return self.data
    def get_sow_key(self): return self.key


def test_amps_settings_from_env():
    s = amps_sow.AmpsSettings.from_env({"OTR_AMPS_URI": "tcp://u:secret@a:9007/amps/fix, tcp://b:9007/amps/fix",
                                        "OTR_AMPS_BATCH_SIZE": "500"})
    assert s.uris == ("tcp://u:secret@a:9007/amps/fix", "tcp://b:9007/amps/fix") and s.batch_size == 500
    assert "secret" not in s.describe() and "u:****@a" in s.describe()
    with pytest.raises(ValueError, match="OTR_AMPS_URI"):
        amps_sow.AmpsSettings.from_env({})
    with pytest.raises(ValueError, match="OTR_AMPS_TIMEOUT_MS"):
        amps_sow.AmpsSettings.from_env({"OTR_AMPS_URI": "tcp://a", "OTR_AMPS_TIMEOUT_MS": "soon"})


def test_handler_classifies_amps_commands(fix_config):
    source = amps_sow.AmpsSowSource(fix_config, amps_sow.AmpsSettings(["tcp://a"]))
    h = source.handler("RAPTOR", "SOW/RAPTOR/ORDERS")
    for m in [_Msg("group_begin"), _Msg("sow", A, "ka"), _Msg("sow", B, "kb"), _Msg("group_end"), _Msg("ack"),
              _Msg("publish", B, "kb"), _Msg("oof", B, "kb"), _Msg("sow_delete", "", "ka"), _Msg("heartbeat"),
              _Msg("group_begin"), _Msg("sow", A, "ka"), _Msg("group_end")]:
        h(m)
    rows = source.buffers["RAPTOR"].drain()
    assert [(r[2], r[3], r[4], r[1]) for r in rows] == [
        (UPSERT, 1, 1, "ka"), (UPSERT, 1, 2, "kb"), (SNAPSHOT_END, 1, 3, ""), (UPSERT, 1, 4, "kb"),
        (DELETE, 1, 5, "kb"), (DELETE, 1, 6, "ka"), (UPSERT, 2, 7, "ka"), (SNAPSHOT_END, 2, 8, ""),
    ]
    assert rows[0][5] == A and rows[2][5] == ""
    status = {r["Topic"]: r for r in source.status()}["SOW/RAPTOR/ORDERS"]
    assert (status["Epoch"], status["snapshots"], status["last_snapshot_records"], status[DELETE]) == (2, 2, 1, 2)
    # The events the handler produced fold through the mirror twin: A came back in snapshot 2.
    events = [dict(zip(fixstate.EVENT_COLUMNS, r)) for r in rows]
    assert _ids(fixstate.sow_current(events)) == ["A"]


def test_connect_subscribes_every_topic_with_oof(fix_config, topology):
    calls: List[Any] = []

    class Client:
        def add_connection_state_listener(self, f): calls.append("listener")
        def connect_and_logon(self): calls.append("logon")
        def execute_async(self, command, handler): calls.append(("sub", command)); return f"s{len(calls)}"
        def unsubscribe(self, sub_id): calls.append(("unsub", sub_id))
        def close(self): calls.append("close")

    cfg = fixstate.parse_fix_config(_cfg(OR={"filter": "/39 != '2'"}), topology)
    source = amps_sow.AmpsSowSource(cfg, amps_sow.AmpsSettings(["tcp://a"]), client_factory=lambda s: Client(),
                                    command_factory=lambda topic, flt, s: (topic, flt))
    source.connect()
    subs = [c[1] for c in calls if isinstance(c, tuple) and c[0] == "sub"]
    assert subs == [("SOW/RAPTOR/ORDERS", None), ("SOW/ALGO/ORDERS", None), ("SOW/OR/ORDERS", "/39 != '2'")]
    assert calls[:2] == ["listener", "logon"] and source.connected
    source.stop()
    assert calls[-1] == "close" and sum(1 for c in calls if isinstance(c, tuple) and c[0] == "unsub") == 3


def test_default_command_is_sow_and_subscribe_with_oof_and_keys(monkeypatch):
    class Command:
        def __init__(self, name): self.calls = [("new", name)]
        def __getattr__(self, attr):
            def setter(value):
                self.calls.append((attr, value))
                return self
            return setter

    monkeypatch.setitem(sys.modules, "AMPS", types.SimpleNamespace(Command=Command))
    command = amps_sow._default_command_factory("SOW/ALGO/ORDERS", "/55 = 'AAPL'", amps_sow.AmpsSettings(["tcp://a"]))
    assert command.calls == [("new", "sow_and_subscribe"), ("set_topic", "SOW/ALGO/ORDERS"),
                             ("set_options", "oof,send_keys"), ("set_batch_size", 1000), ("set_timeout", 10000),
                             ("set_filter", "/55 = 'AAPL'")]


def test_background_connect_retries_until_amps_answers(fix_config):
    attempts = []
    connected = threading.Event()

    class Client:
        def add_connection_state_listener(self, f): pass
        def connect_and_logon(self):
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("not yet")
        def execute_async(self, command, handler):
            connected.set()
            return "s"
        def unsubscribe(self, sub_id): pass
        def close(self): pass

    source = amps_sow.AmpsSowSource(fix_config, amps_sow.AmpsSettings(["tcp://a"]), client_factory=lambda s: Client(),
                                    command_factory=lambda t, f, s: t, retry_seconds=0.01)
    source.tables = {"stub": object()}  # skip the Deephaven tables
    source.start(background=True)
    assert connected.wait(5) and len(attempts) == 3 and source.last_error == "ConnectionError: not yet"
    source.stop()


def test_event_buffer_blocks_then_drains():
    buffer = amps_sow.EventBuffer(max_pending=1)
    assert buffer.offer(("a",))
    assert not buffer.offer(("b",), timeout=0.01)  # full: waits, then gives up
    threading.Timer(0.05, buffer.drain).start()
    assert buffer.offer(("c",), timeout=5)  # unblocked by the drain
    buffer.close()
    assert not buffer.offer(("d",))


# --------------------------------------------------------------------------------------
# OTR_SOURCE / OTR_FIX_DIR / OTR_FIX_CONFIG
# --------------------------------------------------------------------------------------


def test_source_settings(tmp_path):
    assert config.source({}) == "mock"
    assert config.source({"OTR_SOURCE": " AMPS "}) == "amps"
    assert config.source({"OTR_SOURCE": "fix-files"}) == "fix-files"
    with pytest.raises(ValueError, match="OTR_SOURCE"):
        config.source({"OTR_SOURCE": "kafka"})
    with pytest.raises(ValueError, match="not a directory"):
        config.fix_dir({"OTR_FIX_DIR": str(tmp_path / "missing")})
    assert config.fix_config_path({"OTR_FIX_DIR": str(tmp_path / "missing")}) == ""  # amps: built-in mapping
    env = {"OTR_FIX_DIR": str(tmp_path)}
    (tmp_path / "fix_config.json").write_text("{}")
    assert config.fix_config_path(env) == str(tmp_path / "fix_config.json")
    (tmp_path / "mine.json").write_text("{}")
    assert config.fix_config_path(dict(env, OTR_FIX_CONFIG="mine.json")) == str(tmp_path / "mine.json")
    with pytest.raises(ValueError, match="OTR_FIX_CONFIG"):
        config.fix_config_path(dict(env, OTR_FIX_CONFIG="nope.json"))


# --------------------------------------------------------------------------------------
# amps_sow: heartbeat, connection flag, one client per name
# --------------------------------------------------------------------------------------


def test_heartbeat_setting_reaches_the_client(monkeypatch):
    s = amps_sow.AmpsSettings.from_env({"OTR_AMPS_URI": "tcp://a"})
    assert s.heartbeat == 5 and "heartbeat 5 s" in s.describe()
    assert amps_sow.AmpsSettings.from_env({"OTR_AMPS_URI": "tcp://a", "OTR_AMPS_HEARTBEAT": "0"}).heartbeat == 0
    with pytest.raises(ValueError, match="OTR_AMPS_HEARTBEAT"):
        amps_sow.AmpsSettings.from_env({"OTR_AMPS_URI": "tcp://a", "OTR_AMPS_HEARTBEAT": "-1"})

    calls: List[Any] = []

    class HAClient:
        def __init__(self, name): calls.append(("name", name))
        def set_server_chooser(self, chooser): pass
        def set_heartbeat(self, seconds): calls.append(("heartbeat", seconds))

    class Chooser:
        def add(self, uri): pass

    monkeypatch.setitem(sys.modules, "AMPS", types.SimpleNamespace(HAClient=HAClient, DefaultServerChooser=Chooser))
    amps_sow._default_client_factory(s)
    assert ("heartbeat", 5) in calls
    calls.clear()
    amps_sow._default_client_factory(amps_sow.AmpsSettings(["tcp://a"], heartbeat=0))
    assert not any(c[0] == "heartbeat" for c in calls)


def test_connection_state_listener_tracks_the_bool_the_client_passes(fix_config):
    source = amps_sow.AmpsSowSource(fix_config, amps_sow.AmpsSettings(["tcp://a"]))
    source._on_connection_state(True)
    assert source.connected
    source._on_connection_state(False)  # a disconnect must not leave the flag up
    assert not source.connected


def test_start_source_never_runs_two_clients_under_one_name(monkeypatch, fix_config):
    started = []

    class FakeSource:
        def __init__(self, config, settings, **kwargs):
            self.config, self.settings, self.stopped = config, settings, False

        def start(self, background=True):
            started.append(self)

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(amps_sow, "AmpsSowSource", FakeSource)
    monkeypatch.setattr(amps_sow, "_ACTIVE", [])
    first = amps_sow.start_source(fix_config, amps_sow.AmpsSettings(["tcp://a"]))
    assert amps_sow.start_source(fix_config, amps_sow.AmpsSettings(["tcp://a"])) is first  # a re-run reuses it
    second = amps_sow.start_source(fix_config, amps_sow.AmpsSettings(["tcp://b"]))  # same name, other broker
    assert first.stopped and amps_sow._ACTIVE == [second]
    third = amps_sow.start_source(fix_config, amps_sow.AmpsSettings(["tcp://b"], client_name="dh-2"))
    assert not second.stopped and amps_sow._ACTIVE == [second, third] and len(started) == 3


def test_a_shared_topic_splits_parents_and_children_on_tag_26009(topology, fix_config):
    """SOW/ALGO/ORDERS holds parents and children: a child names its parent in 26009, a
    parent leaves it null -- absent or empty (``26009=``), both read as null."""
    msgs = [
        "35=8|37=AP-1|11=RPT-1|55=AAPL|54=1|38=300|14=0|151=300|39=0|",             # 26009 absent
        "35=8|37=AP-2|11=RPT-2|26009=|55=AAPL|54=1|38=300|14=0|151=300|39=0|",      # 26009 empty
        "35=8|37=AC-1|11=AC-1|26009=AP-1|55=AAPL|54=1|38=300|14=0|151=300|39=0|",   # a child
    ]
    events = {"ALGO": [fixstate.event("SOW/ALGO/ORDERS", raw, i + 1) for i, raw in enumerate(msgs)]}
    rows, _ = fixstate.fold_events(fix_config, events)
    levels = {r["OrderId"]: (r["Level"], r["LinkId"]) for r in rows}
    assert levels == {"AP-1": ("ALGO_PARENT", "RPT-1"), "AP-2": ("ALGO_PARENT", "RPT-2"),
                      "AC-1": ("ALGO_CHILD", "AP-1")}
