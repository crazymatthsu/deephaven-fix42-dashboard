"""Order tree over AMPS, end to end: FIX order states -> AMPS SOW topics -> Deephaven -> pydeephaven.

Run it through ``run_e2e.sh``: it brings up the order-tree app with ``OTR_SOURCE=amps``
(``docker/docker-compose.order-tree.yml`` + ``docker-compose.order-tree-amps.yml``, profile
``broker``: a real AMPS from your local image, loaded with ``order-tree-recon/amps/amps-config.xml``)
from ``down -v``, then runs this file. Running ``pytest`` directly works against a stack that
is already up; without one every test skips, saying what is missing.

The test plays Raptor, the algo server and the router. It publishes FIX order-state
messages to the five SOW topics of ``fixstate.DEFAULT_FIX_CONFIG`` (the mapping the
container mounts from ``fix-samples/``) and ``sow_delete``s records, and it keeps its own
log of every event the subscriber should see (:class:`Book`). The oracle is the module's
pure-python twin of the FIX path: ``fixstate.fold_events`` (SOW mirror + latest-state fold +
id history) feeding ``reference.reconcile``. After every step the engine's ``otr_recon``
must equal it row for row, column for column. Every step changes the book, so the state
before a step can never satisfy the check for it.

1. the app subscribed to the five topics; an empty SOW is an empty book;
2. the mock book's order states in send order (NEW, cancel/replace, partial and final
   fills, cancels: ``fixstate_mock.send_order``) match the reference, and so do the ladder,
   the exposure table and the query API;
3. a venue fill climbs the chain one hop at a time: each hop's edge is ``UNBOOKED_FILL``
   until the next system up publishes the fill, and ``NetUnbookedQty`` is 100 until the
   client order does;
4. a family published child-first (router, then algo, then Raptor) is ``ORPHAN`` and
   unattributed until its parents land, then links under its moniker;
5. ``sow_delete`` of a venue order (an OOF to the subscriber) removes it, and its router
   parent shows the fill its children no longer explain (``PHANTOM_FILL``) until it is
   republished;
6. a garbled quantity folds to the same null on both sides, and the feed lives on;
7. a restarted Deephaven rebuilds the identical book from one snapshot per topic;
8. Deephaven paused past the AMPS heartbeat: AMPS drops it, a delete and a fill happen
   meanwhile, and the re-snapshot (a new epoch) drops the deleted record and lands the fill;
9. a restarted AMPS (SOW files on its volume) is re-subscribed and the feed resumes.

Environment (``run_e2e.sh`` sets them):

==========================  ======================================  ========================
``OTRE2E_DH_HOST/PORT``     Deephaven gRPC                          ``localhost`` / 10020
``OTRE2E_AMPS_URI``         where the test publishes                ``tcp://localhost:39017/amps/fix``
``OTRE2E_FIX_CONFIG``       the mapping the server runs with        ``fix-samples/fix_config.json``
``OTRE2E_SEED/FAMILIES``    the mock book                           42 / 24
``OTRE2E_DH_CONTAINER``     restarted / paused by tests 7-8         ``otr-e2e-deephaven``
``OTRE2E_AMPS_CONTAINER``   restarted by test 9                     ``otr-e2e-amps``
``OTRE2E_SETTLE_TIMEOUT``   seconds for the engine to converge      60
``OTRE2E_RESTART_TIMEOUT``  seconds for a restarted server          300
``CONTAINER_CLI``           ``podman`` or ``docker``                ``podman``
==========================  ======================================  ========================
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from typing import Any, Dict, List, Mapping, Optional, Tuple

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
MODULE_DIR = os.path.dirname(HERE)
MODULE_SRC = os.path.join(MODULE_DIR, "src")
if MODULE_SRC not in sys.path:
    sys.path.insert(0, MODULE_SRC)

# The oracle is the shipped pure-python twin of the FIX path, not a copy of its formulas.
from order_tree_recon import config, fixstate, fixstate_mock, mockdata, reference  # noqa: E402
from order_tree_recon.amps_sow import DEFAULT_HEARTBEAT  # noqa: E402

DH_HOST = os.environ.get("OTRE2E_DH_HOST", "localhost")
DH_PORT = int(os.environ.get("OTRE2E_DH_PORT", "10020"))
AMPS_URI = os.environ.get("OTRE2E_AMPS_URI", "tcp://localhost:39017/amps/fix")
FIX_CONFIG_PATH = os.environ.get("OTRE2E_FIX_CONFIG", os.path.join(MODULE_DIR, "fix-samples", "fix_config.json"))
SEED = int(os.environ.get("OTRE2E_SEED", "42"))
FAMILIES = int(os.environ.get("OTRE2E_FAMILIES", "24"))
DH_CONTAINER = os.environ.get("OTRE2E_DH_CONTAINER", "otr-e2e-deephaven")
AMPS_CONTAINER = os.environ.get("OTRE2E_AMPS_CONTAINER", "otr-e2e-amps")
CONTAINER_CLI = os.environ.get("CONTAINER_CLI", "podman")
SETTLE_TIMEOUT = float(os.environ.get("OTRE2E_SETTLE_TIMEOUT", "60"))
RESTART_TIMEOUT = float(os.environ.get("OTRE2E_RESTART_TIMEOUT", "300"))

TOL = config.DEFAULT_QTY_TOL
TOPOLOGY = config.default_topology()
FIX_CONFIG = fixstate.load_fix_config(FIX_CONFIG_PATH, TOPOLOGY)
#: topic -> system, from the mapping the server runs with.
SYSTEM_OF_TOPIC: Dict[str, str] = {
    topic: system.system for system in FIX_CONFIG.systems for topic in FIX_CONFIG.topics[system.system]
}
ALL_TOPICS: Tuple[str, ...] = tuple(SYSTEM_OF_TOPIC)
#: The level a mock order is published from (fixstate_mock's topic layout).
TOPIC_OF_LEVEL = fixstate_mock.TOPIC_OF_LEVEL

#: Run server side to turn ``amps_source.status()`` into a table pydeephaven can read.
STATUS_SCRIPT = """
from deephaven import new_table as _e2e_nt
from deephaven.column import long_col as _e2e_lc, string_col as _e2e_sc
_e2e_rows = amps_source.status()
_e2e_status = _e2e_nt([
    _e2e_sc("Topic", [r["Topic"] for r in _e2e_rows]),
    _e2e_sc("System", [r["System"] for r in _e2e_rows]),
    _e2e_lc("Epoch", [int(r["Epoch"]) for r in _e2e_rows]),
    _e2e_lc("Snapshots", [int(r["snapshots"]) for r in _e2e_rows]),
    _e2e_lc("Upserts", [int(r["UPSERT"]) for r in _e2e_rows]),
    _e2e_lc("Deletes", [int(r["DELETE"]) for r in _e2e_rows]),
    _e2e_lc("Connected", [int(bool(amps_source.connected))] * len(_e2e_rows)),
])
"""


# --------------------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------------------


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def _amps_host_port() -> Tuple[str, int]:
    hostport = AMPS_URI.split("://", 1)[1].split("/", 1)[0]
    host, port = hostport.rsplit(":", 1)
    return host, int(port)


@pytest.fixture(scope="module")
def stack():
    try:
        import AMPS  # noqa: F401
        import pydeephaven  # noqa: F401
    except ImportError as exc:
        pytest.skip(f"client libraries missing ({exc}); run through run_e2e.sh")
    if not _port_open(DH_HOST, DH_PORT):
        pytest.skip(f"no Deephaven at {DH_HOST}:{DH_PORT}; run through run_e2e.sh")
    if not _port_open(*_amps_host_port()):
        pytest.skip(f"no AMPS at {AMPS_URI}; run through run_e2e.sh")
    return True


class Dh:
    """A pydeephaven session that survives the server restarts the suite causes."""

    def __init__(self) -> None:
        self._session: Any = None

    @property
    def session(self) -> Any:
        if self._session is None:
            from pydeephaven import Session

            self._session = Session(host=DH_HOST, port=DH_PORT)
        return self._session

    def reset(self) -> None:
        if self._session is not None:
            try:
                self._session.close()
            except Exception:  # noqa: BLE001 - the server under it may be gone
                pass
        self._session = None

    def rows(self, name: str) -> List[Dict[str, Any]]:
        return self.session.open_table(name).to_arrow().to_pylist()

    def script(self, code: str) -> None:
        self.session.run_script(code)

    def status(self) -> Dict[str, Dict[str, Any]]:
        """``amps_source.status()`` by topic (empty until the source has subscribed)."""
        self.script(STATUS_SCRIPT)
        return {row["Topic"]: row for row in self.rows("_e2e_status")}


@pytest.fixture(scope="module")
def dh(stack):
    handle = Dh()
    yield handle
    handle.reset()


def _connect_publisher() -> Any:
    import AMPS

    client = AMPS.Client("otr-e2e-publisher")
    client.connect(AMPS_URI)
    client.logon()
    return client


class Book:
    """Every event the server's subscriptions should have seen, as this test caused them.

    Messages are kept ``|`` delimited (the parser takes both) and published with SOH, the
    way ``fixstate_mock.publish`` does. The oracle folds this log exactly as the server folds
    its event tables: the latest record per (topic, OrderID), deletes removed, ids from
    every message an order ever carried.
    """

    def __init__(self, client: Any) -> None:
        self.client = client
        self.log: Dict[str, List[Dict[str, Any]]] = {system.system: [] for system in FIX_CONFIG.systems}

    def _record(self, topic: str, raw: str, action: str) -> None:
        events = self.log[SYSTEM_OF_TOPIC[topic]]
        events.append(fixstate.event(topic, raw, len(events) + 1, action))

    def publish(self, *messages: Tuple[str, str]) -> None:
        for topic, raw in messages:
            self.client.publish(topic, raw.replace("|", fixstate.SOH))
            self._record(topic, raw, fixstate.UPSERT)
        self.client.publish_flush()

    def latest(self, topic: str, order_id: str) -> Optional[str]:
        """The current record of an order, or ``None`` once it is deleted."""
        for event in reversed(self.log[SYSTEM_OF_TOPIC[topic]]):
            if event["Topic"] == topic and fixstate.parse_fix(event["RawFix"]).get("37") == order_id:
                return event["RawFix"] if event["Action"] == fixstate.UPSERT else None
        return None

    def delete(self, topic: str, order_id: str) -> None:
        raw = self.latest(topic, order_id)
        assert raw is not None, (topic, order_id)
        self.client.sow_delete(topic, f"/37 = '{order_id}'")
        self._record(topic, raw, fixstate.DELETE)  # the OOF carries the record's data

    def clear(self) -> None:
        for topic in ALL_TOPICS:
            self.client.sow_delete(topic, "1=1")
        for events in self.log.values():
            events.clear()

    def current(self) -> int:
        """Records in the SOW right now."""
        return sum(len(fixstate.sow_current(events)) for events in self.log.values())

    def expected(self) -> Dict[str, Dict[str, Any]]:
        rows, ids = fixstate.fold_events(FIX_CONFIG, self.log)
        return reference.reconcile(TOPOLOGY, rows, TOL, extra_ids=ids)


@pytest.fixture(scope="module")
def book(stack):
    handle = Book(_connect_publisher())
    yield handle
    try:
        handle.client.close()
    except Exception:  # noqa: BLE001
        pass


@pytest.fixture(scope="module")
def mock_book():
    return mockdata.generate(SEED, FAMILIES)


# --------------------------------------------------------------------------------------
# FIX helpers
# --------------------------------------------------------------------------------------


def with_tags(raw: str, updates: Mapping[str, Any]) -> str:
    """``raw`` with some tags replaced (in place) or appended; ``|`` delimited."""
    fields = [field.split("=", 1) for field in raw.replace(fixstate.SOH, "|").split("|") if "=" in field]
    seen = set()
    out = []
    for tag, value in fields:
        if tag in updates:
            value = str(updates[tag])
            seen.add(tag)
        out.append(f"{tag}={value}")
    out += [f"{tag}={value}" for tag, value in updates.items() if tag not in seen]
    return "|".join(out) + "|"


def fill(book: Book, node: Mapping[str, Any], qty: int) -> None:
    """Publish the order's next state: ``qty`` more filled, ``qty`` less open."""
    topic = TOPIC_OF_LEVEL[node["Level"]]
    raw = book.latest(topic, node["OrderId"])
    assert raw is not None, node["NodeKey"]
    tags = fixstate.parse_fix(raw)
    book.publish((topic, with_tags(raw, {"14": int(float(tags["14"])) + qty, "151": int(float(tags["151"])) - qty})))


# --------------------------------------------------------------------------------------
# Convergence
# --------------------------------------------------------------------------------------


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is None and b is None
        return abs(float(a) - float(b)) < 1e-9
    if a in (None, "") and b in (None, ""):
        return True
    return a == b


def _recon_diff(engine: Dict[str, Dict[str, Any]], expected: Dict[str, Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    missing = sorted(set(expected) - set(engine))
    extra = sorted(set(engine) - set(expected))
    if missing:
        out.append(f"missing {len(missing)}: {missing[:5]}")
    if extra:
        out.append(f"extra {len(extra)}: {extra[:5]}")
    for key in sorted(set(engine) & set(expected)):
        for column, value in engine[key].items():
            if column not in expected[key]:
                out.append(f"{key}.{column}: not in the reference")
            elif not _same(value, expected[key][column]):
                out.append(f"{key}.{column}: engine {value!r} != reference {expected[key][column]!r}")
    return out


def settle(dh: Dh, book: Book, step: str) -> Dict[str, Dict[str, Any]]:
    """Wait until ``otr_recon`` equals the reference of ``book``; returns the reference."""
    expected = book.expected()
    deadline = time.time() + SETTLE_TIMEOUT
    diff: List[str] = ["never read"]
    while time.time() < deadline:
        try:
            engine = {row["NodeKey"]: row for row in dh.rows("otr_recon")}
            diff = _recon_diff(engine, expected)
        except Exception as exc:  # noqa: BLE001 - a restarting server; keep polling
            diff = [f"read failed: {type(exc).__name__}: {exc}"]
            dh.reset()
        if not diff:
            return expected
        time.sleep(1.0)
    pytest.fail(f"{step}: otr_recon did not converge in {SETTLE_TIMEOUT:.0f}s "
                f"({len(diff)} differences):\n  " + "\n  ".join(diff[:20]) + f"\nstatus: {_safe_status(dh)}")


def _safe_status(dh: Dh) -> Any:
    try:
        return dh.status()
    except Exception as exc:  # noqa: BLE001
        return f"unavailable ({exc})"


def wait_for(predicate, what: str, timeout: float = SETTLE_TIMEOUT) -> None:
    deadline = time.time() + timeout
    last: Any = None
    while time.time() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(1.0)
    pytest.fail(f"timed out after {timeout:.0f}s waiting for {what} (last error: {last})")


def subscribed(dh: Dh, epoch: int = 1) -> bool:
    """Every topic has completed a snapshot of at least ``epoch`` and the client is connected."""
    rows = dh.status()
    return (set(rows) == set(ALL_TOPICS)
            and all(r["Snapshots"] >= epoch and r["Epoch"] >= epoch and r["Connected"] for r in rows.values()))


def scenario_nodes(expected: Mapping[str, Mapping[str, Any]], mock_book: Any, scenario: str) -> List[Mapping[str, Any]]:
    system, order_id = mock_book.scenario_roots[scenario]
    return [n for n in expected.values() if n["RootKey"] == f"{system}|{order_id}"]


def _cli(*args: str) -> str:
    done = subprocess.run([CONTAINER_CLI, *args], capture_output=True, text=True, timeout=180)
    if done.returncode != 0:
        pytest.fail(f"{CONTAINER_CLI} {' '.join(args)} failed: {done.stderr.strip()}")
    return done.stdout


def _app_ready_count() -> int:
    done = subprocess.run([CONTAINER_CLI, "logs", DH_CONTAINER], capture_output=True, text=True, timeout=60)
    return (done.stdout + done.stderr).count("[order-tree] exported:")


# --------------------------------------------------------------------------------------
# 1. The stack
# --------------------------------------------------------------------------------------


def test_the_app_is_subscribed_to_every_topic(dh):
    wait_for(lambda: subscribed(dh), "a completed snapshot on every topic", RESTART_TIMEOUT)
    assert set(dh.status()) == set(ALL_TOPICS) == set(TOPIC_OF_LEVEL.values())


def test_an_empty_sow_is_an_empty_book(dh, book):
    book.clear()  # idempotent against a stack someone left running
    settle(dh, book, "empty SOW")
    assert dh.rows("otr_exposure") == []


# --------------------------------------------------------------------------------------
# 2. The mock book
# --------------------------------------------------------------------------------------


def test_the_mock_book_matches_the_reference_row_for_row(dh, book, mock_book):
    book.publish(*fixstate_mock.send_order(mock_book))
    expected = settle(dh, book, "mock book")
    assert len(expected) == len(mock_book.raptor) + len(mock_book.algo) + len(mock_book.router)
    # the FIX path and the table path agree on the same book (the amended DMA client
    # order links through its OrigClOrdID)
    tables = reference.reconcile(TOPOLOGY, mockdata.canonical_rows(mock_book), TOL)
    assert {k: n["BreakKind"] for k, n in expected.items()} == {k: n["BreakKind"] for k, n in tables.items()}


def test_ladder_and_exposure_match_the_reference(dh, book):
    expected = book.expected()

    def agree() -> bool:
        ladder = {tuple(r[c] for c in reference.LADDER_BY): r for r in dh.rows("otr_level_ladder")}
        exposure = {tuple(r[c] for c in reference.SUMMARY_BY): r for r in dh.rows("otr_exposure")}
        want_ladder = reference.level_ladder(expected)
        want_exposure = reference.exposure_summary(expected)
        if len(ladder) != len(want_ladder) or len(exposure) != len(want_exposure):
            return False
        for want, got, by, columns in (
            (want_ladder, ladder, reference.LADDER_BY,
             ("Orders",) + reference.LADDER_SUMS + ("CumVsClient", "LeavesVsClient")),
            (want_exposure, exposure, reference.SUMMARY_BY,
             ("Orders",) + reference.SUMMARY_SUMS + ("NetUnbookedQty", "StreetOpenVsClient")),
        ):
            for row in want:
                have = got.get(tuple(row[c] for c in by))
                if have is None or not all(_same(float(have[c]), float(row[c])) for c in columns):
                    return False
        return True

    wait_for(agree, "otr_level_ladder and otr_exposure to equal the reference")


def test_the_query_api_works_over_the_amps_book(dh, book, mock_book):
    dh.script(
        '_e2e_r = order_tree("DUNE", "MSFT", "BUY", include_unlinked=False)\n'
        '_e2e_breaks = _e2e_r["breaks"]\n'
        '_e2e_exposure = _e2e_r["exposure"]\n'
    )
    assert [r["BreakKind"] for r in dh.rows("_e2e_breaks")] == ["CHILD_STILL_OPEN"]
    assert [r["StreetOpenVsClient"] for r in dh.rows("_e2e_exposure")] == [100.0]

    tree = scenario_nodes(book.expected(), mock_book, "cancel_not_propagated")
    venue = next(n for n in tree if n["IsStreet"] and n["LeavesQty"] > 0)
    dh.script(f'_e2e_found = find_tree("{venue["OrderId"]}")')
    found = dh.rows("_e2e_found")
    assert {r["RootKey"] for r in found} == {venue["RootKey"]}
    assert len(found) == len(tree)


# --------------------------------------------------------------------------------------
# 3. A fill climbs the chain
# --------------------------------------------------------------------------------------


def test_a_fill_climbs_the_chain_one_hop_at_a_time(dh, book, mock_book):
    expected = book.expected()
    venue = next(n for n in scenario_nodes(expected, mock_book, "working") if n["IsStreet"] and n["LeavesQty"] >= 100)
    chain = [venue]
    while chain[-1]["ParentKey"]:
        chain.append(expected[chain[-1]["ParentKey"]])
    assert [n["Level"] for n in chain] == ["OR_CHILD", "OR_PARENT", "ALGO_CHILD", "ALGO_PARENT", "CLIENT"]
    before = {n["NodeKey"]: n["BreakKind"] for n in chain}

    for hop, order in enumerate(chain):
        fill(book, order, 100)
        now = settle(dh, book, f"fill published by {order['Level']}")
        bucket = next(b for b in reference.exposure_summary(now)
                      if (b["RootMoniker"], b["RootSymbol"], b["RootSide"])
                      == (order["RootMoniker"], order["RootSymbol"], order["RootSide"]))
        if hop + 1 < len(chain):
            # the next system up hasn't published it yet: its edge is the one that is short
            parent = now[chain[hop + 1]["NodeKey"]]
            assert parent["BreakKind"] == "UNBOOKED_FILL" and parent["FillGap"] == -100.0, parent
            assert bucket["NetUnbookedQty"] == 100.0
        else:
            assert bucket["NetUnbookedQty"] == 0.0
            assert {n["NodeKey"]: now[n["NodeKey"]]["BreakKind"] for n in chain} == before


# --------------------------------------------------------------------------------------
# 4. Child-first arrival
# --------------------------------------------------------------------------------------

_T = "60=20261011-14:00:00.000"
E2E_RAPTOR = [("raptor.orders",
               f"35=8|37=E2E-RPT-1|11=E2E-CL-1|1=EAGLE|55=AMZN|54=2|38=500|14=200|151=300|6=186.5|39=1|{_T}|")]
E2E_ALGO = [
    ("algo.parent.orders",
     f"35=8|37=E2E-ALG-1|11=E2E-RPT-1|55=AMZN|54=2|38=500|14=200|151=300|6=186.5|39=1|{_T}|847=VWAP|"),
    ("algo.child.orders",
     f"35=8|37=E2E-ALG-1-01|11=E2E-ALG-1-01|20001=E2E-ALG-1|55=AMZN|54=2|38=500|14=200|151=300|6=186.5|39=1|{_T}|847=VWAP|"),
]
E2E_ROUTER = [
    ("router.parent.orders",
     f"35=8|37=E2E-OR-1|11=E2E-ALG-1-01|55=AMZN|54=2|38=500|14=200|151=300|6=186.5|39=1|{_T}|100=SOR|"),
    ("router.child.orders",
     f"35=8|37=E2E-OR-1.1|11=VE2E-1.1|20001=E2E-OR-1|55=AMZN|54=2|38=300|14=200|151=100|6=186.5|39=1|{_T}|100=NYSE|"),
    ("router.child.orders",
     f"35=8|37=E2E-OR-1.2|11=VE2E-1.2|20001=E2E-OR-1|55=AMZN|54=2|38=200|14=0|151=200|6=0|39=0|{_T}|100=IEX|"),
]


def test_orders_arriving_child_first_link_up_when_their_parents_land(dh, book):
    book.publish(*E2E_ROUTER)
    now = settle(dh, book, "router orders before their parents")
    router_parent = now["OR|E2E-OR-1"]
    assert (router_parent["LinkState"], router_parent["BreakKind"], router_parent["Depth"]) == ("ORPHAN", "ORPHAN", 0)
    unattributed = [b for b in reference.exposure_summary(now) if (b["RootMoniker"], b["RootSymbol"]) == ("", "AMZN")]
    assert unattributed and unattributed[0]["StreetLeavesQty"] >= 300.0

    book.publish(*E2E_ALGO)
    now = settle(dh, book, "algo orders before Raptor's")
    assert now["OR|E2E-OR-1"]["LinkState"] == "LINKED"
    algo_parent = now["ALGO|E2E-ALG-1"]
    assert (algo_parent["Level"], algo_parent["LinkState"], algo_parent["Depth"]) == ("ALGO_PARENT", "ORPHAN", 0)

    book.publish(*E2E_RAPTOR)
    now = settle(dh, book, "Raptor order lands")
    family = [n for n in now.values() if n["RootKey"] == "RAPTOR|E2E-RPT-1"]
    assert len(family) == 6
    assert {n["RootMoniker"] for n in family} == {"EAGLE"}
    assert {n["BreakKind"] for n in family} == {"NONE"}
    assert {n["LinkState"] for n in family} == {"ROOT", "LINKED"}


# --------------------------------------------------------------------------------------
# 5. sow_delete
# --------------------------------------------------------------------------------------


def test_sow_delete_removes_an_order_and_reopens_its_edge(dh, book, mock_book):
    venue = next(n for n in scenario_nodes(book.expected(), mock_book, "clean_filled") if n["IsStreet"] and n["CumQty"] > 0)
    topic = TOPIC_OF_LEVEL["OR_CHILD"]
    saved = book.latest(topic, venue["OrderId"])

    book.delete(topic, venue["OrderId"])
    now = settle(dh, book, "venue order deleted")
    assert venue["NodeKey"] not in now
    assert now[venue["ParentKey"]]["BreakKind"] == "PHANTOM_FILL"

    book.publish((topic, saved))
    now = settle(dh, book, "venue order republished")
    assert now[venue["ParentKey"]]["BreakKind"] == "NONE"


# --------------------------------------------------------------------------------------
# 6. A garbled quantity
# --------------------------------------------------------------------------------------


def test_a_garbled_quantity_folds_to_null_on_both_sides(dh, book):
    topic = "router.child.orders"
    raw = book.latest(topic, "E2E-OR-1.2")
    # LeavesQty was 200: garbled, it folds to null -> 0 on both sides, so the router parent
    # now holds 200 its children don't show
    book.publish((topic, with_tags(raw, {"151": "lots"})))
    now = settle(dh, book, "a garbled LeavesQty")
    assert now["OR|E2E-OR-1.2"]["LeavesQty"] == 0.0
    assert now["OR|E2E-OR-1"]["BreakKind"] == "HELD"

    book.publish((topic, with_tags(raw, {"14": "100", "151": "100", "6": "186.5", "39": "1"})))
    now = settle(dh, book, "the next good message")
    assert now["OR|E2E-OR-1"]["BreakKind"] == "UNBOOKED_FILL"  # the router parent hasn't booked it


# --------------------------------------------------------------------------------------
# 7-9. Restarts and a dropped connection
# --------------------------------------------------------------------------------------


def test_a_restarted_deephaven_rebuilds_the_book_from_the_sow(dh, book):
    ready = _app_ready_count()
    dh.reset()
    _cli("restart", DH_CONTAINER)
    wait_for(lambda: _app_ready_count() > ready, "the restarted app to export its tables", RESTART_TIMEOUT)
    wait_for(lambda: subscribed(dh), "the snapshots", RESTART_TIMEOUT)
    settle(dh, book, "after a Deephaven restart")
    rows = dh.status()
    assert all(r["Epoch"] == 1 for r in rows.values()), rows
    # a fresh process saw one snapshot and nothing else
    assert sum(r["Upserts"] for r in rows.values()) == book.current()


def test_a_dropped_connection_resyncs_the_deletes_it_missed(dh, book, mock_book):
    venues = [n for n in scenario_nodes(book.expected(), mock_book, "over_routed") if n["IsStreet"]]
    gone, filled = venues[0], venues[-1]
    epochs = {topic: r["Epoch"] for topic, r in dh.status().items()}

    dh.reset()
    _cli("pause", DH_CONTAINER)
    try:
        # Past the heartbeat timeout AMPS drops the frozen client, so the OOF below is never
        # sent to it: only the re-snapshot after reconnecting can remove the record.
        time.sleep(DEFAULT_HEARTBEAT * 3 + 5)
        book.delete(TOPIC_OF_LEVEL["OR_CHILD"], gone["OrderId"])
        fill(book, filled, 100)
    finally:
        _cli("unpause", DH_CONTAINER)

    wait_for(lambda: all(r["Epoch"] > epochs[t] for t, r in dh.status().items()), "a re-snapshot on every topic",
             RESTART_TIMEOUT)
    now = settle(dh, book, "after the dropped connection")
    assert gone["NodeKey"] not in now


def test_a_restarted_amps_is_resubscribed(dh, book, mock_book):
    epochs = {topic: r["Epoch"] for topic, r in dh.status().items()}
    _cli("restart", AMPS_CONTAINER)
    wait_for(lambda: _port_open(*_amps_host_port()), "AMPS to listen again", RESTART_TIMEOUT)
    wait_for(lambda: all(r["Epoch"] > epochs[t] and r["Connected"] for t, r in dh.status().items()),
             "a re-snapshot on every topic", RESTART_TIMEOUT)
    settle(dh, book, "after the AMPS restart (the SOW survived on its volume)")

    try:
        book.client.close()  # its connection died with the broker
    except Exception:  # noqa: BLE001
        pass
    book.client = _connect_publisher()
    venue = next(n for n in scenario_nodes(book.expected(), mock_book, "working") if n["IsStreet"] and n["LeavesQty"] >= 100)
    fill(book, venue, 100)
    now = settle(dh, book, "a fill after the AMPS restart")
    assert now[venue["ParentKey"]]["BreakKind"] == "UNBOOKED_FILL"


def test_every_snapshot_completed_and_the_client_is_connected(dh):
    for topic, row in dh.status().items():
        assert row["Connected"] and row["Snapshots"] == row["Epoch"], (topic, row)
