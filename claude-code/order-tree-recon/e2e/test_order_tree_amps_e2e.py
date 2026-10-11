"""Order tree over AMPS, end to end: publisher -> AMPS SOW topics -> Deephaven -> pydeephaven.

Run it through ``run_e2e.sh``: it brings up ``docker/docker-compose.order-tree-amps.yml``
(AMPS ``otr-amps`` + Deephaven ``otr-amps-deephaven`` with ``OTR_SOURCE=amps``) from
``down -v`` and then runs this file. Running ``pytest`` directly works against a stack that
is already up; without one every test skips, saying what is missing.

The test plays the three upstream systems from the host: it publishes native order rows as
JSON to ``otr.raptor_orders`` / ``otr.algo_orders`` / ``otr.router_orders`` and
``sow_delete``s them, keeping its own copy of what the SOW holds (:class:`Book`). The
oracle is the module's pure-python reference (``reference.reconcile`` over
``mockdata.canonical_rows`` of that copy), so after every step the engine's ``otr_recon``
must equal the reference **row for row, column for column** -- the same bar as the
embedded test, but through a real broker, the app's AMPS source and Application Mode.

Every step changes the book, so "the engine matches the new expected state" can't pass
on the state before it. In order:

1. the stack is up and subscribed to the three topics; an empty SOW is an empty book;
2. the whole mock book (seed 42, 24 families: 340 orders) matches the reference, and so
   do the ladder and the exposure table; the query API works over it;
3. a venue fill climbs the chain one hop at a time: each hop's edge is ``UNBOOKED_FILL``
   until the next system up books it, and ``NetUnbookedQty`` is 100 until the client does;
4. a family published child-first (router, then algo, then Raptor) is ``ORPHAN`` and
   unattributed until its parents land, then links under its moniker -- the topics are
   independent and nothing orders them; its parents carry a *null* parent id;
5. ``sow_delete`` of a venue order removes it, and its router parent shows the fill its
   children no longer explain (``PHANTOM_FILL``) until it is republished;
6. a malformed message is counted in ``otr_amps_status`` and the feed lives on;
7. a restarted Deephaven rebuilds the identical book from the SOW snapshot;
8. a connection dropped by heartbeat (Deephaven paused) misses an OOF; on reconnect the
   re-snapshot **resyncs** -- the deleted order goes, the missed fill lands;
9. a restarted AMPS (SOW on its volume) is re-subscribed and the feed resumes.

Environment (``run_e2e.sh`` sets them):

==========================  ======================================  ========================
``OTRE2E_DH_HOST/PORT``     Deephaven gRPC                          ``localhost`` / 10020
``OTRE2E_AMPS_URI``         where the test publishes                ``tcp://localhost:39007/amps/json``
``OTRE2E_PREFIX``           topic prefix                            ``otr.``
``OTRE2E_SEED/FAMILIES``    the mock book                           42 / 24
``OTRE2E_DH_CONTAINER``     restarted / paused by tests 7-8         ``otr-amps-deephaven``
``OTRE2E_AMPS_CONTAINER``   restarted by test 9                     ``otr-amps``
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
from typing import Any, Dict, List, Mapping

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
MODULE_SRC = os.path.join(os.path.dirname(HERE), "src")
if MODULE_SRC not in sys.path:
    sys.path.insert(0, MODULE_SRC)

# The oracle is the shipped pure-python reference, not a copy of the formulas.
from order_tree_recon import amps_codec, amps_publish, config, mockdata, reference  # noqa: E402
from order_tree_recon.amps_source import HEARTBEAT_SECONDS  # noqa: E402
from order_tree_recon.native import NATIVE_SCHEMAS, ROUTER  # noqa: E402

DH_HOST = os.environ.get("OTRE2E_DH_HOST", "localhost")
DH_PORT = int(os.environ.get("OTRE2E_DH_PORT", "10020"))
AMPS_URI = os.environ.get("OTRE2E_AMPS_URI", "tcp://localhost:39007/amps/json")
PREFIX = os.environ.get("OTRE2E_PREFIX", config.DEFAULT_AMPS_TOPIC_PREFIX)
SEED = int(os.environ.get("OTRE2E_SEED", "42"))
FAMILIES = int(os.environ.get("OTRE2E_FAMILIES", "24"))
DH_CONTAINER = os.environ.get("OTRE2E_DH_CONTAINER", "otr-amps-deephaven")
AMPS_CONTAINER = os.environ.get("OTRE2E_AMPS_CONTAINER", "otr-amps")
CONTAINER_CLI = os.environ.get("CONTAINER_CLI", "podman")
SETTLE_TIMEOUT = float(os.environ.get("OTRE2E_SETTLE_TIMEOUT", "60"))
RESTART_TIMEOUT = float(os.environ.get("OTRE2E_RESTART_TIMEOUT", "300"))

TOL = config.DEFAULT_QTY_TOL
TOPOLOGY = config.default_topology()

#: The native fields a fill moves, per level: (table, cum column, leaves column).
FILL_FIELDS = {
    "CLIENT": ("raptor_orders", "CumQty", "LeavesQty"),
    "ALGO_PARENT": ("algo_orders", "Filled", "Open"),
    "ALGO_CHILD": ("algo_orders", "Filled", "Open"),
    "OR_PARENT": ("router_orders", "CumQty", "LeavesQty"),
    "OR_CHILD": ("router_orders", "CumQty", "LeavesQty"),
}


# --------------------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------------------


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def _amps_host_port() -> tuple:
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


@pytest.fixture(scope="module")
def dh(stack):
    handle = Dh()
    yield handle
    handle.reset()


class Book:
    """What the SOW topics hold, as this test published it: the oracle's input."""

    def __init__(self, client: Any) -> None:
        self.client = client
        self.rows: Dict[str, Dict[str, Dict[str, Any]]] = {name: {} for name in NATIVE_SCHEMAS}

    def publish(self, table: str, *rows: Mapping[str, Any]) -> None:
        schema = NATIVE_SCHEMAS[table]
        amps_publish.publish_rows(self.client, schema, rows, PREFIX)
        for row in rows:
            self.rows[table][row[schema.key]] = dict(row)

    def delete(self, table: str, *order_ids: str) -> int:
        deleted = amps_publish.delete_orders(self.client, NATIVE_SCHEMAS[table], order_ids, PREFIX)
        for order_id in order_ids:
            self.rows[table].pop(order_id, None)
        return deleted

    def clear(self) -> None:
        amps_publish.clear(self.client, PREFIX)
        for rows in self.rows.values():
            rows.clear()

    def row(self, table: str, order_id: str) -> Dict[str, Any]:
        return dict(self.rows[table][order_id])

    def expected(self) -> Dict[str, Dict[str, Any]]:
        data = mockdata.MockData(
            raptor=list(self.rows["raptor_orders"].values()),
            algo=list(self.rows["algo_orders"].values()),
            router=list(self.rows["router_orders"].values()),
            scenario_roots={},
            roots=[],
        )
        return reference.reconcile(TOPOLOGY, mockdata.canonical_rows(data), TOL)

    def size(self) -> int:
        return sum(len(rows) for rows in self.rows.values())


@pytest.fixture(scope="module")
def book(stack):
    client = amps_publish.connect(AMPS_URI, "otr-e2e-publisher")
    yield Book(client)
    try:
        client.close()
    except Exception:  # noqa: BLE001
        pass


@pytest.fixture(scope="module")
def mock_book():
    return mockdata.generate(SEED, FAMILIES)


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
    status = _safe_status(dh)
    pytest.fail(f"{step}: otr_recon did not converge in {SETTLE_TIMEOUT:.0f}s "
                f"({len(diff)} differences):\n  " + "\n  ".join(diff[:20]) + f"\nstatus: {status}")


def status(dh: Dh) -> Dict[str, Dict[str, Any]]:
    return {row["Table"]: row for row in dh.rows("otr_amps_status")}


def _safe_status(dh: Dh) -> Any:
    try:
        return status(dh)
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


def node(expected: Mapping[str, Mapping[str, Any]], system: str, order_id: str) -> Mapping[str, Any]:
    return expected[f"{system}|{order_id}"]


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


def test_the_app_is_subscribed_to_the_three_topics(dh):
    wait_for(lambda: len(status(dh)) == 3, "otr_amps_status")
    rows = status(dh)
    assert {name: row["Topic"] for name, row in rows.items()} == amps_codec.topics(PREFIX)
    for row in rows.values():
        assert row["Connected"] is True and row["Snapshots"] >= 1, row


def test_an_empty_sow_is_an_empty_book(dh, book):
    book.clear()  # idempotent against a stack someone left running
    settle(dh, book, "empty SOW")
    assert dh.rows("otr_exposure") == []


# --------------------------------------------------------------------------------------
# 2. The mock book
# --------------------------------------------------------------------------------------


def test_the_mock_book_matches_the_reference_row_for_row(dh, book, mock_book):
    book.publish("raptor_orders", *mock_book.raptor)
    book.publish("algo_orders", *mock_book.algo)
    book.publish("router_orders", *mock_book.router)
    expected = settle(dh, book, "mock book")
    assert len(expected) == len(mock_book.raptor) + len(mock_book.algo) + len(mock_book.router)


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

    expected = book.expected()
    system, order_id = mock_book.scenario_roots["cancel_not_propagated"]
    root_key = f"{system}|{order_id}"
    venue = next(n for n in expected.values() if n["RootKey"] == root_key and n["IsStreet"] and n["LeavesQty"] > 0)
    dh.script(f'_e2e_found = find_tree("{venue["OrderId"]}")')
    found = dh.rows("_e2e_found")
    assert {r["RootKey"] for r in found} == {root_key}
    assert len(found) == sum(1 for n in expected.values() if n["RootKey"] == root_key)


# --------------------------------------------------------------------------------------
# 3. A fill climbs the chain
# --------------------------------------------------------------------------------------


def _fill(book: Book, level: str, order_id: str, qty: int) -> None:
    table, cum, leaves = FILL_FIELDS[level]
    row = book.row(table, order_id)
    row[cum] += qty
    row[leaves] -= qty
    book.publish(table, row)


def test_a_fill_climbs_the_chain_one_hop_at_a_time(dh, book, mock_book):
    expected = book.expected()
    system, order_id = mock_book.scenario_roots["working"]
    root_key = f"{system}|{order_id}"
    venue = next(n for n in expected.values()
                 if n["RootKey"] == root_key and n["IsStreet"] and n["LeavesQty"] >= 100)
    chain = [venue]
    while chain[-1]["ParentKey"]:
        chain.append(expected[chain[-1]["ParentKey"]])
    assert [n["Level"] for n in chain] == ["OR_CHILD", "OR_PARENT", "ALGO_CHILD", "ALGO_PARENT", "CLIENT"]
    before = {n["NodeKey"]: n["BreakKind"] for n in chain}

    for hop, order in enumerate(chain):
        _fill(book, order["Level"], order["OrderId"], 100)
        now = settle(dh, book, f"fill booked by {order['Level']}")
        bucket = next(b for b in reference.exposure_summary(now)
                      if (b["RootMoniker"], b["RootSymbol"], b["RootSide"])
                      == (order["RootMoniker"], order["RootSymbol"], order["RootSide"]))
        if hop + 1 < len(chain):
            # the next system up hasn't booked it yet: its edge is the one that is short
            parent = now[chain[hop + 1]["NodeKey"]]
            assert parent["BreakKind"] == "UNBOOKED_FILL" and parent["FillGap"] == -100.0, parent
            assert bucket["NetUnbookedQty"] == 100.0
        else:
            assert bucket["NetUnbookedQty"] == 0.0
            assert {n["NodeKey"]: now[n["NodeKey"]]["BreakKind"] for n in chain} == before


# --------------------------------------------------------------------------------------
# 4. Child-first arrival
# --------------------------------------------------------------------------------------

E2E_RAPTOR = {"OrderId": "E2E-RPT-1", "ClOrdID": "E2E-CL-1", "Moniker": "EAGLE", "Symbol": "AMZN", "Side": "2",
              "OrderQty": 500, "CumQty": 200, "LeavesQty": 300, "AvgPx": 186.5, "OrdStatus": "PARTIALLY_FILLED"}
E2E_ALGO = [
    # A null parent id (a JSON feed that omits the field) must still be read as a parent.
    {"AlgoOrderId": "E2E-ALG-1", "ParentAlgoOrderId": None, "RaptorOrderId": "E2E-RPT-1", "Strategy": "VWAP",
     "Symbol": "AMZN", "Side": "SELL", "Qty": 500, "Filled": 200, "Open": 300, "AvgPrice": 186.5,
     "State": "PARTIALLY_FILLED"},
    {"AlgoOrderId": "E2E-ALG-1-01", "ParentAlgoOrderId": "E2E-ALG-1", "RaptorOrderId": None, "Strategy": "VWAP",
     "Symbol": "AMZN", "Side": "SELL", "Qty": 500, "Filled": 200, "Open": 300, "AvgPrice": 186.5,
     "State": "PARTIALLY_FILLED"},
]
E2E_ROUTER = [
    {"RouterOrderId": "E2E-OR-1", "ParentRouterOrderId": None, "UpstreamOrderId": "E2E-ALG-1-01", "Venue": "SOR",
     "Symbol": "AMZN", "Side": "SELL", "Qty": 500, "CumQty": 200, "LeavesQty": 300, "AvgPx": 186.5,
     "Status": "PARTIALLY_FILLED"},
    {"RouterOrderId": "E2E-OR-1.1", "ParentRouterOrderId": "E2E-OR-1", "UpstreamOrderId": None, "Venue": "NYSE",
     "Symbol": "AMZN", "Side": "SELL", "Qty": 300, "CumQty": 200, "LeavesQty": 100, "AvgPx": 186.5,
     "Status": "PARTIALLY_FILLED"},
    {"RouterOrderId": "E2E-OR-1.2", "ParentRouterOrderId": "E2E-OR-1", "UpstreamOrderId": None, "Venue": "IEX",
     "Symbol": "AMZN", "Side": "SELL", "Qty": 200, "CumQty": 0, "LeavesQty": 200, "AvgPx": None,
     "Status": "NEW"},
]


def test_orders_arriving_child_first_link_up_when_their_parents_land(dh, book):
    book.publish("router_orders", *E2E_ROUTER)
    now = settle(dh, book, "router orders before their parents")
    router_parent = node(now, "OR", "E2E-OR-1")
    assert (router_parent["LinkState"], router_parent["BreakKind"], router_parent["Depth"]) == ("ORPHAN", "ORPHAN", 0)
    unattributed = [b for b in reference.exposure_summary(now) if (b["RootMoniker"], b["RootSymbol"]) == ("", "AMZN")]
    assert unattributed and unattributed[0]["StreetLeavesQty"] >= 300.0

    book.publish("algo_orders", *E2E_ALGO)
    now = settle(dh, book, "algo orders before Raptor's")
    assert node(now, "OR", "E2E-OR-1")["LinkState"] == "LINKED"
    algo_parent = node(now, "ALGO", "E2E-ALG-1")
    assert (algo_parent["Level"], algo_parent["LinkState"], algo_parent["Depth"]) == ("ALGO_PARENT", "ORPHAN", 0)

    book.publish("raptor_orders", E2E_RAPTOR)
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
    expected = book.expected()
    system, order_id = mock_book.scenario_roots["clean_filled"]
    venue = next(n for n in expected.values()
                 if n["RootKey"] == f"{system}|{order_id}" and n["IsStreet"] and n["CumQty"] > 0)
    saved = book.row("router_orders", venue["OrderId"])

    assert book.delete("router_orders", venue["OrderId"]) == 1
    now = settle(dh, book, "venue order deleted")
    assert venue["NodeKey"] not in now
    assert now[venue["ParentKey"]]["BreakKind"] == "PHANTOM_FILL"

    book.publish("router_orders", saved)
    now = settle(dh, book, "venue order republished")
    assert now[venue["ParentKey"]]["BreakKind"] == "NONE"


# --------------------------------------------------------------------------------------
# 6. A malformed message
# --------------------------------------------------------------------------------------


def test_a_malformed_message_is_counted_and_the_feed_lives_on(dh, book):
    rejected = status(dh)["router_orders"]["Rejected"]
    deletes = status(dh)["router_orders"]["Deletes"]
    book.client.publish(amps_codec.topic(ROUTER, PREFIX), '{"RouterOrderId":"E2E-BAD-1","Qty":"lots"}')
    book.client.publish_flush()
    wait_for(lambda: status(dh)["router_orders"]["Rejected"] == rejected + 1, "the reject to be counted")
    assert "Qty" in status(dh)["router_orders"]["LastError"]

    _fill(book, "OR_CHILD", "E2E-OR-1.2", 100)  # the next good message still flows
    settle(dh, book, "a good message after a bad one")

    # the bad record is in the SOW (AMPS only needed its key); its OOF resolves the key
    # alone and deletes an order the table never held -- harmless, and not a reject
    amps_publish.delete_orders(book.client, ROUTER, ["E2E-BAD-1"], PREFIX)
    wait_for(lambda: status(dh)["router_orders"]["Deletes"] == deletes + 1, "the OOF of the bad record")
    assert status(dh)["router_orders"]["Rejected"] == rejected + 1
    settle(dh, book, "after deleting the bad record")


# --------------------------------------------------------------------------------------
# 7-9. Restarts and a dropped connection
# --------------------------------------------------------------------------------------


def test_a_restarted_deephaven_rebuilds_the_book_from_the_sow(dh, book):
    ready = _app_ready_count()
    dh.reset()
    _cli("restart", DH_CONTAINER)
    wait_for(lambda: _app_ready_count() > ready, "the restarted app to export its tables", RESTART_TIMEOUT)
    settle(dh, book, "after a Deephaven restart")
    rows = status(dh)
    assert all(row["Snapshots"] == 1 for row in rows.values()), rows
    # a fresh process saw the snapshot and nothing else
    assert sum(row["Upserts"] for row in rows.values()) == book.size()


def test_a_dropped_connection_resyncs_the_deletes_it_missed(dh, book, mock_book):
    expected = book.expected()
    system, order_id = mock_book.scenario_roots["over_routed"]
    venues = [n for n in expected.values() if n["RootKey"] == f"{system}|{order_id}" and n["IsStreet"]]
    gone, filled = venues[0], venues[-1]
    before = status(dh)["router_orders"]

    dh.reset()
    _cli("pause", DH_CONTAINER)
    try:
        # Past the heartbeat timeout AMPS drops the frozen client, so the OOF below is
        # never sent to it -- only the re-snapshot after reconnecting can bring it back.
        time.sleep(HEARTBEAT_SECONDS * 3 + 5)
        book.delete("router_orders", gone["OrderId"])
        _fill(book, "OR_CHILD", filled["OrderId"], 100)
    finally:
        _cli("unpause", DH_CONTAINER)

    settle(dh, book, "after the dropped connection")
    after = status(dh)["router_orders"]
    assert after["Snapshots"] > before["Snapshots"], (before, after)
    assert after["Resynced"] >= before["Resynced"] + 1, (before, after)
    assert after["Connected"] is True


def test_a_restarted_amps_is_resubscribed(dh, book, mock_book):
    before = status(dh)["raptor_orders"]["Snapshots"]
    _cli("restart", AMPS_CONTAINER)
    wait_for(lambda: _port_open(*_amps_host_port()), "AMPS to listen again", RESTART_TIMEOUT)
    wait_for(lambda: status(dh)["raptor_orders"]["Snapshots"] > before, "the re-snapshot", RESTART_TIMEOUT)

    try:
        book.client.close()  # its connection died with the broker
    except Exception:  # noqa: BLE001
        pass
    book.client = amps_publish.connect(AMPS_URI, "otr-e2e-publisher")
    system, order_id = mock_book.scenario_roots["working"]
    expected = book.expected()
    venue = next(n for n in expected.values()
                 if n["RootKey"] == f"{system}|{order_id}" and n["IsStreet"] and n["LeavesQty"] >= 100)
    _fill(book, "OR_CHILD", venue["OrderId"], 100)
    now = settle(dh, book, "a fill after the AMPS restart")
    assert now[venue["ParentKey"]]["BreakKind"] == "UNBOOKED_FILL"


def test_no_batch_failed_and_nothing_is_pending(dh):
    # The status row is written at the start of an update cycle, before that cycle drains
    # the feeds, so Pending can read 1 for one cycle after the DAG already has the row.
    wait_for(lambda: all(row["Pending"] == 0 for row in status(dh).values()), "the feeds to drain")
    for row in status(dh).values():
        assert row["FailedBatches"] == 0 and row["Connected"] is True, row
        # counters restarted with Deephaven in test 7, after the bad record was deleted
        assert row["Rejected"] == 0, row
