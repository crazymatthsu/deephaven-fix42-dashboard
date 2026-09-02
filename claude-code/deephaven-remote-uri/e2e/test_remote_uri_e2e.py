"""Remote-URI e2e: AMPS -> N Deephaven leaves -> one collector, asserted through
pydeephaven against the generator's own expected export (doc 10 s12).

Run it through ``run_e2e.sh`` (builds the derived image, brings up
``docker/docker-compose.remote-uri.yml``, publishes the four correlated drop-copy
tapes to AMPS with ``--amps-uri`` and then invokes pytest). Running ``pytest``
directly works too, provided the stack is already up and ``expected_remote_uri.json``
exists -- otherwise every test skips with a message saying what is missing, so this
file is safe in CI.

What is asserted (doc 10 s12, in this order):

1. every doc 10 s5.3 / s6 global exists on its own server -- plain tables through
   ``open_table``, the query API / dashboard / runtime objects through a
   ``run_script`` probe. ``oms_fix_messages`` must be **absent** on a leaf (doc 10
   s2.4: AMPS is the audit trail);
2. ``rx_orders`` on each leaf holds exactly the hubs that leaf folds (read from its
   own ``leaf_config``, not hard-coded), and the union over the fleet is the
   expected hub-order set;
3. on the collector, per hub-order ``OrdStatus`` / ``CumQty`` / ``LeavesQty`` /
   ``AvgPx`` / ``ExtOrdID`` / ``LinkState`` / ``RootKey`` / ``BreakKind`` equal the
   oracle -- cross-server linking is byte-for-byte doc 09's;
4. ``find_exposure(root_oms, account, symbol)`` returns every hop of every matching
   family in ``RootKey, Depth, Oms, OrderKey`` order, and ``exposure_for(...)``
   equals ``remote_uri.exposure.sum_exposure`` -- the pure-python reference in the
   module under test -- fed with the *oracle's* quantities and the ``MarkPx`` read
   from ``market_data_latest`` in the same server-side snapshot;
5. ``remote_executions(global_key)`` (a remote console query on the owning leaf)
   returns the same ``ExecID``s as that leaf's own ``oms_executions``, for an OMS-A
   order (leaf 1) and an OMS-C one (leaf 2); ``remote_live_executions`` returns the
   same rows as a *refreshing* table;
6. ``fleet`` has one row per leaf whose ``Orders`` equals that leaf's ``rx_orders``
   size; the suite prints ``HeapUsedMb / Orders`` per leaf -- the first measured
   bytes-per-order figure behind doc 10 s2.4;
7. restarting a leaf and calling ``reconnect()`` on the collector reproduces (3)
   exactly: the leaf replayed its tapes from ``EPOCH`` and the collector rebuilt its
   whole DAG from fresh subscriptions.

The expected export is the *independent* oracle: the generator computes the same
per-edge taxonomy (doc 09 s5.4) from its own scripts, and the point of the suite is
that two independent implementations agree -- here across three JVMs.

Environment (all optional; ``run_e2e.sh`` sets them):

===========================  ==========================================================
``RXE2E_EXPECTED``           the generator's ``--emit-expected`` JSON
``RXE2E_COLLECTOR_HOST``     collector host (default ``localhost``)
``RXE2E_COLLECTOR_PORT``     collector gRPC port (default ``10010``)
``RXE2E_LEAF_PORTS``         ``DH1:10011,DH2:10012`` (or ``DH1:host:10011``)
``RXE2E_CONTAINERS``         ``rx-dh1,rx-dh2,rx-collector`` -- leaves in
                             ``RXE2E_LEAF_PORTS`` order, collector last
``RXE2E_POLL_TIMEOUT``       seconds to let a table settle (default 180)
``RXE2E_RESTART_TIMEOUT``    seconds to wait for a restarted leaf (default 300)
``CONTAINER_CLI``            ``podman`` (default) or ``docker``
===========================  ==========================================================
"""

from __future__ import annotations

import collections
import json
import math
import os
import re
import socket
import subprocess
import sys
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
MODULE_SRC = os.path.join(os.path.dirname(HERE), "src")
MOMS_SRC = os.path.abspath(
    os.path.join(HERE, "..", "..", "deephaven-app-multi-oms-blotter", "src")
)

# The suite's oracle for assertion 4 is the *shipped* pure-python reference, not a
# second copy of the formulas: an edit to remote_uri/exposure.py that is not mirrored
# in the query strings (or vice versa) has to fail here. Both modules are stdlib-only
# by design (doc 10 s12), so importing them client-side costs nothing -- multi_oms is
# on the path because remote_uri.uris borrows its `sanitize_hub`.
for _path in (MOMS_SRC, MODULE_SRC):
    if os.path.isdir(_path) and _path not in sys.path:
        sys.path.insert(0, _path)

try:
    from remote_uri.exposure import EXPOSURE_SUM_COLUMNS, order_exposure, sum_exposure
    from remote_uri.uris import LEAF_EXPORTS, leaf_global_name, raw_global_name
except Exception as _exc:  # noqa: BLE001 - reported as a skip, never a collection error
    _IMPORT_ERROR = _exc
    EXPOSURE_SUM_COLUMNS = ()
    LEAF_EXPORTS = ("rx_orders", "rx_id_index", "rx_exposure", "rx_leaf_stats")
    order_exposure = sum_exposure = leaf_global_name = raw_global_name = None
else:
    _IMPORT_ERROR = None


# --------------------------------------------------------------------------
# configuration
# --------------------------------------------------------------------------
COLLECTOR_HOST = os.environ.get("RXE2E_COLLECTOR_HOST", "localhost")
COLLECTOR_PORT = int(os.environ.get("RXE2E_COLLECTOR_PORT", "10010"))
LEAF_PORTS_ENV = os.environ.get("RXE2E_LEAF_PORTS", "DH1:10011,DH2:10012")
CONTAINERS_ENV = os.environ.get("RXE2E_CONTAINERS", "rx-dh1,rx-dh2,rx-collector")
CONTAINER_CLI = os.environ.get("CONTAINER_CLI", "podman")
EXPECTED_FILE = os.environ.get("RXE2E_EXPECTED", os.path.join(HERE, "expected_remote_uri.json"))

POLL_TIMEOUT = float(os.environ.get("RXE2E_POLL_TIMEOUT", "180"))
POLL_INTERVAL = float(os.environ.get("RXE2E_POLL_INTERVAL", "2"))
STABLE_POLLS = int(os.environ.get("RXE2E_STABLE_POLLS", "2"))
RESTART_TIMEOUT = float(os.environ.get("RXE2E_RESTART_TIMEOUT", "300"))

#: Absolute tolerance for quantities (doc 09's ``MULTIOMS_QTY_TOL`` default).
TOL = 1e-6
#: Relative tolerance for marked notionals. ``MarkPx`` is a live random walk, so the
#: comparison is against the *same server-side snapshot* the totals were computed
#: from (see :func:`_snapshot_lookup`); what is left is float-summation order.
REL_TOL = 1e-6

ROOT_HUB = os.environ.get("RXE2E_ROOT_OMS", "OMS-A")

#: Identifiers interpolated into a ``run_script`` body must look like the data they
#: came from. Belt and braces: the query API sanitises them again server-side.
_SAFE_ID = re.compile(r"^[A-Za-z0-9_.:|-]{1,64}$")

#: Leaf globals doc 10 s5.1-s5.3 promises, minus the per-hub raw blinks (derived
#: from each leaf's own ``leaf_config``, so the list holds for any hub assignment).
LEAF_TABLES = (
    # the five published streams (doc 09 s4.1, reused unchanged)
    "oms_order_state_blink",
    "oms_executions_blink",
    "oms_order_events_blink",
    "oms_fix_messages_blink",
    "oms_ingest_errors",
    # the per-leaf DAG (doc 10 s5.2)
    "oms_orders_latest",
    "oms_executions",
    "oms_events",
    "id_index",
    # the exports (doc 10 s5.3)
    "rx_orders",
    "rx_id_index",
    "rx_exposure",
    "rx_leaf_stats",
    "leaf_config",
)

#: Non-table leaf globals (``run_script`` probe).
LEAF_NON_TABLE_GLOBALS = ("remote_uri_pipeline", "remote_uri_runtime")

#: Deliberately **not** built on a leaf (doc 10 s2.4): at 400M messages
#: ``oms_fix_messages`` is by some distance the largest table and AMPS is the
#: audit trail. A leaf that grew one is a regression, not an improvement.
LEAF_FORBIDDEN_GLOBALS = ("oms_fix_messages",)

#: Collector table globals (doc 10 s6). The per-leaf ``rx_*_<leaf>`` copies are added
#: per configured leaf.
COLLECTOR_TABLES = (
    "orders_all",
    "id_index",
    "hub_config",
    "leaf_config",
    "orders_linked",
    "child_rollup",
    "orders_recon",
    "roots",
    "market_data_latest",
    "orders_marked",
    "exposure_by_level",
    "exposure_by_source",
    "exposure_by_leaf",
    "fleet",
    "source_oms_list",
    "account_list",
    "symbol_list",
)

#: The query API (doc 10 s9) plus the dashboard and the runtime handle.
QUERY_API_NAMES = (
    "find_exposure",
    "family_totals",
    "exposure_for",
    "leaf_of",
    "remote_executions",
    "remote_live_executions",
    "snapshot_leaf",
    "reconnect",
)
COLLECTOR_NON_TABLE_GLOBALS = ("remote_uri_dashboard", "remote_uri_runtime")


def _parse_leaf_ports(text: str):
    """``"DH1:10011,DH2:10012"`` -> ``[("DH1", "localhost", 10011), ...]``.

    A three-part entry (``DH1:host:10011``) pins the host too, for a fleet that is
    not published on loopback.
    """
    leaves = []
    for chunk in str(text or "").split(","):
        item = chunk.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) == 2:
            name, host, port = parts[0], "localhost", parts[1]
        elif len(parts) == 3:
            name, host, port = parts
        else:
            raise ValueError(
                f"RXE2E_LEAF_PORTS entry {item!r} must be 'NAME:PORT' or 'NAME:HOST:PORT'"
            )
        leaves.append((name.strip(), host.strip(), int(port)))
    return leaves


LEAVES = _parse_leaf_ports(LEAF_PORTS_ENV)


def _leaf_container(leaf_name: str):
    """The container name of one leaf, or ``None`` when ``RXE2E_CONTAINERS`` is short.

    The env var lists the leaves in ``RXE2E_LEAF_PORTS`` order and the collector
    last, which is what ``run_e2e.sh`` passes (``rx-dh1,rx-dh2,rx-collector``).
    """
    names = _container_names()
    order = [leaf[0] for leaf in LEAVES]
    if leaf_name not in order or len(names) < len(order):
        return None
    return names[order.index(leaf_name)]


def _container_names():
    """``RXE2E_CONTAINERS`` split into a list (leaves in order, collector last)."""
    return [c.strip() for c in str(CONTAINERS_ENV or "").split(",") if c.strip()]


def _collector_container():
    """The collector's container name, or ``None`` when it was not configured."""
    names = _container_names()
    return names[-1] if len(names) > len(LEAVES) else None


# --------------------------------------------------------------------------
# connection helpers
# --------------------------------------------------------------------------
def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _new_session(host: str, port: int):
    from pydeephaven import Session

    return Session(host=host, port=port)


def _wait_for_session(host: str, port: int, timeout: float):
    """Return a live Session, or None if the server never came back."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _port_open(host, port):
            try:
                return _new_session(host, port)
            except Exception:  # noqa: BLE001 - gRPC comes up a beat after the port
                pass
        time.sleep(POLL_INTERVAL)
    return None


def _container_log(container: str, tail: int = 400) -> str:
    """Best-effort tail of a container log (empty string when unavailable).

    Never piped into ``grep``: under ``set -o pipefail`` a matching grep kills the
    producer with SIGPIPE and the *pipeline* reports failure, which silently inverts
    the check. The shell script has the same rule; here the capture is free anyway.
    """
    if not container:
        return ""
    try:
        done = subprocess.run(
            [CONTAINER_CLI, "logs", "--tail", str(tail), container],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except Exception:  # noqa: BLE001 - the CLI may not be on PATH at all
        return ""
    return (done.stdout or "") + (done.stderr or "")


def read_table(session, name):
    """Snapshot a server-side global as a pandas DataFrame."""
    return session.open_table(name).to_arrow().to_pandas()


def poll_table(session, name, min_rows=0, timeout=POLL_TIMEOUT, stable=STABLE_POLLS):
    """Read ``name`` until its row count is unchanged across ``stable`` polls.

    Everything here is streaming -- AMPS replay into the leaves, Barrage deltas into
    the collector -- so a single read can catch a partially-ingested frontier.
    Settling on a stable count is the cheap, deterministic way to know the fleet has
    caught up. Every sleep is client-side on purpose: a ``run_script`` that sleeps
    holds the server's update graph and the table would never advance.
    """
    deadline = time.time() + timeout
    last_n, streak, df = -1, 0, None
    last_err = None
    while time.time() < deadline:
        try:
            df = read_table(session, name)
        except Exception as exc:  # noqa: BLE001 - table may not exist (or be failed) yet
            last_err = exc
            df = None
            time.sleep(POLL_INTERVAL)
            continue
        n = len(df)
        streak = streak + 1 if n == last_n else 1
        last_n = n
        if streak >= stable and n >= min_rows:
            return df
        time.sleep(POLL_INTERVAL)

    if df is None:
        pytest.fail(f"table '{name}' never became readable within {timeout}s (last error: {last_err})")
    pytest.fail(
        f"table '{name}' did not settle within {timeout}s "
        f"(last row count {last_n}, wanted >= {min_rows} stable over {stable} polls)"
    )


def probe_globals(session, names, probe_table="_rxe2e_globals_probe"):
    """Return ``{name: type-name}`` for server globals, ``"<absent>"`` when missing.

    ``run_script`` is the only way to see a global that is not a plain table: the
    query API entries are functions, ``remote_uri_dashboard`` is a ``deephaven.ui``
    element and ``remote_uri_runtime`` / ``remote_uri_pipeline`` are plain objects.
    The probe materializes the answer as a two-column table so it can come back over
    the normal Barrage path -- the same trick doc 10 s3 relies on for ``s/`` tickets.
    """
    literal = json.dumps(list(names))
    session.run_script(
        "from deephaven import new_table as _rxe2e_new_table\n"
        "from deephaven.column import string_col as _rxe2e_string_col\n"
        f"_rxe2e_names = {literal}\n"
        "_rxe2e_kinds = [\n"
        "    type(globals()[_n]).__name__ if _n in globals() else '<absent>'\n"
        "    for _n in _rxe2e_names\n"
        "]\n"
        f"{probe_table} = _rxe2e_new_table([\n"
        "    _rxe2e_string_col('Name', _rxe2e_names),\n"
        "    _rxe2e_string_col('Kind', _rxe2e_kinds),\n"
        "])\n"
    )
    df = read_table(session, probe_table)
    return {str(r["Name"]): str(r["Kind"]) for _, r in df.iterrows()}


def _safe(value, label):
    """Reject an identifier that must not reach a ``run_script`` body."""
    text = "" if value is None else str(value)
    assert _SAFE_ID.match(text), f"{label}={text!r} is not a plain identifier"
    return text


# --------------------------------------------------------------------------
# value helpers (identical semantics to the multi-OMS suite)
# --------------------------------------------------------------------------
def _norm(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _num(value) -> float:
    """Coerce a cell to a float, treating null/NaN/empty as 0.0.

    The cache stores an un-filled order's ``AvgPx`` as ``NULL_DOUBLE`` while the
    generator writes ``0.0``; every other difference still fails the comparison.
    """
    if value is None or value == "":
        return 0.0
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(out) else out


def _close(a, b, tol=TOL) -> bool:
    return abs(_num(a) - _num(b)) <= tol


def _close_rel(a, b, rel=REL_TOL) -> bool:
    """Relative comparison with an absolute floor, for marked notionals."""
    got, want = _num(a), _num(b)
    return abs(got - want) <= max(rel * max(abs(got), abs(want)), TOL)


def _key(oms, clordid) -> str:
    return f"{_norm(oms)}|{_norm(clordid)}"


def _by_key(df):
    """Index a recon/orders DataFrame by (``Oms``, ``ClOrdID``) rendered as a GlobalKey."""
    for col in ("Oms", "ClOrdID"):
        assert col in df.columns, f"table missing {col}; has {list(df.columns)}"
    return {_key(r["Oms"], r["ClOrdID"]): r for _, r in df.iterrows()}


def _compare_expected(expected, actual_by_key):
    """Return human-readable mismatches; empty means the collector agrees with the oracle."""
    problems = []
    missing = [
        _norm(e.get("GlobalKey")) for e in expected if _norm(e.get("GlobalKey")) not in actual_by_key
    ]
    if missing:
        problems.append(
            f"{len(missing)} generated hub-order(s) absent from orders_recon: {sorted(missing)[:10]}"
        )
    for exp in expected:
        gk = _norm(exp.get("GlobalKey"))
        row = actual_by_key.get(gk)
        if row is None:
            continue
        checks = (
            ("OrdStatus", _norm(exp.get("OrdStatus")), _norm(row.get("OrdStatus"))),
            ("ExtOrdID", _norm(exp.get("ExtOrdID")), _norm(row.get("ExtOrdID"))),
            ("GlobalKey", gk, _norm(row.get("GlobalKey"))),
            ("LinkState", _norm(exp.get("LinkState")), _norm(row.get("LinkState"))),
            ("BreakKind", _norm(exp.get("BreakKind")), _norm(row.get("BreakKind"))),
            ("RootKey", _norm(exp.get("RootGlobalKey")), _norm(row.get("RootKey"))),
        )
        for name, want, got in checks:
            if want != got:
                problems.append(f"{gk}: {name} expected {want!r} got {got!r}")
        for name in ("CumQty", "LeavesQty", "AvgPx"):
            if not _close(exp.get(name), row.get(name)):
                problems.append(
                    f"{gk}: {name} expected {exp.get(name)} got {row.get(name)}"
                    f" (scenario {_norm(exp.get('Scenario'))})"
                )
    return problems


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------
@pytest.fixture(scope="module")
def collector():
    """A session on the collector, or a skip explaining the stack is not up."""
    pytest.importorskip("pydeephaven", reason="pip install -r requirements.txt")
    if not _port_open(COLLECTOR_HOST, COLLECTOR_PORT):
        pytest.skip(
            f"collector not reachable at {COLLECTOR_HOST}:{COLLECTOR_PORT} -- "
            "start the stack with ./run_e2e.sh (or KEEP_STACK=1 leftovers)"
        )
    try:
        session = _new_session(COLLECTOR_HOST, COLLECTOR_PORT)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"cannot open a pydeephaven session on the collector: {exc}")
    yield session
    try:
        session.close()
    except Exception:  # noqa: BLE001
        pass


@pytest.fixture(scope="module")
def leaves():
    """``{leaf name: Session}`` for every leaf in ``RXE2E_LEAF_PORTS``."""
    pytest.importorskip("pydeephaven", reason="pip install -r requirements.txt")
    assert LEAVES, "RXE2E_LEAF_PORTS is empty; expected e.g. 'DH1:10011,DH2:10012'"
    sessions = {}
    for name, host, port in LEAVES:
        if not _port_open(host, port):
            for open_session in sessions.values():
                open_session.close()
            pytest.skip(f"leaf {name} not reachable at {host}:{port} -- start the stack")
        try:
            sessions[name] = _new_session(host, port)
        except Exception as exc:  # noqa: BLE001
            for open_session in sessions.values():
                open_session.close()
            pytest.skip(f"cannot open a pydeephaven session on leaf {name}: {exc}")
    yield sessions
    for session in sessions.values():
        try:
            session.close()
        except Exception:  # noqa: BLE001
            pass


@pytest.fixture(scope="module")
def leaf_hubs(leaves):
    """``{leaf name: [hub, ...]}`` read from each leaf's own ``leaf_config``.

    Read rather than hard-coded: the suite must hold for any hub assignment (doc 10
    s11 -- "adding DHn is one compose block and one JSON entry").
    """
    ports = {name: port for name, _host, port in LEAVES}
    hubs = {}
    for name, session in leaves.items():
        df = read_table(session, "leaf_config")
        assert "Oms" in df.columns and "Leaf" in df.columns, (
            f"leaf {name}: leaf_config has columns {list(df.columns)}, expected Leaf, Oms, Topic"
        )
        named = {_norm(v) for v in df.get("Leaf", [])}
        assert named == {name}, (
            f"the server on port {ports[name]} calls itself {sorted(named)} but "
            f"RXE2E_LEAF_PORTS calls it {name!r} -- REMOTEURI_LEAF_NAME and the port map disagree"
        )
        hubs[name] = sorted(_norm(v) for v in df["Oms"])
        assert hubs[name], f"leaf {name} folds no hub at all"
    return hubs


@pytest.fixture
def report(request):
    """Write a line straight to the terminal, bypassing pytest's stdout capture.

    The fleet's ``HeapUsedMb / Orders`` figures (doc 10 s2.4) are a *result* of this
    suite, not debug output: they have to appear in a passing run's log, and a plain
    ``print`` would only surface under ``-s`` or on failure.
    """
    reporter = request.config.pluginmanager.getplugin("terminalreporter")

    def write(line: str) -> None:
        if reporter is None:  # pragma: no cover - only when -p no:terminal
            print(line)
        else:
            reporter.write_line(line)

    return write


@pytest.fixture(scope="module")
def expected():
    """The per-hub-order rows the generator says it produced (``--emit-expected``)."""
    if not os.path.exists(EXPECTED_FILE):
        pytest.skip(
            f"no expected export at {EXPECTED_FILE} -- run ./run_e2e.sh so the "
            "generator emits its seed-42 expectations"
        )
    with open(EXPECTED_FILE, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    rows = payload if isinstance(payload, list) else payload.get("orders", [])
    if not rows:
        pytest.skip(f"{EXPECTED_FILE} contained no hub orders")
    return rows


@pytest.fixture(scope="module")
def recon(collector, expected):
    """``orders_recon`` once the whole fleet has settled."""
    # orders_recon is a row-preserving transform of the merged rx_orders (every join
    # in doc 09 s5.3/s5.4 is a natural_join), so both settle at the same count.
    poll_table(collector, "orders_all", min_rows=len(expected))
    return poll_table(collector, "orders_recon", min_rows=len(expected))


# --------------------------------------------------------------------------
# 1. every contract global exists on its own server
# --------------------------------------------------------------------------
def test_leaf_globals_present(leaves, leaf_hubs):
    """Doc 10 s5.1-s5.3: the streams, the leaf DAG, the four exports, and no more."""
    if raw_global_name is None:
        pytest.skip(f"remote_uri is not importable from {MODULE_SRC}: {_IMPORT_ERROR}")

    problems = []
    for name, session in leaves.items():
        wanted = list(LEAF_TABLES) + [raw_global_name(hub) for hub in leaf_hubs[name]]
        for table in wanted:
            try:
                session.open_table(table)
            except Exception as exc:  # noqa: BLE001
                problems.append(f"{name}: {table} -- {type(exc).__name__}: {exc}")

        kinds = probe_globals(session, list(LEAF_NON_TABLE_GLOBALS) + list(LEAF_FORBIDDEN_GLOBALS))
        for global_name in LEAF_NON_TABLE_GLOBALS:
            if kinds.get(global_name, "<absent>") == "<absent>":
                problems.append(f"{name}: global {global_name} is absent (probe saw {kinds})")
        for global_name in LEAF_FORBIDDEN_GLOBALS:
            if kinds.get(global_name, "<absent>") != "<absent>":
                problems.append(
                    f"{name}: {global_name} exists ({kinds[global_name]}) -- doc 10 s2.4 says a "
                    "leaf must NOT build it (AMPS is the audit trail; it is the largest table)"
                )

    assert not problems, (
        "leaf globals do not match doc 10 s5:\n  "
        + "\n  ".join(problems)
        + f"\ncheck `{CONTAINER_CLI} logs <leaf container>` for [remote-uri] errors"
    )


def test_collector_globals_present(collector, leaves):
    """Doc 10 s6/s9: the DAG, the per-leaf copies, the query API and the dashboard."""
    if leaf_global_name is None:
        pytest.skip(f"remote_uri is not importable from {MODULE_SRC}: {_IMPORT_ERROR}")

    wanted = list(COLLECTOR_TABLES)
    for name in leaves:
        wanted.extend(leaf_global_name(export, name) for export in LEAF_EXPORTS)

    failures = {}
    for table in wanted:
        try:
            collector.open_table(table)
        except Exception as exc:  # noqa: BLE001
            failures[table] = f"{type(exc).__name__}: {exc}"
    assert not failures, (
        "the collector did not publish these tables:\n  "
        + "\n  ".join(f"{k}: {v}" for k, v in sorted(failures.items()))
        + "\na failed (rather than missing) table means a leaf went away -- doc 10 s2.7"
    )

    kinds = probe_globals(collector, list(QUERY_API_NAMES) + list(COLLECTOR_NON_TABLE_GLOBALS))
    absent = [
        n
        for n in list(QUERY_API_NAMES) + list(COLLECTOR_NON_TABLE_GLOBALS)
        if kinds.get(n, "<absent>") == "<absent>"
    ]
    assert not absent, f"missing collector globals: {absent}\nprobe saw: {kinds}"
    assert kinds["remote_uri_dashboard"] != "NoneType", (
        "remote_uri_dashboard is None -- deephaven.ui was unavailable at startup; the "
        f"banner in `{CONTAINER_CLI} logs <collector>` says so (doc 10 s8)"
    )


# --------------------------------------------------------------------------
# 2. the sharding: each leaf folds its own hubs, the union is everything
# --------------------------------------------------------------------------
def test_leaf_partition_and_union(leaves, leaf_hubs, expected):
    """``rx_orders`` per leaf holds exactly that leaf's hubs; the union is the oracle.

    This is the whole point of the topology: a family's hops live on different
    servers (doc 10 s2.1) and no leaf ever sees a chain it does not own.
    """
    want_by_hub = collections.Counter(_norm(e.get("Oms")) for e in expected)
    union = {}
    problems = []

    for name, session in leaves.items():
        hubs = set(leaf_hubs[name])
        wanted_rows = sum(want_by_hub[hub] for hub in hubs)
        df = poll_table(session, "rx_orders", min_rows=wanted_rows)

        got_hubs = {_norm(v) for v in df.get("Oms", [])}
        if not got_hubs <= hubs:
            problems.append(
                f"{name}: rx_orders carries hubs {sorted(got_hubs - hubs)} that leaf_config "
                f"does not assign to it ({sorted(hubs)})"
            )
        if len(df) != wanted_rows:
            problems.append(
                f"{name}: rx_orders settled at {len(df)} rows, expected {wanted_rows} for "
                f"hubs {sorted(hubs)}. More means a dirty AMPS journal (run_e2e.sh always "
                "does `down -v`); fewer means the replay did not finish"
            )
        for _, row in df.iterrows():
            union.setdefault(_key(row["Oms"], row["ClOrdID"]), name)

    want_keys = {_norm(e.get("GlobalKey")) for e in expected}
    got_keys = set(union)
    missing = sorted(want_keys - got_keys)
    extra = sorted(got_keys - want_keys)
    if missing:
        problems.append(f"{len(missing)} expected hub-order(s) on no leaf: {missing[:10]}")
    if extra:
        problems.append(f"{len(extra)} unexpected hub-order(s) in the fleet: {extra[:10]}")

    assert not problems, "the fleet does not partition the tapes:\n  " + "\n  ".join(problems)


# --------------------------------------------------------------------------
# 3. cross-server linking equals the single-server oracle
# --------------------------------------------------------------------------
def test_collector_recon_matches_oracle(recon, expected):
    """Per hub-order final state on the collector equals the generator's oracle.

    Every hop was folded on the leaf that owns its hub and linked on the collector
    over the merged projection, so agreement here is the claim of doc 10 s2.1: an
    ``OMS-C`` order on one server links to its ``OMS-A`` ancestor on another exactly
    as it would inside one server.
    """
    assert len(recon) == len(expected), (
        f"orders_recon settled at {len(recon)} rows, expected {len(expected)} hub orders"
    )
    problems = _compare_expected(expected, _by_key(recon))
    assert not problems, (
        f"{len(problems)} orders_recon mismatch(es):\n  " + "\n  ".join(problems[:40])
    )


# --------------------------------------------------------------------------
# 4. the lookup the assignment asks for
# --------------------------------------------------------------------------
def _snapshot_lookup(session, oms, account, symbol):
    """Snapshot the four tables one lookup is asserted against, in ONE script.

    ``market_data_latest`` is a live random walk (doc 10 s6), so ``MarkPx`` -- and
    therefore every ``OpenNotional`` -- moves once a second. A ``run_script`` body
    holds the update graph, so the four ``snapshot()`` calls below observe the *same*
    cycle: the mids the totals were computed from are exactly the mids the reference
    is fed. Reading the tables separately over four round trips would race the walk.
    """
    oms, account, symbol = (_safe(oms, "oms"), _safe(account, "account"), _safe(symbol, "symbol"))
    session.run_script(
        "_rxe2e_md = market_data_latest.snapshot()\n"
        "_rxe2e_marked = orders_marked.snapshot()\n"
        f"_rxe2e_fam = find_exposure({oms!r}, {account!r}, {symbol!r}).snapshot()\n"
        f"_rxe2e_src = exposure_for({oms!r}, {account!r}, {symbol!r}).snapshot()\n"
    )
    return (
        read_table(session, "_rxe2e_md"),
        read_table(session, "_rxe2e_marked"),
        read_table(session, "_rxe2e_fam"),
        read_table(session, "_rxe2e_src"),
    )


def test_find_exposure_and_exposure_for_match_the_reference(collector, recon, expected):
    """``find_exposure`` returns whole families in order; ``exposure_for`` is the reference.

    For every (account, symbol) that has a family rooted at ``OMS-A``:

    * ``find_exposure`` must return **every hop** of **every** such family (the hops
      live on both leaves) sorted ``RootKey, Depth, Oms, OrderKey``;
    * ``exposure_for`` must equal ``remote_uri.exposure.sum_exposure`` over those
      families' root rows, fed with the *oracle's* ``AvgPx``/``CumQty``/``LeavesQty``
      and the ``MarkPx`` implied by the same snapshot's ``market_data_latest``.
    """
    if sum_exposure is None:
        pytest.skip(f"remote_uri is not importable from {MODULE_SRC}: {_IMPORT_ERROR}")

    by_key = _by_key(recon)
    oracle = {_norm(e.get("GlobalKey")): e for e in expected}
    family_of = collections.defaultdict(set)
    for row in expected:
        family_of[_norm(row.get("RootGlobalKey"))].add(_norm(row.get("GlobalKey")))

    # Roots of the source hub, and the (account, symbol) pairs they cover. Read from
    # the collector's own rows: the oracle carries no Account/Symbol column.
    roots = {}
    for gk, row in by_key.items():
        if _num(row.get("Depth")) == 0 and _norm(row.get("Oms")) == ROOT_HUB:
            roots[gk] = (_norm(row.get("Account")), _norm(row.get("Symbol")))
    assert roots, f"no family rooted at {ROOT_HUB} in orders_recon -- nothing to look up"

    pairs = sorted(set(roots.values()))
    problems = []
    for account, symbol in pairs:
        want_roots = {gk for gk, pair in roots.items() if pair == (account, symbol)}
        want_hops = set()
        for root in want_roots:
            want_hops |= family_of.get(root, {root})

        md, marked, family, source = _snapshot_lookup(collector, ROOT_HUB, account, symbol)
        label = f"{ROOT_HUB}/{account}/{symbol}"

        # -- find_exposure: every hop, in the frozen order --------------------
        got_hops = [_norm(v) for v in family.get("GlobalKey", [])]
        if set(got_hops) != want_hops:
            problems.append(
                f"{label}: find_exposure returned {len(got_hops)} hop(s); missing "
                f"{sorted(want_hops - set(got_hops))[:6]}, unexpected "
                f"{sorted(set(got_hops) - want_hops)[:6]}"
            )
        order_key = [
            (_norm(r["RootKey"]), _num(r["Depth"]), _norm(r["Oms"]), _norm(r["OrderKey"]))
            for _, r in family.iterrows()
        ]
        if order_key != sorted(order_key):
            problems.append(
                f"{label}: find_exposure is not sorted RootKey, Depth, Oms, OrderKey "
                f"(doc 10 s9): {order_key[:6]}"
            )
        stray_roots = {_norm(v) for v in family.get("RootKey", [])} - want_roots
        if stray_roots:
            problems.append(f"{label}: find_exposure mixed in families {sorted(stray_roots)[:6]}")

        # -- MarkPx really is the market mid (doc 10 s7) -----------------------
        mids = {_norm(r["Symbol"]): _num(r["Mid"]) for _, r in md.iterrows()}
        for _, row in family.iterrows():
            want_mark = mids.get(_norm(row["Symbol"]), _num(row.get("Price")))
            if not _close_rel(row.get("MarkPx"), want_mark):
                problems.append(
                    f"{label}: {_norm(row['GlobalKey'])} MarkPx={row.get('MarkPx')} but "
                    f"market_data_latest says Mid={want_mark} for {_norm(row['Symbol'])}"
                )

        # -- exposure_for: the totals, against the pure-python reference --------
        marked_by_key = {_norm(r["GlobalKey"]): r for _, r in marked.iterrows()}
        reference_rows = []
        for root in sorted(want_roots):
            row = marked_by_key.get(root)
            exp = oracle.get(root)
            if row is None or exp is None:
                problems.append(f"{label}: root {root} is missing from orders_marked/the oracle")
                continue
            # Quantities from the ORACLE (the independent implementation), identity
            # and static order attributes from the row -- the oracle carries no
            # Account/Symbol/Side/Price/OrderQty column.
            reference_rows.append(
                {
                    "Symbol": _norm(row["Symbol"]),
                    "Side": _norm(row["Side"]),
                    "Price": _num(row.get("Price")),
                    "OrderQty": _num(row.get("OrderQty")),
                    "AvgPx": _num(exp.get("AvgPx")),
                    "CumQty": _num(exp.get("CumQty")),
                    "LeavesQty": _num(exp.get("LeavesQty")),
                }
            )
        want_totals = sum_exposure(reference_rows, mids)

        if len(source) != 1:
            problems.append(
                f"{label}: exposure_for returned {len(source)} row(s), expected exactly one "
                "(it is grouped by RootOms, RootAccount, RootSymbol)"
            )
            continue
        got = source.iloc[0]
        if int(_num(got.get("Orders"))) != int(want_totals["Orders"]):
            problems.append(
                f"{label}: Orders={got.get('Orders')}, expected {want_totals['Orders']} "
                f"(root rows of {len(want_roots)} family/families)"
            )
        for column in EXPOSURE_SUM_COLUMNS:
            if not _close_rel(got.get(column), want_totals[column]):
                problems.append(
                    f"{label}: {column}={got.get(column)!r}, reference says "
                    f"{want_totals[column]!r} (relative tolerance {REL_TOL})"
                )

    assert not problems, (
        f"{len(problems)} exposure mismatch(es) over {len(pairs)} lookup(s):\n  "
        + "\n  ".join(problems[:40])
    )


def test_order_exposure_reference_matches_every_marked_row(collector, recon):
    """Every ``orders_marked`` row's five doc 10 s7 columns equal the python reference.

    ``exposure_for`` only compares the *sums*; this walks all 72 hops so a formula
    that is wrong on one hop (a null ``AvgPx``, a ``SELL`` sign, a symbol with no
    quote) cannot cancel out inside an aggregate.
    """
    if order_exposure is None:
        pytest.skip(f"remote_uri is not importable from {MODULE_SRC}: {_IMPORT_ERROR}")

    collector.run_script(
        "_rxe2e_md = market_data_latest.snapshot()\n_rxe2e_marked = orders_marked.snapshot()\n"
    )
    md = read_table(collector, "_rxe2e_md")
    marked = read_table(collector, "_rxe2e_marked")
    mids = {_norm(r["Symbol"]): _num(r["Mid"]) for _, r in md.iterrows()}

    problems = []
    for _, row in marked.iterrows():
        want = order_exposure(
            {
                "AvgPx": row.get("AvgPx"),
                "CumQty": row.get("CumQty"),
                "LeavesQty": row.get("LeavesQty"),
                "Price": row.get("Price"),
                "Side": _norm(row.get("Side")),
            },
            mids.get(_norm(row.get("Symbol"))),
        )
        for column, value in want.items():
            if not _close_rel(row.get(column), value):
                problems.append(
                    f"{_norm(row.get('GlobalKey'))}: {column}={row.get(column)!r}, "
                    f"reference says {value!r}"
                )
    assert not problems, (
        f"{len(problems)} marked-column mismatch(es) over {len(marked)} hops:\n  "
        + "\n  ".join(problems[:40])
    )


# --------------------------------------------------------------------------
# 5. the remote call: the filter runs on the leaf
# --------------------------------------------------------------------------
def _pick_key(recon, hub):
    """A ``GlobalKey`` on ``hub`` that actually has executions (``CumQty > 0``)."""
    candidates = [
        _norm(r["GlobalKey"])
        for _, r in recon.iterrows()
        if _norm(r["Oms"]) == hub and _num(r.get("CumQty")) > 0
    ]
    return sorted(candidates)[0] if candidates else None


def test_remote_executions_match_the_owning_leaf(collector, leaves, leaf_hubs, recon, report):
    """``remote_executions`` == the owning leaf's own ``oms_executions``, filtered.

    The whole point of doc 10 s3's remote query: ``oms_executions`` (about 70% of the
    message count -- doc 10 s2.4) is never held by the collector, the ``where`` is
    compiled and run *on the leaf*, and only the matching rows cross the wire. The
    assertion is that the rows that arrive are exactly the rows on the leaf.
    """
    hub_owner = {hub: leaf for leaf, hubs in leaf_hubs.items() for hub in hubs}
    upstream_hub, downstream_hub = ROOT_HUB, "OMS-C"
    keys = {hub: _pick_key(recon, hub) for hub in (upstream_hub, downstream_hub)}
    if not all(keys.values()):
        pytest.skip(f"no filled orders to query: {keys}")
    assert len({hub_owner.get(hub) for hub in keys}) == 2, (
        f"{upstream_hub} and {downstream_hub} are on the same leaf ({hub_owner}); this test "
        "is meant to cross two servers"
    )

    problems = []
    for hub, key in keys.items():
        leaf_name = hub_owner[hub]
        assert leaf_name in leaves, f"hub {hub} is owned by unknown leaf {leaf_name!r}"
        safe_key = _safe(key, "GlobalKey")
        safe_hub = _safe(hub, "Oms")

        # what the leaf itself holds
        direct = read_table(leaves[leaf_name], "oms_executions")
        want = sorted(
            _norm(r["ExecID"]) for _, r in direct.iterrows() if _norm(r["GlobalKey"]) == key
        )
        if not want:
            problems.append(f"{key}: leaf {leaf_name} has no executions for it at all")
            continue

        # what the remote query brings back, plus the leaf the API says owns it
        collector.run_script(
            f"_rxe2e_rq = remote_executions({safe_key!r})\n"
            f"_rxe2e_rl = remote_live_executions({safe_key!r})\n"
            f"_rxe2e_leaf_of = leaf_of({safe_hub!r})\n"
            "from deephaven import new_table as _rxe2e_new_table\n"
            "from deephaven.column import string_col as _rxe2e_string_col\n"
            "_rxe2e_meta = _rxe2e_new_table([\n"
            "    _rxe2e_string_col('Leaf', [str(_rxe2e_leaf_of)]),\n"
            "    _rxe2e_string_col('StaticRefreshing', [str(_rxe2e_rq.is_refreshing)]),\n"
            "    _rxe2e_string_col('LiveRefreshing', [str(_rxe2e_rl.is_refreshing)]),\n"
            "])\n"
        )
        static = read_table(collector, "_rxe2e_rq")
        live = read_table(collector, "_rxe2e_rl")
        meta = read_table(collector, "_rxe2e_meta").iloc[0]

        got_static = sorted(_norm(v) for v in static.get("ExecID", []))
        got_live = sorted(_norm(v) for v in live.get("ExecID", []))
        if got_static != want:
            problems.append(
                f"{key}: remote_executions returned ExecIDs {got_static}, leaf {leaf_name} "
                f"holds {want}"
            )
        if got_live != want:
            problems.append(
                f"{key}: remote_live_executions returned ExecIDs {got_live}, leaf "
                f"{leaf_name} holds {want}"
            )
        if {_norm(v) for v in static.get("GlobalKey", [])} - {key}:
            problems.append(f"{key}: remote_executions returned rows for other orders")
        if _norm(meta["Leaf"]) != leaf_name:
            problems.append(
                f"{hub}: leaf_of said {meta['Leaf']!r}, leaf_config says {leaf_name!r}"
            )
        if _norm(meta["LiveRefreshing"]) != "True":
            problems.append(
                f"{key}: remote_live_executions returned a static table -- doc 10 s9 makes it "
                "a live Barrage subscription (is_refreshing=" + _norm(meta["LiveRefreshing"]) + ")"
            )
        if _norm(meta["StaticRefreshing"]) != "False":
            problems.append(
                f"{key}: remote_executions returned a refreshing table -- doc 10 s9 makes it a "
                "snapshot, and its rx_q_<n> global is dropped on the leaf immediately"
            )
        report(f"    remote query {key} -> leaf {leaf_name}: {len(want)} execution(s) on the wire")

    assert not problems, "the remote query disagrees with the leaf:\n  " + "\n  ".join(problems)


# --------------------------------------------------------------------------
# 6. fleet health -- and the first bytes-per-order measurement
# --------------------------------------------------------------------------
def test_fleet_reports_every_leaf(collector, leaves, expected, report):
    """One ``fleet`` row per leaf, ``Orders`` == that leaf's ``rx_orders`` size.

    Also prints ``HeapUsedMb / Orders`` per leaf: doc 10 s2.4 estimates ~4 KB per
    hub-order with no measurement behind it, and this is the first data point. The
    demo's numbers are dominated by JVM baseline heap, so it is a floor, not a proof.
    """
    sizes = {name: len(read_table(session, "rx_orders")) for name, session in leaves.items()}
    deadline = time.time() + POLL_TIMEOUT
    fleet = None
    while time.time() < deadline:
        fleet = poll_table(collector, "fleet", min_rows=len(leaves))
        rows = {_norm(r["Leaf"]): r for _, r in fleet.iterrows()}
        if set(rows) == set(sizes) and all(
            int(_num(rows[name]["Orders"])) == sizes[name] for name in sizes
        ):
            break
        # rx_leaf_stats refreshes on its own clock (REMOTEURI_STATS_PERIOD_MS), so a
        # lagging count is expected for up to one period after ingestion settles.
        time.sleep(POLL_INTERVAL)
        sizes = {name: len(read_table(session, "rx_orders")) for name, session in leaves.items()}

    rows = {_norm(r["Leaf"]): r for _, r in fleet.iterrows()}
    assert set(rows) == set(sizes), (
        f"fleet names leaves {sorted(rows)}, the configured fleet is {sorted(sizes)}"
    )
    assert len(fleet) == len(leaves), f"fleet has {len(fleet)} rows for {len(leaves)} leaves"

    problems = []
    total_orders = 0
    for name, size in sorted(sizes.items()):
        row = rows[name]
        orders = int(_num(row["Orders"]))
        total_orders += orders
        if orders != size:
            problems.append(f"{name}: fleet says {orders} orders, rx_orders has {size}")
        if int(_num(row["Failed"])) != 0:
            problems.append(f"{name}: {int(_num(row['Failed']))} failed message(s) in the fold")
        heap = int(_num(row["HeapUsedMb"]))
        per_order = (heap * 1024.0 / orders) if orders else float("nan")
        report(
            f"    {name:6s} hubs={_norm(row['Hubs'])!r} orders={orders} "
            f"executions={int(_num(row['Executions']))} processed={int(_num(row['Processed']))} "
            f"pending={int(_num(row['Pending']))} heap={heap}MB "
            f"({per_order:.0f} KB/order, JVM baseline included)"
        )
    assert total_orders == len(expected), (
        f"the fleet holds {total_orders} hub-orders, the generator produced {len(expected)}"
    )
    assert not problems, "fleet disagrees with the leaves:\n  " + "\n  ".join(problems)


# --------------------------------------------------------------------------
# 7. restart resilience -- LAST: it takes a leaf (and the collector DAG) down
# --------------------------------------------------------------------------
def _banner_count(container: str, banner: str) -> int:
    """How many times a leaf has announced itself ready in its (cumulative) log.

    ``restart`` keeps the container, so its log keeps the *old* banner too and the
    only reliable "it came back" signal is the count going up. The tail is generous
    (a leaf logs a few lines a minute once it is idle) so both banners stay in view.
    """
    return len(re.findall(banner, _container_log(container, tail=20000)))


def _collector_state(session):
    """``{connected, complete, recon_failed, report}`` read off the live collector.

    This is what makes the restart assertion mean anything. A Deephaven table whose
    remote source went away is **failed**, but a failed table still answers a Barrage
    snapshot with its last known rows -- so ``orders_recon`` reads 72 rows both when
    the collector has rebuilt itself and when it is serving a corpse. Row counts
    cannot tell those apart; ``Table.is_failed`` and the runtime's own resolve report
    can.
    """
    session.run_script(
        "from deephaven import new_table as _rxe2e_new_table\n"
        "from deephaven.column import string_col as _rxe2e_string_col\n"
        "def _rxe2e_probe_value(_reader):\n"
        "    try:\n"
        "        return str(_reader())\n"
        "    except Exception as _exc:\n"
        "        return f'<{type(_exc).__name__}: {_exc}>'\n"
        "_rxe2e_state = _rxe2e_new_table([\n"
        "    _rxe2e_string_col('Key', ['connected', 'complete', 'recon_failed', 'report']),\n"
        "    _rxe2e_string_col('Value', [\n"
        "        _rxe2e_probe_value(lambda: remote_uri_runtime.connected),\n"
        "        _rxe2e_probe_value(\n"
        "            lambda: bool(remote_uri_runtime.report and remote_uri_runtime.report.complete)\n"
        "        ),\n"
        "        _rxe2e_probe_value(lambda: orders_recon.is_failed),\n"
        "        _rxe2e_probe_value(\n"
        "            lambda: remote_uri_runtime.report.describe() if remote_uri_runtime.report else ''\n"
        "        ),\n"
        "    ]),\n"
        "])\n"
    )
    df = read_table(session, "_rxe2e_state")
    return {str(r["Key"]): str(r["Value"]) for _, r in df.iterrows()}


#: The signature of a resolver session that outlived the leaf it points at. See
#: :func:`_reconnect` -- it turns an opaque "4 exports missing" into the cause.
_STALE_SESSION = re.compile(r"UNAUTHENTICATED: Authentication details invalid")


def _reconnect(collector, report, container=None, timeout=RESTART_TIMEOUT):
    """Call ``reconnect()`` until the collector says it is connected again.

    ``reconnect()`` makes a *single* resolve pass (doc 10 s6) and never raises: a leaf
    that is up but not yet exported leaves the runtime unconnected and prints one line
    per missing export, so retrying is the caller's job. Two transient causes are
    normal for a few seconds after a container restart -- the leaf's gRPC port
    (``Connection refused``) and the JVM's ~30s positive DNS cache still pointing at
    the container's previous address.

    Returns:
        The final :func:`_collector_state` dict (connected == "True").
    """
    deadline = time.time() + timeout
    attempt = 0
    stale = False
    state = _collector_state(collector)
    while time.time() < deadline:
        attempt += 1
        collector.run_script("reconnect()")
        state = _collector_state(collector)
        if state["connected"] == "True" and state["complete"] == "True":
            report(f"    reconnect() succeeded on attempt {attempt}: {state['report']}")
            return state
        report(f"    reconnect() attempt {attempt}: {state['report']} -- retrying")
        # A stale resolver session never heals (see the diagnosis below), so once the
        # signature is in the log there is nothing to wait for: fail in seconds with
        # the cause rather than in five minutes with "exports missing".
        if attempt >= 3 and _STALE_SESSION.search(_container_log(container, tail=2000)):
            stale = True
            break
        time.sleep(POLL_INTERVAL * 2)

    # A permanent failure is worth naming precisely: with REMOTEURI_RESOLVER=uri the
    # subscription goes through the *server's* BarrageTableResolver, which caches one
    # session per target and never re-authenticates it. After a leaf restart that
    # cached session is invalid for good, and nothing the collector's own resolver
    # closes can reach it -- so every resolve fails UNAUTHENTICATED however long the
    # caller retries. REMOTEURI_RESOLVER=barrage subscribes on a session the resolver
    # owns and drops in close(), and recovers on the first attempt.
    diagnosis = ""
    if stale or _STALE_SESSION.search(_container_log(container, tail=2000)):
        diagnosis = (
            "\n  the collector log says 'UNAUTHENTICATED: Authentication details invalid': "
            "the subscription is being re-resolved on a Barrage session that died with the "
            "leaf. With REMOTEURI_RESOLVER=uri that session lives inside the *server's* "
            "BarrageTableResolver cache, which reconnect() cannot invalidate, so it never "
            "heals. Doc 10 s2.7 promises this recovery, so this is a defect in the resolver, "
            "not in the test: REMOTEURI_RESOLVER=barrage subscribes on a session "
            "RemoteResolver.close() owns and recovers on the first attempt."
        )
    raise AssertionError(
        f"reconnect() never re-resolved the fleet ({attempt} attempt(s) in "
        f"{timeout:.0f}s); last report: {state['report']}{diagnosis}"
    )


def test_restart_leaf_and_reconnect_rebuilds_identical_recon(
    collector, leaves, leaf_hubs, expected, report
):
    """Restart a leaf, ``reconnect()`` the collector, and assertion 3 must hold again.

    Two independent recoveries in one: the leaf replays its tapes from the AMPS
    journal (``EPOCH`` bookmark -- doc 10 s2.6), and the collector rebuilds its
    entire DAG from fresh Barrage subscriptions, because in Deephaven a failed remote
    table fails every dependent (doc 10 s2.7). The assertion is therefore the *same*
    per-order comparison as before the restart, not a row count.

    The row count would in fact prove nothing at all: a failed Barrage table keeps
    answering snapshots with its last known rows, so ``orders_recon`` still reads all
    72 hub-orders while the collector is disconnected. The test therefore checks the
    doc 10 s2.7 failure model explicitly -- ``orders_recon.is_failed`` after the
    restart -- and only compares against the oracle once the runtime reports a
    *complete* re-resolve.
    """
    leaf_name = LEAVES[0][0]
    container = _leaf_container(leaf_name)
    if container is None:
        pytest.skip(
            f"no container name for leaf {leaf_name} in RXE2E_CONTAINERS={CONTAINERS_ENV!r}"
        )
    probe = subprocess.run(
        [CONTAINER_CLI, "container", "inspect", container, "--format", "{{.Id}}"],
        capture_output=True,
        text=True,
    )
    if probe.returncode != 0:
        pytest.skip(f"cannot inspect container {container} via {CONTAINER_CLI}: {probe.stderr}")

    banner = rf"Remote-URI leaf {re.escape(leaf_name)} -- ready"
    before = _banner_count(container, banner)
    host, port = next((h, p) for n, h, p in LEAVES if n == leaf_name)
    hub_rows = sum(
        1 for e in expected if _norm(e.get("Oms")) in set(leaf_hubs[leaf_name])
    )

    was = _collector_state(collector)
    assert was["connected"] == "True" and was["recon_failed"] == "False", (
        f"the collector was already broken before the restart: {was}"
    )

    restart = subprocess.run(
        [CONTAINER_CLI, "restart", container], capture_output=True, text=True, timeout=300
    )
    assert restart.returncode == 0, (
        f"`{CONTAINER_CLI} restart {container}` failed: {restart.stderr}"
    )

    # 1. the leaf comes back and re-announces itself...
    session = _wait_for_session(host, port, RESTART_TIMEOUT)
    assert session is not None, (
        f"leaf {leaf_name} did not accept connections within {RESTART_TIMEOUT}s of the restart; "
        f"see `{CONTAINER_CLI} logs {container}`"
    )
    try:
        deadline = time.time() + RESTART_TIMEOUT
        while time.time() < deadline and _banner_count(container, banner) <= before:
            time.sleep(POLL_INTERVAL)
        assert _banner_count(container, banner) > before, (
            f"leaf {leaf_name} never printed '{banner}' again within {RESTART_TIMEOUT}s "
            f"(app mode may have failed; see `{CONTAINER_CLI} logs {container}`)"
        )
        # ...and replays its own tapes from EPOCH.
        replayed = poll_table(session, "rx_orders", min_rows=hub_rows, timeout=RESTART_TIMEOUT)
        assert len(replayed) == hub_rows, (
            f"after the restart leaf {leaf_name} holds {len(replayed)} orders, "
            f"expected {hub_rows} from the AMPS journal"
        )
    finally:
        try:
            session.close()
        except Exception:  # noqa: BLE001
            pass

    # 2. the collector noticed: every dependent of the lost subscription failed...
    broken = _collector_state(collector)
    assert broken["recon_failed"] == "True", (
        "orders_recon did NOT fail when the leaf went away. Doc 10 s2.7 is written "
        f"around Deephaven's 'a failed remote table fails every dependent' semantics: {broken}"
    )

    # 3. ...and reconnect() rebuilds the whole DAG from fresh subscriptions.
    state = _reconnect(collector, report, container=_collector_container())
    assert state["recon_failed"] == "False", (
        f"reconnect() reported success but orders_recon is still failed: {state}"
    )
    rebuilt = poll_table(collector, "orders_recon", min_rows=len(expected))

    # 4. Every hub-order is exactly what it was before.
    assert len(rebuilt) == len(expected), (
        f"orders_recon rebuilt with {len(rebuilt)} rows, expected {len(expected)}"
    )
    problems = _compare_expected(expected, _by_key(rebuilt))
    assert not problems, (
        f"{len(problems)} mismatch(es) after restarting {leaf_name} and reconnecting -- "
        "the replay or the rebuild is not idempotent:\n  " + "\n  ".join(problems[:40])
    )
