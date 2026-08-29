"""Multi-OMS e2e: four correlated drop-copy tapes -> Kafka -> Deephaven, asserted
through pydeephaven against the generator's own expected export (doc 09 s10).

Run it through ``run_e2e.sh`` (brings the stack up with ``DH_APP=multi-oms-blotter``,
runs ``--multi-oms`` and then invokes pytest). Running ``pytest`` directly works too,
provided the stack is already up and ``expected_multi_oms.json`` exists -- otherwise
every test skips with a message saying what is missing, so this file is safe in CI.

What is asserted (doc 09 s10):

1. every global doc 09 s5 promises exists on the server -- plain tables through
   ``open_table``, ``orders_tree`` and the dashboard through ``run_script`` (a
   hierarchical table is not a plain-table ticket, and the dashboard is not a table
   at all);
2. per hub-order final ``OrdStatus`` / ``CumQty`` / ``LeavesQty`` / ``AvgPx`` /
   ``ExtOrdID`` / ``LinkState`` / ``BreakKind`` / ``RootKey`` match the generator's
   ``--emit-expected`` output, keyed by (``Oms``, ``ClOrdID``);
3. clean / ``working_fanout`` / ``late_parent`` families are absent from
   ``oms_breaks`` (``late_parent`` proves out-of-order arrival healed);
   ``missed_fill`` shows ``QTY_BREAK`` on exactly its OMS-A and OMS-B-parent rows;
   ``dangling_child`` shows ``DANGLING``; ``partial_route`` shows ``UNROUTED`` at
   OMS-B-parent and is **not** in ``oms_breaks`` (but is in ``breaks_only()``);
4. the per-edge math itself: a clean family's parents carry ``HasChildren`` with
   zero deltas, its OMS-C leaves carry none;
5. ``find_chain`` returns the whole family from either end of the chain;
6. ``break_summary`` counts equal what the expected export implies;
7. restart resilience: the whole recon rebuilds identically from the Kafka journal.

The expected export is the *independent* oracle: the generator computes the same
per-edge taxonomy (doc 09 s5.4) from its own scripts, and the point of the suite is
that two independent implementations agree.
"""

from __future__ import annotations

import collections
import json
import math
import os
import socket
import subprocess
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))

DH_HOST = os.environ.get("DH_HOST", "localhost")
DH_PORT = int(os.environ.get("DH_PORT", "10000"))
DH_CONTAINER = os.environ.get("DH_CONTAINER", "fix42-deephaven")
CONTAINER_CLI = os.environ.get("CONTAINER_CLI", "podman")
EXPECTED_FILE = os.environ.get("MOMS_EXPECTED", os.path.join(HERE, "expected_multi_oms.json"))

POLL_TIMEOUT = float(os.environ.get("MOMS_POLL_TIMEOUT", "120"))
POLL_INTERVAL = float(os.environ.get("MOMS_POLL_INTERVAL", "2"))
STABLE_POLLS = int(os.environ.get("MOMS_STABLE_POLLS", "2"))
RESTART_TIMEOUT = float(os.environ.get("MOMS_RESTART_TIMEOUT", "240"))

TOL = 1e-6
NOTIONAL_TOL = 0.01

HUB_A = "OMS-A"
HUB_BP = "OMS-B-parent"
HUB_BC = "OMS-B-child"
HUB_C = "OMS-C"

#: Plain-table globals doc 09 s4.1/s5 promises. ``orders_tree`` is deliberately not
#: here: a hierarchical table has no plain-table ticket, so it is probed separately.
REQUIRED_TABLES = [
    # per-hub raw blinks (one kc.consume per hub topic -- doc 09 s4)
    "oms_raw_oms_a",
    "oms_raw_oms_b_parent",
    "oms_raw_oms_b_child",
    "oms_raw_oms_c",
    # the five published streams (doc 09 s4.1)
    "oms_order_state_blink",
    "oms_executions_blink",
    "oms_order_events_blink",
    "oms_fix_messages_blink",
    "oms_ingest_errors",
    # derived nodes (doc 09 s5.1 - s5.5)
    "hub_config",
    "oms_orders_latest",
    "oms_executions",
    "oms_executions_latest",
    "oms_events",
    "oms_fix_messages",
    "id_index",
    "orders_linked",
    "child_rollup",
    "orders_recon",
    "oms_breaks",
    "break_summary",
    "chain_summary",
    "chain_recon",
    # dashboard filter sources
    "account_list",
    "symbol_list",
    "side_list",
    "oms_list",
]

#: Non-table globals probed through ``run_script``.
NON_TABLE_GLOBALS = ["multi_oms_pipeline", "multi_oms_topology", "multi_oms_blotter", "orders_tree"]

#: Query-API functions doc 09 s7 promises.
QUERY_API_NAMES = [
    "find_chain",
    "get_order",
    "find_by_account",
    "find_by_symbol",
    "hub_orders",
    "breaks_only",
    "order_detail",
]

CLEAN_SCENARIOS = ("clean_fill", "working_fanout", "late_parent")


# --------------------------------------------------------------------------
# connection helpers
# --------------------------------------------------------------------------
def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _new_session():
    from pydeephaven import Session

    return Session(host=DH_HOST, port=DH_PORT)


def _wait_for_session(timeout: float):
    """Return a live Session, or None if the server never came back."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _port_open(DH_HOST, DH_PORT):
            try:
                return _new_session()
            except Exception:  # noqa: BLE001 - gRPC comes up a beat after the port
                pass
        time.sleep(POLL_INTERVAL)
    return None


def _server_log(tail: int = 400) -> str:
    """Best-effort tail of the Deephaven container log (empty string if unavailable)."""
    try:
        done = subprocess.run(
            [CONTAINER_CLI, "logs", "--tail", str(tail), DH_CONTAINER],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception:  # noqa: BLE001 - the CLI may not be on PATH at all
        return ""
    return (done.stdout or "") + (done.stderr or "")


def _resolve_container():
    """Find the Deephaven container name, tolerating compose's naming scheme."""
    probe = subprocess.run(
        [CONTAINER_CLI, "inspect", DH_CONTAINER, "--format", "{{.Name}}"],
        capture_output=True,
        text=True,
    )
    if probe.returncode == 0:
        return DH_CONTAINER
    listing = subprocess.run(
        [CONTAINER_CLI, "ps", "--format", "{{.Names}}"], capture_output=True, text=True
    )
    if listing.returncode != 0:
        return None
    for name in listing.stdout.split():
        if "deephaven" in name:
            return name
    return None


@pytest.fixture(scope="module")
def session():
    pytest.importorskip("pydeephaven", reason="pip install -r requirements.txt")
    if not _port_open(DH_HOST, DH_PORT):
        pytest.skip(
            f"Deephaven not reachable at {DH_HOST}:{DH_PORT} -- "
            "start the stack with ./run_e2e.sh (or KEEP_STACK=1 leftovers)"
        )
    try:
        s = _new_session()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"cannot open pydeephaven session: {exc}")
    yield s
    try:
        s.close()
    except Exception:  # noqa: BLE001 - may already be dead after the restart test
        pass


def read_table(session, name):
    """Snapshot a server-side global as a pandas DataFrame."""
    return session.open_table(name).to_arrow().to_pandas()


def poll_table(session, name, min_rows=0, timeout=POLL_TIMEOUT, stable=STABLE_POLLS):
    """Read ``name`` until its row count is unchanged across ``stable`` consecutive polls.

    The pipeline is streaming, so a single read can catch a partially-ingested
    frontier. Settling on a stable count is the cheap, deterministic way to know
    ingestion has caught up. Every sleep is client-side on purpose: a ``run_script``
    that sleeps holds the server's update graph and the table would never advance.
    """
    deadline = time.time() + timeout
    last_n, streak, df = -1, 0, None
    last_err = None
    while time.time() < deadline:
        try:
            df = read_table(session, name)
        except Exception as exc:  # noqa: BLE001 - table may not exist yet
            last_err = exc
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


def probe_globals(session, names, probe_table="e2e_globals_probe"):
    """Return ``{name: type-name}`` for server globals, ``"<absent>"`` when missing.

    ``run_script`` is the only way to see a global that is not a plain table:
    ``orders_tree`` is hierarchical (no plain-table ticket), ``multi_oms_blotter`` is
    a ``deephaven.ui`` object and the query API entries are functions. The probe
    materializes the answer as a two-column table so it can come back over the
    normal Barrage path.
    """
    literal = json.dumps(list(names))
    session.run_script(
        "from deephaven import new_table as _e2e_new_table\n"
        "from deephaven.column import string_col as _e2e_string_col\n"
        f"_e2e_names = {literal}\n"
        "_e2e_kinds = [\n"
        "    type(globals()[_n]).__name__ if _n in globals() else '<absent>'\n"
        "    for _n in _e2e_names\n"
        "]\n"
        f"{probe_table} = _e2e_new_table([\n"
        "    _e2e_string_col('Name', _e2e_names),\n"
        "    _e2e_string_col('Kind', _e2e_kinds),\n"
        "])\n"
    )
    df = read_table(session, probe_table)
    return {str(r["Name"]): str(r["Kind"]) for _, r in df.iterrows()}


# --------------------------------------------------------------------------
# expected-data helpers
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


def _tri(value):
    """Coerce a Deephaven tri-state boolean cell to ``True`` / ``False`` / ``None``.

    A primitive ``boolean`` column arrives as a numpy scalar and a nullable one as
    ``None``/``NaN``; neither is safely comparable with ``is True``.
    """
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return bool(value)


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
def recon(session, expected):
    """``orders_recon`` once ingestion has settled, keyed rows included."""
    # orders_recon is a row-preserving transform of oms_orders_latest (every join in
    # doc 09 s5.3/s5.4 is a natural_join), so both settle at the same count.
    poll_table(session, "oms_orders_latest", min_rows=len(expected))
    return poll_table(session, "orders_recon", min_rows=len(expected))


def _key(oms, clordid) -> str:
    return f"{_norm(oms)}|{_norm(clordid)}"


def _by_key(df):
    """Index a recon DataFrame by (``Oms``, ``ClOrdID``) rendered as a GlobalKey."""
    for col in ("Oms", "ClOrdID"):
        assert col in df.columns, f"orders_recon missing {col}; has {list(df.columns)}"
    return {_key(r["Oms"], r["ClOrdID"]): r for _, r in df.iterrows()}


def _scenario_rows(expected, scenario):
    return [e for e in expected if _norm(e.get("Scenario")) == scenario]


def _families(rows):
    """Group expected rows by ``RootGlobalKey`` (the chain id)."""
    out = collections.defaultdict(list)
    for row in rows:
        out[_norm(row.get("RootGlobalKey"))].append(row)
    return out


def _compare_expected(expected, actual_by_key):
    """Return a list of human-readable mismatches; empty means the blotter agrees."""
    problems = []
    missing = [_norm(e.get("GlobalKey")) for e in expected if _norm(e.get("GlobalKey")) not in actual_by_key]
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
# 1. globals
# --------------------------------------------------------------------------
def test_required_table_globals_open(session):
    """Every plain-table node of doc 09 s5 is reachable through ``open_table``."""
    failures = {}
    for name in REQUIRED_TABLES:
        try:
            session.open_table(name)
        except Exception as exc:  # noqa: BLE001
            failures[name] = f"{type(exc).__name__}: {exc}"
    assert not failures, (
        "app mode did not publish these tables:\n  "
        + "\n  ".join(f"{k}: {v}" for k, v in sorted(failures.items()))
        + f"\ncheck `{CONTAINER_CLI} logs {DH_CONTAINER}` for [multi-oms] errors"
    )


def test_non_table_globals_and_query_api_present(session):
    """The dashboard, the topology objects and the query API are exported too."""
    kinds = probe_globals(session, NON_TABLE_GLOBALS + QUERY_API_NAMES)

    absent = [n for n in NON_TABLE_GLOBALS + QUERY_API_NAMES if kinds.get(n, "<absent>") == "<absent>"]
    # orders_tree is the one legitimately optional global: the server may reject
    # Table.tree, in which case the app says so in its banner (doc 09 s5.5).
    tree_excused = False
    if "orders_tree" in absent and "orders_tree UNAVAILABLE" in _server_log():
        absent.remove("orders_tree")
        tree_excused = True

    assert not absent, (
        f"missing server globals: {absent}\nprobe saw: {kinds}\n"
        f"check `{CONTAINER_CLI} logs {DH_CONTAINER}` for the [multi-oms] banner"
    )
    assert kinds["multi_oms_blotter"] != "NoneType", (
        "multi_oms_blotter is None -- deephaven.ui was unavailable at startup; the "
        f"banner in `{CONTAINER_CLI} logs {DH_CONTAINER}` says which"
    )
    if tree_excused:
        pytest.skip("server rejected Table.tree; the banner reports 'orders_tree UNAVAILABLE'")
    assert "Tree" in kinds["orders_tree"], (
        f"orders_tree is a {kinds['orders_tree']}, expected a hierarchical TreeTable "
        "(doc 09 s5.5: orders_recon.tree('GlobalKey', 'ParentGlobalKey', promote_orphans=True))"
    )


# --------------------------------------------------------------------------
# 2 + 3. the blotter matches the generator's own edge math
# --------------------------------------------------------------------------
def test_orders_latest_settles_at_expected_size(session, expected):
    """``oms_orders_latest`` holds exactly one row per generated hub order."""
    df = poll_table(session, "oms_orders_latest", min_rows=len(expected))
    assert len(df) == len(expected), (
        f"oms_orders_latest settled at {len(df)} rows, expected {len(expected)} hub orders. "
        "A larger count means a dirty topic (run_e2e.sh always does `down -v` first); "
        "a smaller one means a tape did not fully ingest."
    )


def test_recon_matches_expected(recon, expected):
    """Per hub-order final state equals the generator's independently computed oracle."""
    problems = _compare_expected(expected, _by_key(recon))
    assert not problems, (
        f"{len(problems)} orders_recon mismatch(es):\n  " + "\n  ".join(problems[:40])
    )


# --------------------------------------------------------------------------
# 4. family-level break taxonomy
# --------------------------------------------------------------------------
def test_clean_families_absent_from_breaks(session, recon, expected):
    """clean_fill / working_fanout / late_parent never reach ``oms_breaks``.

    ``late_parent`` is the interesting one: its OMS-B-parent tape is published after
    every child message, so the family dangles transiently and must have healed by
    the time ingestion settles (doc 09 s5.3 -- a late parent heals with no replay).
    """
    breaks = poll_table(session, "oms_breaks", min_rows=0)
    broken_keys = {_norm(v) for v in breaks.get("GlobalKey", [])}

    problems = []
    for scenario in CLEAN_SCENARIOS:
        rows = _scenario_rows(expected, scenario)
        if not rows:
            problems.append(f"{scenario}: no family in this run (seed/orders too small?)")
            continue
        for row in rows:
            gk = _norm(row.get("GlobalKey"))
            if gk in broken_keys:
                problems.append(f"{scenario}: {gk} appears in oms_breaks")
            if _norm(row.get("BreakKind")) != "NONE":
                problems.append(
                    f"{scenario}: generator itself expected BreakKind "
                    f"{row.get('BreakKind')!r} on {gk} -- catalog drift"
                )
    assert not problems, "clean families are not clean:\n  " + "\n  ".join(problems)


def test_missed_fill_breaks_on_a_and_b_parent(session, recon, expected):
    """``missed_fill`` shows ``QTY_BREAK`` on exactly the OMS-A and OMS-B-parent rows."""
    rows = _scenario_rows(expected, "missed_fill")
    if not rows:
        pytest.skip("no missed_fill family in this run")

    breaks = poll_table(session, "oms_breaks", min_rows=1)
    broken_keys = {_norm(v) for v in breaks.get("GlobalKey", [])}
    actual = _by_key(recon)

    problems = []
    for root, family in _families(rows).items():
        want = {_norm(r["GlobalKey"]) for r in family if _norm(r["Oms"]) in (HUB_A, HUB_BP)}
        got = {
            _norm(r["GlobalKey"])
            for r in family
            if _norm(actual.get(_norm(r["GlobalKey"]), {}).get("BreakKind")) == "QTY_BREAK"
        }
        if want != got:
            problems.append(
                f"family {root}: QTY_BREAK on {sorted(got)}, expected exactly {sorted(want)}"
            )
        for gk in want:
            if gk not in broken_keys:
                problems.append(f"family {root}: {gk} is a QTY_BREAK but is absent from oms_breaks")
        # The B-parent tape stops one execution report short, so it must end short too.
        for row in family:
            if _norm(row["Oms"]) != HUB_BP:
                continue
            got_status = _norm(actual.get(_norm(row["GlobalKey"]), {}).get("OrdStatus"))
            if got_status != "PARTIALLY_FILLED":
                problems.append(
                    f"family {root}: OMS-B-parent OrdStatus {got_status!r}, expected "
                    "'PARTIALLY_FILLED' (the tape omits its final execution report)"
                )
    assert not problems, "missed_fill taxonomy wrong:\n  " + "\n  ".join(problems)


def test_dangling_child_is_dangling(session, recon, expected):
    """``dangling_child`` leaves exactly one unresolvable OMS-C order per family."""
    rows = _scenario_rows(expected, "dangling_child")
    if not rows:
        pytest.skip("no dangling_child family in this run")

    breaks = poll_table(session, "oms_breaks", min_rows=1)
    broken_keys = {_norm(v) for v in breaks.get("GlobalKey", [])}
    actual = _by_key(recon)

    dangling = [r for r in rows if _norm(r.get("LinkState")) == "DANGLING"]
    assert dangling, (
        "the generator's dangling_child families contain no DANGLING order -- "
        "expected one OMS-C order whose 16668 names an id no tape defines"
    )

    problems = []
    for row in dangling:
        gk = _norm(row["GlobalKey"])
        got = actual.get(gk)
        if got is None:
            problems.append(f"{gk}: absent from orders_recon")
            continue
        if _norm(got.get("LinkState")) != "DANGLING":
            problems.append(f"{gk}: LinkState {got.get('LinkState')!r}, expected 'DANGLING'")
        if _norm(got.get("BreakKind")) != "DANGLING":
            problems.append(f"{gk}: BreakKind {got.get('BreakKind')!r}, expected 'DANGLING'")
        if _norm(got.get("Oms")) != HUB_C:
            problems.append(f"{gk}: on hub {got.get('Oms')!r}, expected {HUB_C!r}")
        if gk not in broken_keys:
            problems.append(f"{gk}: DANGLING but absent from oms_breaks")
        # A DANGLING order is its own root until its parent shows up (doc 09 s5.3).
        if _norm(got.get("RootKey")) != gk:
            problems.append(f"{gk}: RootKey {got.get('RootKey')!r}, expected to be its own root")
    assert not problems, "dangling_child taxonomy wrong:\n  " + "\n  ".join(problems)


def test_partial_route_is_amber_not_red(session, recon, expected):
    """``partial_route`` is ``UNROUTED`` at OMS-B-parent: amber, excluded from ``oms_breaks``."""
    rows = _scenario_rows(expected, "partial_route")
    if not rows:
        pytest.skip("no partial_route family in this run")

    breaks = poll_table(session, "oms_breaks", min_rows=0)
    broken_keys = {_norm(v) for v in breaks.get("GlobalKey", [])}
    actual = _by_key(recon)

    problems = []
    unrouted_keys = set()
    for root, family in _families(rows).items():
        parents = [r for r in family if _norm(r["Oms"]) == HUB_BP]
        if not parents:
            problems.append(f"family {root}: no OMS-B-parent row in the expected export")
            continue
        for row in family:
            gk = _norm(row["GlobalKey"])
            got = actual.get(gk)
            if got is None:
                problems.append(f"{gk}: absent from orders_recon")
                continue
            kind = _norm(got.get("BreakKind"))
            if _norm(row["Oms"]) == HUB_BP:
                if kind != "UNROUTED":
                    problems.append(f"{gk}: BreakKind {kind!r}, expected 'UNROUTED' at OMS-B-parent")
                else:
                    unrouted_keys.add(gk)
            if gk in broken_keys:
                problems.append(
                    f"{gk}: partial_route rows must stay out of oms_breaks "
                    f"(BreakKind {kind!r}); only red kinds belong there"
                )
    assert not problems, "partial_route taxonomy wrong:\n  " + "\n  ".join(problems)

    # ...but breaks_only() is the wider "anything not clean" view and must include them.
    session.run_script("e2e_breaks_only = breaks_only()")
    wide = read_table(session, "e2e_breaks_only")
    wide_keys = {_norm(v) for v in wide.get("GlobalKey", [])}
    missed = sorted(unrouted_keys - wide_keys)
    assert not missed, (
        f"breaks_only() omitted UNROUTED rows {missed} -- doc 09 s7 makes it wider "
        "than oms_breaks, not equal to it"
    )


# --------------------------------------------------------------------------
# 5. the per-edge math itself
# --------------------------------------------------------------------------
def test_clean_family_edge_math(recon, expected):
    """Spot-check one clean family: parents roll up to zero, leaves have no children."""
    clean = _families(_scenario_rows(expected, "clean_fill"))
    if not clean:
        pytest.skip("no clean_fill family in this run")

    root, family = sorted(clean.items())[0]
    actual = _by_key(recon)
    problems = []

    for row in family:
        gk = _norm(row["GlobalKey"])
        got = actual.get(gk)
        if got is None:
            problems.append(f"{gk}: absent from orders_recon")
            continue
        has_children = _tri(got.get("HasChildren"))
        is_leaf = _norm(row["Oms"]) == HUB_C
        if is_leaf:
            # A nullable column would arrive as None; `!isNull(ChildCount)` produces a
            # primitive false. Both mean "no children" -- accept either.
            if has_children not in (False, None):
                problems.append(f"{gk}: OMS-C leaf has HasChildren={has_children!r}")
            continue
        if has_children is not True:
            problems.append(
                f"{gk}: HasChildren={has_children!r} -- every hop above OMS-C in a "
                "clean family routes to at least one child"
            )
            continue
        for col, tol in (
            ("DeltaCumQty", TOL),
            ("DeltaLeavesQty", TOL),
            ("DeltaNotional", NOTIONAL_TOL),
        ):
            if abs(_num(got.get(col))) > tol:
                problems.append(
                    f"{gk}: {col}={got.get(col)!r} exceeds tolerance {tol} "
                    f"(own CumQty={got.get('CumQty')} vs ChildCumQty={got.get('ChildCumQty')})"
                )
        if _norm(got.get("BreakKind")) != "NONE":
            problems.append(f"{gk}: BreakKind {got.get('BreakKind')!r} on a clean edge")

    assert not problems, (
        f"per-edge math wrong on clean family {root}:\n  " + "\n  ".join(problems)
    )


# --------------------------------------------------------------------------
# 6. find_chain works from either end
# --------------------------------------------------------------------------
def test_find_chain_from_both_ends(session, recon, expected):
    """Selecting an OMS-C hop and its OMS-A ancestor return the identical family."""
    clean = _families(_scenario_rows(expected, "clean_fill"))
    if not clean:
        pytest.skip("no clean_fill family in this run")

    root, family = sorted(clean.items())[0]
    leaves = sorted(_norm(r["ClOrdID"]) for r in family if _norm(r["Oms"]) == HUB_C)
    roots = sorted(_norm(r["ClOrdID"]) for r in family if _norm(r["Oms"]) == HUB_A)
    assert leaves and roots, f"family {root} has no OMS-C leaf or no OMS-A root: {family}"

    want_keys = {_norm(r["GlobalKey"]) for r in family}
    assert len(want_keys) >= 4, f"family {root} has only {len(want_keys)} hops: {sorted(want_keys)}"

    results = {}
    for label, any_id in (("downstream", leaves[0]), ("upstream", roots[0])):
        session.run_script(f'e2e_chain_{label} = find_chain("{any_id}")')
        df = read_table(session, f"e2e_chain_{label}")
        got_keys = {_norm(v) for v in df.get("GlobalKey", [])}
        root_keys = {_norm(v) for v in df.get("RootKey", [])}
        assert len(df) >= 4, (
            f"find_chain({any_id!r}) returned {len(df)} rows, expected the whole "
            f">=4-hop family {sorted(want_keys)}"
        )
        assert root_keys == {root}, (
            f"find_chain({any_id!r}) mixed families: RootKeys {sorted(root_keys)}, expected {[root]}"
        )
        missing = sorted(want_keys - got_keys)
        assert not missing, f"find_chain({any_id!r}) missed family members {missing}"
        results[label] = got_keys

    assert results["downstream"] == results["upstream"], (
        "find_chain is not symmetric: from the OMS-C leaf it returned "
        f"{sorted(results['downstream'])}, from the OMS-A root {sorted(results['upstream'])}"
    )


# --------------------------------------------------------------------------
# 7. break_summary
# --------------------------------------------------------------------------
def test_break_summary_counts(session, recon, expected):
    """``break_summary`` counts every non-``NONE`` row by (``Oms``, ``BreakKind``)."""
    want = collections.Counter(
        (_norm(e["Oms"]), _norm(e["BreakKind"]))
        for e in expected
        if _norm(e.get("BreakKind")) != "NONE"
    )
    df = poll_table(session, "break_summary", min_rows=len(want))
    count_col = "Count" if "Count" in df.columns else df.columns[-1]
    got = collections.Counter()
    for _, row in df.iterrows():
        got[(_norm(row["Oms"]), _norm(row["BreakKind"]))] += int(row[count_col])

    assert got == want, (
        "break_summary disagrees with the generator's expected export\n"
        f"  expected: {sorted(want.items())}\n"
        f"  actual  : {sorted(got.items())}\n"
        f"  only-expected: {sorted((want - got).items())}\n"
        f"  only-actual  : {sorted((got - want).items())}"
    )


# --------------------------------------------------------------------------
# 8. restart resilience -- runs last: it tears down the shared session
# --------------------------------------------------------------------------
def test_restart_rebuilds_identical_recon(expected):
    """Restarting Deephaven rebuilds the whole recon from the Kafka journal.

    Four topics are four journals; ``ALL_PARTITIONS_SEEK_TO_BEGINNING`` plus the
    idempotent per-hub fold means every hub-order, its link, its root and its
    ``BreakKind`` are a pure function of what was published (doc 03 s3.3, doc 09 s4).
    So the assertion is the *same* per-order comparison as before the restart, not a
    row count.
    """
    pytest.importorskip("pydeephaven")
    if not _port_open(DH_HOST, DH_PORT):
        pytest.skip(f"Deephaven not reachable at {DH_HOST}:{DH_PORT}")

    container = _resolve_container()
    if container is None:
        pytest.skip(f"cannot locate the Deephaven container via `{CONTAINER_CLI} ps`")

    restart = subprocess.run(
        [CONTAINER_CLI, "restart", container], capture_output=True, text=True, timeout=300
    )
    assert restart.returncode == 0, (
        f"`{CONTAINER_CLI} restart {container}` failed: {restart.stderr}"
    )

    after = _wait_for_session(RESTART_TIMEOUT)
    assert after is not None, (
        f"Deephaven did not accept connections within {RESTART_TIMEOUT}s of restart; "
        f"see `{CONTAINER_CLI} logs {container}`"
    )
    try:
        poll_table(after, "oms_orders_latest", min_rows=len(expected))
        df = poll_table(after, "orders_recon", min_rows=len(expected))
        problems = _compare_expected(expected, _by_key(df))
    finally:
        try:
            after.close()
        except Exception:  # noqa: BLE001
            pass

    assert not problems, (
        f"{len(problems)} mismatch(es) after restart -- replay is not idempotent:\n  "
        + "\n  ".join(problems[:40])
    )
