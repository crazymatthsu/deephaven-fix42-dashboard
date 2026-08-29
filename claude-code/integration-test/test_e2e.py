"""End-to-end test: generator -> Kafka -> Deephaven, asserted through pydeephaven.

Run it through ``run_integration.sh`` (brings the stack up, runs the generator,
then invokes pytest). Running ``pytest`` directly works too, provided the stack is
already up and ``.out/expected*.json`` exists -- otherwise every test skips with a
message saying what is missing, so this file is safe in CI.

What is asserted (doc 05 s6):
  1. every documented global from doc 03 exists on the server;
  2. per-chain final OrdStatus / CumQty / LeavesQty / ClOrdID match the generator's
     ``--emit-expected`` output;
  3. ``executions_latest`` carries the post-bust/DK disposition (FillStatus);
  4. ``clordid_index`` resolves an amended chain's ROOT ClOrdID and its final
     ClOrdID to the same OrderKey;
  5. ``status_summary`` counts are internally consistent with the cache;
  6. restarting the Deephaven container reproduces an identical cache
     (replay idempotence -- doc 03 s3.3).

Rerun semantics: replay idempotence (doc 01 s3 / doc 03 s3.3) is about re-*reading* the
journal -- the same records rebuild the same cache, which is what test 6 above asserts.
It does not make a second *publish* a no-op: the generator restarts its venue-side
counters at ORD-0001 on every invocation, so publishing onto a topic that still holds an
earlier batch folds both batches into one chain and no expected file describes the
result. ``run_integration.sh`` empties the topic before generating for exactly that
reason. Assertions here still match on **expected keys** rather than total row counts, so
a chain that reached the topic some other way is tolerated rather than fatal.
"""

from __future__ import annotations

import glob
import json
import os
import socket
import subprocess
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))

DH_HOST = os.environ.get("DH_HOST", "localhost")
DH_PORT = int(os.environ.get("DH_PORT", "10000"))
DH_CONTAINER = os.environ.get("DH_CONTAINER", "fix42-deephaven")
OUT_DIR = os.environ.get("IT_OUT_DIR", os.path.join(HERE, ".out"))
CONTAINER_CLI = os.environ.get("CONTAINER_CLI", "podman")

POLL_TIMEOUT = float(os.environ.get("IT_POLL_TIMEOUT", "90"))
POLL_INTERVAL = float(os.environ.get("IT_POLL_INTERVAL", "2"))
STABLE_POLLS = 3
RESTART_TIMEOUT = float(os.environ.get("IT_RESTART_TIMEOUT", "180"))

# Globals the app-mode script must expose (doc 03 s2.4 / doc 05 s4).
REQUIRED_TABLES = [
    "order_state_latest",
    "executions",
    "executions_latest",
    "order_events",
    "fix_messages",
    "clordid_index",
    "execid_index",
    "status_summary",
    "symbol_summary",
    "open_orders",
    "account_list",
]

LEGAL_FILL_STATUS = {"NORMAL", "BUSTED", "CORRECTED", "DK"}


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
            except Exception:
                pass
        time.sleep(POLL_INTERVAL)
    return None


@pytest.fixture(scope="module")
def session():
    pytest.importorskip("pydeephaven", reason="pip install -r requirements.txt")
    if not _port_open(DH_HOST, DH_PORT):
        pytest.skip(
            f"Deephaven not reachable at {DH_HOST}:{DH_PORT} -- "
            "start the stack with ./run_integration.sh (or KEEP_STACK=1 leftovers)"
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
    """Read `name` until its row count is unchanged across `stable` consecutive polls.

    The pipeline is streaming, so a single read can catch a partially-ingested
    frontier. Settling on a stable count is the cheap, deterministic way to know
    ingestion has caught up.
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


# --------------------------------------------------------------------------
# expected-data helpers
# --------------------------------------------------------------------------
def _unwrap(payload):
    """Accept a bare list or a {orders|expected|chains: [...]} wrapper."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("orders", "expected", "chains", "results"):
            if isinstance(payload.get(key), list):
                return payload[key]
    return []


@pytest.fixture(scope="module")
def expected():
    """Chains the generator says it produced (--emit-expected)."""
    files = sorted(glob.glob(os.path.join(OUT_DIR, "expected*.json")))
    if not files:
        pytest.skip(
            f"no expected*.json under {OUT_DIR} -- run ./run_integration.sh so the "
            "generator emits its seed-42 expectations"
        )
    rows = []
    for path in files:
        with open(path, "r", encoding="utf-8") as fh:
            rows.extend(_unwrap(json.load(fh)))
    if not rows:
        pytest.skip(f"expected*.json under {OUT_DIR} contained no chains")
    return rows


def _chain_id(row: dict) -> str:
    """Venue OrderID for a chain, falling back to ChainKey (doc 05 s2.1: they match)."""
    for key in ("OrderID", "ChainKey"):
        val = row.get(key)
        if val not in (None, ""):
            return str(val)
    raise AssertionError(f"expected row has neither OrderID nor ChainKey: {row}")


def _norm(value) -> str:
    return "" if value is None else str(value).strip()


def _num(value) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def _by_order_id(df):
    assert "OrderID" in df.columns, f"order_state_latest missing OrderID column; has {list(df.columns)}"
    return {_norm(r["OrderID"]): r for _, r in df.iterrows() if _norm(r["OrderID"])}


def _scenario_chains(expected, scenario):
    """Chains the generator tagged with a catalog name (doc 05 s2.2).

    ``--emit-expected`` records the scenario that produced each chain, which is what
    makes the bust/DK/amend assertions below deterministic instead of a guess about
    what the weighted 'all' mix happened to draw.
    """
    return [e for e in expected if _norm(e.get("Scenario")) == scenario]


def _order_key_for(state_df, order_id):
    """Map a venue OrderID to the chain's OrderKey (they differ -- doc 01 s3)."""
    for _, row in state_df.iterrows():
        if _norm(row.get("OrderID")) == order_id:
            return _norm(row.get("OrderKey"))
    return None


# --------------------------------------------------------------------------
# tests
# --------------------------------------------------------------------------
def test_required_globals_exist(session):
    """Every DAG node doc 03 promises is published as a server global."""
    try:
        available = set(session.tables)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"server does not support table listing: {exc}")
    missing = [t for t in REQUIRED_TABLES if t not in available]
    assert not missing, (
        f"app mode did not publish: {missing}\n"
        f"present: {sorted(available)}\n"
        f"check `{CONTAINER_CLI} logs {DH_CONTAINER}` for [fix42-loader] errors"
    )


def test_order_state_matches_expected(session, expected):
    """Per-chain end state equals what the seeded generator scripted."""
    df = poll_table(session, "order_state_latest", min_rows=len(expected))
    actual = _by_order_id(df)

    missing = [_chain_id(e) for e in expected if _chain_id(e) not in actual]
    assert not missing, (
        f"{len(missing)} generated chain(s) absent from order_state_latest: {missing[:10]}\n"
        f"cache holds {len(actual)} chains"
    )

    problems = []
    for exp in expected:
        oid = _chain_id(exp)
        row = actual[oid]
        if _norm(exp.get("OrdStatus")) != _norm(row.get("OrdStatus")):
            problems.append(f"{oid}: OrdStatus expected {exp.get('OrdStatus')!r} got {row.get('OrdStatus')!r}")
        if _num(exp.get("CumQty")) != pytest.approx(_num(row.get("CumQty"))):
            problems.append(f"{oid}: CumQty expected {exp.get('CumQty')} got {row.get('CumQty')}")
        if _num(exp.get("LeavesQty")) != pytest.approx(_num(row.get("LeavesQty"))):
            problems.append(f"{oid}: LeavesQty expected {exp.get('LeavesQty')} got {row.get('LeavesQty')}")
        if _norm(exp.get("ClOrdID")) and _norm(exp.get("ClOrdID")) != _norm(row.get("ClOrdID")):
            problems.append(f"{oid}: ClOrdID expected {exp.get('ClOrdID')!r} got {row.get('ClOrdID')!r}")
    assert not problems, "final order state mismatches:\n  " + "\n  ".join(problems)


def test_executions_latest_dispositions(session, expected):
    """last_by(ExecID) reflects post-bust / post-correct / post-DK truth (doc 01 s6)."""
    df = poll_table(session, "executions_latest", min_rows=1)
    assert "FillStatus" in df.columns, f"executions_latest missing FillStatus; has {list(df.columns)}"

    seen = {_norm(v) for v in df["FillStatus"] if _norm(v)}
    illegal = seen - LEGAL_FILL_STATUS
    assert not illegal, f"illegal FillStatus values {illegal}; legal set is {LEGAL_FILL_STATUS}"

    assert "OrderKey" in df.columns, f"executions_latest missing OrderKey; has {list(df.columns)}"
    state = poll_table(session, "order_state_latest", min_rows=len(expected))

    for status, scenario in (("BUSTED", "fill_bust"), ("DK", "dk_trade")):
        chains = _scenario_chains(expected, scenario)

        if not chains:
            # No Scenario tag (older generator) or the weighted 'all' mix skipped it:
            # fall back to asserting the disposition exists and points somewhere real.
            rows = df[df["FillStatus"].map(_norm) == status]
            if rows.empty:
                pytest.skip(
                    f"no '{scenario}' chain in this run and no {status} execution present "
                    f"(FillStatus values seen: {sorted(seen)}). Reproduce deterministically with: "
                    f'./gradlew :fix-mock-generator:run --args="--scenario {scenario} --orders 1"'
                )
            orphans = {_norm(k) for k in rows["OrderKey"] if _norm(k)} - {
                _norm(k) for k in state["OrderKey"]
            }
            assert not orphans, f"{status} executions reference unknown OrderKeys {orphans}"
            continue

        problems = []
        for chain in chains:
            oid = _chain_id(chain)
            key = _order_key_for(state, oid)
            if not key:
                problems.append(f"{scenario} chain {oid} is missing from order_state_latest")
                continue
            chain_execs = df[df["OrderKey"].map(_norm) == key]
            statuses = sorted({_norm(v) for v in chain_execs["FillStatus"] if _norm(v)})
            if status not in statuses:
                problems.append(
                    f"{scenario} chain {oid} (OrderKey {key}): expected a {status} execution, "
                    f"got {statuses or 'no executions at all'}"
                )
        assert not problems, (
            f"executions_latest does not reflect {scenario} dispositions:\n  " + "\n  ".join(problems)
        )


def test_clordid_index_resolves_amend_chain(session, expected):
    """An amended chain's root and final ClOrdID both resolve to one OrderKey (doc 01 s7.2)."""
    state = poll_table(session, "order_state_latest", min_rows=len(expected))
    for col in ("RootClOrdID", "ClOrdID", "OrderKey"):
        assert col in state.columns, f"order_state_latest missing {col}; has {list(state.columns)}"

    amend_chains = _scenario_chains(expected, "amend_ack")
    if amend_chains:
        amended, unrotated = [], []
        for chain in amend_chains:
            oid = _chain_id(chain)
            match = [r for _, r in state.iterrows() if _norm(r.get("OrderID")) == oid]
            assert match, f"amend_ack chain {oid} missing from order_state_latest"
            row = match[0]
            # An accepted replace (150=5) rotates tag 11, so root and current must differ.
            if _norm(row["RootClOrdID"]) == _norm(row["ClOrdID"]):
                unrotated.append(f"{oid}: ClOrdID never rotated (root == current == {row['ClOrdID']!r})")
            amended.append(row)
        assert not unrotated, "amend_ack chains did not rotate ClOrdID:\n  " + "\n  ".join(unrotated)
    else:
        amended = [
            r
            for _, r in state.iterrows()
            if _norm(r["RootClOrdID"])
            and _norm(r["ClOrdID"])
            and _norm(r["RootClOrdID"]) != _norm(r["ClOrdID"])
        ]
        if not amended:
            pytest.skip(
                "no chain in this run rotated its ClOrdID (no accepted amend). Reproduce with: "
                './gradlew :fix-mock-generator:run --args="--scenario amend_ack --orders 1"'
            )

    idx = poll_table(session, "clordid_index", min_rows=1)
    for col in ("ClOrdID", "OrderKey"):
        assert col in idx.columns, f"clordid_index missing {col}; has {list(idx.columns)}"
    mapping = {_norm(r["ClOrdID"]): _norm(r["OrderKey"]) for _, r in idx.iterrows()}

    problems = []
    for row in amended:
        root, final, key = _norm(row["RootClOrdID"]), _norm(row["ClOrdID"]), _norm(row["OrderKey"])
        if mapping.get(root) != key:
            problems.append(f"root ClOrdID {root!r} -> {mapping.get(root)!r}, expected OrderKey {key!r}")
        if mapping.get(final) != key:
            problems.append(f"final ClOrdID {final!r} -> {mapping.get(final)!r}, expected OrderKey {key!r}")
    assert not problems, "clordid_index did not resolve amend chains:\n  " + "\n  ".join(problems)


def test_status_summary_consistent(session, expected):
    """status_summary partitions the cache: its counts sum to the number of orders."""
    state = poll_table(session, "order_state_latest", min_rows=len(expected))
    summary = poll_table(session, "status_summary", min_rows=1)

    count_col = "Count" if "Count" in summary.columns else summary.columns[-1]
    total = int(summary[count_col].sum())

    assert total == len(state), (
        f"status_summary counts sum to {total} but order_state_latest holds {len(state)} rows"
    )
    # >= (not ==) on purpose: reruns without `down -v` replay onto the same keys,
    # but a *different* --seed/--orders would add chains. Keys are what matter.
    assert total >= len(expected), (
        f"cache holds {total} orders, fewer than the {len(expected)} chains the generator emitted"
    )


def _resolve_container() -> str | None:
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


def test_restart_replay_idempotence(expected):
    """Restarting Deephaven rebuilds a byte-identical cache from the Kafka journal.

    This is the payoff of doc 03 s3.3: seek-to-beginning + idempotent id binding +
    ExecID dedupe means the cache is a pure function of the topic. Runs last: it
    tears down the session the other tests share.
    """
    pytest.importorskip("pydeephaven")
    if not _port_open(DH_HOST, DH_PORT):
        pytest.skip(f"Deephaven not reachable at {DH_HOST}:{DH_PORT}")

    container = _resolve_container()
    if container is None:
        pytest.skip(f"cannot locate the Deephaven container via `{CONTAINER_CLI} ps`")

    sort_cols = ["OrderKey", "OrdStatus", "CumQty"]

    def snapshot(sess):
        df = poll_table(sess, "order_state_latest", min_rows=len(expected))
        return df[sort_cols].sort_values(sort_cols).reset_index(drop=True)

    before_session = _new_session()
    try:
        before = snapshot(before_session)
    finally:
        try:
            before_session.close()
        except Exception:  # noqa: BLE001
            pass

    restart = subprocess.run(
        [CONTAINER_CLI, "restart", container], capture_output=True, text=True, timeout=180
    )
    assert restart.returncode == 0, f"`{CONTAINER_CLI} restart {container}` failed: {restart.stderr}"

    after_session = _wait_for_session(RESTART_TIMEOUT)
    assert after_session is not None, (
        f"Deephaven did not accept connections within {RESTART_TIMEOUT}s of restart; "
        f"see `{CONTAINER_CLI} logs {container}`"
    )
    try:
        after = snapshot(after_session)
    finally:
        try:
            after_session.close()
        except Exception:  # noqa: BLE001
            pass

    assert len(before) == len(after), (
        f"cache size changed across restart: {len(before)} -> {len(after)} rows"
    )
    diff = before.compare(after) if before.shape == after.shape else None
    assert before.equals(after), (
        "cache differed after restart -- replay is not idempotent\n"
        f"{diff if diff is not None else f'before={before.to_dict()} after={after.to_dict()}'}"
    )
