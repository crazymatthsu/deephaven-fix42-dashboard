#!/usr/bin/env bash
#
# End-to-end demo test: bring up Kafka + Deephaven, publish a seeded FIX 4.2
# order flow with the Java generator, then assert the Deephaven cache through
# pydeephaven (doc 05 s6).
#
#   ./run_integration.sh                 # up -> reset topic -> generate -> pytest -> down -v
#   KEEP_STACK=1 ./run_integration.sh    # leave the stack running afterwards
#
# Knobs (all optional):
#   SEED=42 ORDERS=12 RATE=200           generator shape
#   BOOTSTRAP=localhost:19092            Kafka external listener (host side)
#   DH_URL=http://localhost:10000        Deephaven web/gRPC endpoint
#   RESET_TOPIC=0                        do not empty the topic; refuse to run if it
#                                        is dirty (see section 3)
#   IT_TARGETED_SCENARIOS=1              also publish one chain per named scenario
#                                        (see the warning where it is used)
#   PYTEST_ARGS="-k restart"             extra pytest flags
#
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
COMPOSE_FILE="$ROOT/docker/docker-compose.yml"
OUT="$HERE/.out"
VENV="$HERE/.venv-it"

SEED="${SEED:-42}"
ORDERS="${ORDERS:-12}"
RATE="${RATE:-200}"
TOPIC="${TOPIC:-fix42.messages}"
BOOTSTRAP="${BOOTSTRAP:-localhost:19092}"
DH_URL="${DH_URL:-http://localhost:10000}"
KEEP_STACK="${KEEP_STACK:-0}"
IT_TARGETED_SCENARIOS="${IT_TARGETED_SCENARIOS:-0}"
PYTEST_ARGS="${PYTEST_ARGS:-}"
STACK_TIMEOUT="${STACK_TIMEOUT:-240}"
DH_CONTAINER_NAME="${DH_CONTAINER:-fix42-deephaven}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-fix42-kafka}"
RESET_TOPIC="${RESET_TOPIC:-1}"
# Must match the compose healthcheck's --partitions, which is what a fresh stack creates.
TOPIC_PARTITIONS="${TOPIC_PARTITIONS:-3}"
TOPIC_RESET_TIMEOUT="${TOPIC_RESET_TIMEOUT:-60}"
# Container id of the Deephaven server this run is driving, captured once it is up.
# `restart` preserves it; only a recreate by another process changes it.
DH_STACK_ID=""

# The compose stack runs one app out of docker/apps/, chosen by DH_APP. Pin it here so
# a DH_APP left over in the caller's shell cannot silently start a different app and
# make every assertion below fail on missing tables.
export DH_APP="${DH_APP:-fix42-dashboard}"

log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m[warn] %s\033[0m\n' "$*" >&2; }
die()  { printf '\033[1;31m[fail] %s\033[0m\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Container tooling: prefer `podman compose`, fall back to podman-compose,
#    then docker compose. (`podman compose` proxies to podman-compose anyway,
#    but going through podman keeps machine/connection settings consistent.)
# ---------------------------------------------------------------------------
COMPOSE_CMD=()
if command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(podman compose)
  CONTAINER_CLI=podman
elif command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD=(podman-compose)
  CONTAINER_CLI=podman
elif command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD=(docker compose)
  CONTAINER_CLI=docker
else
  die "no compose implementation found (tried: podman compose, podman-compose, docker compose)"
fi
export CONTAINER_CLI
log "compose command: ${COMPOSE_CMD[*]}  (cli: $CONTAINER_CLI)"

# On macOS podman needs its VM running before anything else works.
if [[ "$CONTAINER_CLI" == "podman" ]]; then
  if ! podman info >/dev/null 2>&1; then
    warn "podman is not responding; attempting 'podman machine start'"
    podman machine start || die "could not start the podman machine -- start Podman Desktop and retry"
    podman info >/dev/null 2>&1 || die "podman still not responding after 'podman machine start'"
  fi
fi

[[ -f "$COMPOSE_FILE" ]] || die "missing compose file: $COMPOSE_FILE"

# ---------------------------------------------------------------------------
# 2. Stack lifecycle
# ---------------------------------------------------------------------------
# The compose project name is shared machine-wide, so the stack this run started can be
# replaced by another process's while we are still running. Tearing down blindly on exit
# then reaps THEIR containers -- which is how this run's own server got reaped on
# 2026-08-29, propagating one suite's failure into another's. Only remove what we started.
stack_is_ours() {
  # Never got far enough to identify a stack: fall back to the old unconditional
  # behaviour and clean up whatever our own `up -d` may have left.
  [[ -n "$DH_STACK_ID" ]] || return 0
  local now
  now="$("$CONTAINER_CLI" container inspect --format '{{.Id}}' "$DH_CONTAINER_NAME" 2>/dev/null || true)"
  [[ "$now" == "$DH_STACK_ID" ]]
}

cleanup() {
  local rc=$?
  if [[ "$KEEP_STACK" == "1" ]]; then
    log "KEEP_STACK=1 -- leaving the stack up (IDE: $DH_URL/ide)"
    printf '    tear down later with: %s -f %s down -v\n' "${COMPOSE_CMD[*]}" "$COMPOSE_FILE"
  elif ! stack_is_ours; then
    warn "not tearing down: '$DH_CONTAINER_NAME' is no longer the container this run
    started, so the compose project belongs to another process now. Leaving it alone."
    printf '    if it is in fact stale: %s -f %s down -v\n' "${COMPOSE_CMD[*]}" "$COMPOSE_FILE"
  else
    log "tearing down the stack"
    "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
trap cleanup EXIT

# The java app (docker/apps/fix42-dashboard-java) is a jar mounted at /apps/libs, and the mount is
# read when the container starts -- so it has to be built BEFORE `up -d`, not alongside the
# generator further down.
if [[ "$DH_APP" == *java* ]]; then
  log "building the java deephaven app (mounted at /apps/libs)"
  ( cd "$ROOT" && ./gradlew --console=plain --quiet :deephaven-app-java:assemble ) \
    || die "java app build failed"
fi

# `up -d` returns once containers are created; wait for Deephaven to actually serve.
# depends_on: service_healthy already gates deephaven on a healthy Kafka (verified
# honored by podman-compose 1.6.0), so this loop only waits on the JVM + app mode.
wait_for_deephaven() {
  log "waiting for Deephaven at $DH_URL (timeout ${STACK_TIMEOUT}s)"
  local deadline code
  deadline=$(( $(date +%s) + STACK_TIMEOUT ))
  until code=$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$DH_URL" 2>/dev/null) && [[ "$code" != "000" ]]; do
    # A container that has gone away is never going to answer. Say so now instead of
    # burning the whole timeout and then blaming a slow JVM -- the usual cause is
    # another process running compose against the same project name on this machine.
    if ! "$CONTAINER_CLI" container inspect "$DH_CONTAINER_NAME" >/dev/null 2>&1; then
      die "the Deephaven container '$DH_CONTAINER_NAME' no longer exists -- something
    outside this script removed or recreated it (another compose run against the
    '$(basename "$COMPOSE_FILE")' project?). Leaving whatever is running alone;
    re-run when the stack is yours."
    fi
    if (( $(date +%s) > deadline )); then
      "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" logs --tail 60 deephaven || true
      die "Deephaven did not come up within ${STACK_TIMEOUT}s"
    fi
    sleep 3
  done
  echo "    Deephaven responding (HTTP $code)"
  # Capture once, not on every call: after a recreate by someone else the server still
  # answers, and re-capturing would adopt their container as ours and tear it down.
  [[ -n "$DH_STACK_ID" ]] || \
    DH_STACK_ID="$("$CONTAINER_CLI" container inspect --format '{{.Id}}' "$DH_CONTAINER_NAME" 2>/dev/null || true)"
}

# The Deephaven log is far larger than a pipe buffer, so `logs | grep -q` gets the
# producer killed by SIGPIPE (exit 141) the moment grep exits on its first match --
# and `set -o pipefail` then reports the *pipeline* as failed, i.e. "no match", which
# silently inverts the check below. Capture the log, then grep the captured string.
dh_log() { "$CONTAINER_CLI" logs "$DH_CONTAINER_NAME" 2>&1 || true; }
dh_log_has() { local logs; logs="$(dh_log)"; grep -qE "$1" <<<"$logs"; }
dh_log_show() { local logs; logs="$(dh_log)"; grep -E -A "${2:-12}" "$1" <<<"$logs" || true; }

# The app-mode loader reports its own failures; surface them early rather than
# letting the test fail with a confusing "table not found".
#
# _lib/loader.py prints "[<app>] ERROR"; the java shim raises through app mode instead,
# which the server logs as a Traceback. Match both, and only the app this run started.
#
# The label half is anchored at line start on purpose: app mode echoes the selected
# app's main.py into the log, and apps/fix42-dashboard-java/main.py hardcodes the
# EXPANDED literal "[fix42-dashboard-java] ERROR" inside a print(), so unanchored it
# would make every healthy java run warn about its own error handler. Real output
# starts the line; echoed source is indented. (_lib/loader.py is safe either way --
# it only ever carries "[{label}] ERROR" pre-format.) Traceback deliberately stays
# UNANCHORED: no mounted source contains that phrase, and the server may prefix the
# line, so anchoring it could silently miss a real java-app failure.
APP_ERROR_RE="^\[$DH_APP\] ERROR|Traceback \(most recent call last\)"

check_app_errors() {
  if dh_log_has "$APP_ERROR_RE"; then
    warn "the app-mode script reported an error:"
    dh_log_show "$APP_ERROR_RE" 12 >&2
    warn "continuing -- pytest will report precisely which globals are missing"
  fi
}

log "starting stack (kafka + deephaven)"
"${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" up -d

wait_for_deephaven
check_app_errors

# ---------------------------------------------------------------------------
# 3. Topic hygiene -- start every run from an empty journal.
#
# The generator restarts its venue-side counters (ORD-0001, EXEC-000001, ...) on
# every invocation and the consumer replays from offset 0, so messages left on the
# topic by an *earlier* run carry the same chain keys as this one. The state machine
# then folds both batches into one chain while --emit-expected describes only the
# new batch, and the run fails with mismatches that read like a state-machine bug:
#
#     ORD-0001: CumQty expected 500.0 got 3300.0
#     ORD-0003: OrdStatus expected 'FILLED' got 'PENDING_REPLACE'
#
# `up -d` on an already-running stack does not clear anything, so KEEP_STACK=1 (or
# any hand-started stack) hits this on the second run. Emptying the topic here makes
# the run hermetic whatever state the stack was in.
#
# Deephaven has to be restarted afterwards: kc.consume resolves the topic's
# partitions once at startup (same reason the compose healthcheck pre-creates the
# topic), and the folded cache lives in the running server -- a consumer still
# attached to the deleted topic would keep serving the previous run's rows.
# ---------------------------------------------------------------------------
kafka_exec() { "$CONTAINER_CLI" exec "$KAFKA_CONTAINER" "$@"; }

kafka_topics() {
  kafka_exec /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "$@"
}

# compose pins container_name, so the scan only matters for a hand-rolled stack.
resolve_kafka_container() {
  if "$CONTAINER_CLI" container inspect "$KAFKA_CONTAINER" >/dev/null 2>&1; then
    return 0
  fi
  local name
  for name in $("$CONTAINER_CLI" ps --format '{{.Names}}' 2>/dev/null); do
    [[ "$name" == *kafka* && "$name" != *kafka-ui* ]] || continue
    KAFKA_CONTAINER="$name"
    warn "using Kafka container '$KAFKA_CONTAINER' (override with KAFKA_CONTAINER=)"
    return 0
  done
  return 1
}

# Echoes "<partitions> <records>" for $TOPIC; "0 0" when it does not exist.
# kafka-get-offsets.sh prints one `topic:partition:endOffset` line per partition and
# exits 1 when nothing matches, so both numbers come out of the same call.
topic_stats() {
  local out
  if ! out=$(kafka_exec /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server localhost:9092 --topic "$TOPIC" 2>/dev/null); then
    echo "0 0"
    return 0
  fi
  awk -F: -v t="$TOPIC" \
    '$1 == t && $3 ~ /^[0-9]+$/ { p++; n += $3 } END { print p+0, n+0 }' <<<"$out"
}

reset_topic() {
  local had_partitions="$1" err deadline parts records last_err=""

  if (( had_partitions > 0 )); then
    log "emptying topic '$TOPIC' (delete + re-create)"
    if ! err=$(kafka_topics --delete --topic "$TOPIC" 2>&1); then
      die "could not delete topic '$TOPIC': $err
    tear the stack down instead: ${COMPOSE_CMD[*]} -f $COMPOSE_FILE down -v"
    fi
  else
    log "creating missing topic '$TOPIC'"
  fi

  # Deletion is asynchronous in KRaft, so poll for the shape we need rather than
  # for the tombstone. --if-not-exists also settles the race with the broker's own
  # auto-create (KAFKA_AUTO_CREATE_TOPICS_ENABLE=true): whichever wins, the topic
  # comes back with the same partition count and no records.
  deadline=$(( $(date +%s) + TOPIC_RESET_TIMEOUT ))
  while :; do
    if ! err=$(kafka_topics --create --if-not-exists --topic "$TOPIC" \
        --partitions "$TOPIC_PARTITIONS" --replication-factor 1 2>&1); then
      last_err="$err"
    fi
    read -r parts records <<<"$(topic_stats)"
    if [[ "$parts" == "$TOPIC_PARTITIONS" && "$records" == "0" ]]; then
      echo "    topic '$TOPIC' is empty ($parts partitions)"
      return 0
    fi
    if (( $(date +%s) > deadline )); then
      die "topic '$TOPIC' did not come back empty within ${TOPIC_RESET_TIMEOUT}s \
(partitions=$parts, records=$records)${last_err:+
    last error from kafka-topics.sh: $last_err}"
    fi
    sleep 2
  done
}

resolve_kafka_container \
  || die "cannot find the Kafka container (looked for '$KAFKA_CONTAINER' and any
    running container matching *kafka*). Set KAFKA_CONTAINER= to its name."

read -r TOPIC_PARTS TOPIC_RECORDS <<<"$(topic_stats)"
if [[ "$TOPIC_PARTS" == "$TOPIC_PARTITIONS" && "$TOPIC_RECORDS" == "0" ]]; then
  log "topic '$TOPIC' is already empty ($TOPIC_PARTS partitions) -- nothing to reset"
elif [[ "$RESET_TOPIC" != "1" ]]; then
  die "RESET_TOPIC=0 but topic '$TOPIC' is not a clean journal \
(partitions=$TOPIC_PARTS, records=$TOPIC_RECORDS; wanted $TOPIC_PARTITIONS and 0).
    The generator restarts its chain ids at ORD-0001 and the consumer replays from
    offset 0, so those messages would fold into this run's chains and the assertions
    would fail as if the state machine were broken. Do one of:
      unset RESET_TOPIC        (the default -- this script empties the topic for you)
      ${COMPOSE_CMD[*]} -f $COMPOSE_FILE down -v"
else
  reset_topic "$TOPIC_PARTS"
  log "restarting Deephaven so its consumer binds to the new topic"
  "$CONTAINER_CLI" restart "$DH_CONTAINER_NAME" >/dev/null \
    || die "\`$CONTAINER_CLI restart $DH_CONTAINER_NAME\` failed"
  wait_for_deephaven
  check_app_errors
fi

# ---------------------------------------------------------------------------
# 4. Python client venv
# ---------------------------------------------------------------------------
if [[ ! -x "$VENV/bin/python" ]]; then
  log "creating client venv at $VENV"
  python3 -m venv "$VENV" || die "python3 -m venv failed -- is python3 installed?"
fi
log "installing client requirements"
"$VENV/bin/pip" install --quiet --disable-pip-version-check -r "$HERE/requirements.txt" \
  || die "pip install failed"

# ---------------------------------------------------------------------------
# 5. Publish the seeded order flow
# ---------------------------------------------------------------------------
[[ -x "$ROOT/gradlew" ]] || die "missing $ROOT/gradlew -- build the Java modules first (see README)"

rm -rf "$OUT"
mkdir -p "$OUT"

run_generator() {
  local scenario="$1" orders="$2" seed="$3" outfile="$4"
  log "generator: scenario=$scenario orders=$orders seed=$seed -> $(basename "$outfile")"
  ( cd "$ROOT" && ./gradlew --console=plain --quiet :fix-mock-generator:run \
      --args="--bootstrap-servers $BOOTSTRAP --topic $TOPIC --seed $seed --orders $orders --rate $RATE --scenario $scenario --emit-expected $outfile" ) \
    || die "generator run failed (scenario=$scenario)"
  [[ -s "$outfile" ]] || die "generator did not write $outfile (is --emit-expected implemented?)"
}

run_generator all "$ORDERS" "$SEED" "$OUT/expected-all.json"

# Optional: force the rarer scenarios to appear. OFF by default because the
# generator restarts its venue-side counters (ORD-0001, ...) each invocation, so
# a second run can reuse chain keys from the first and merge into those chains --
# which would corrupt the expectations above. The section 3 reset does not help
# here: these collisions happen *within* one run, after the topic was emptied.
# Enable only if your generator derives its id counters from --seed.
if [[ "$IT_TARGETED_SCENARIOS" == "1" ]]; then
  warn "IT_TARGETED_SCENARIOS=1: extra runs may collide on chain ids -- see comment above"
  for scenario in fill_bust dk_trade amend_ack; do
    run_generator "$scenario" 1 "$((SEED + 1))" "$OUT/expected-$scenario.json"
  done
fi

# ---------------------------------------------------------------------------
# 6. Assert
# ---------------------------------------------------------------------------
log "running pytest"
cd "$HERE"
IT_OUT_DIR="$OUT" DH_CONTAINER="${DH_CONTAINER:-fix42-deephaven}" CONTAINER_CLI="$CONTAINER_CLI" \
  "$VENV/bin/python" -m pytest test_e2e.py -v ${PYTEST_ARGS:+$PYTEST_ARGS}

log "integration test passed"
