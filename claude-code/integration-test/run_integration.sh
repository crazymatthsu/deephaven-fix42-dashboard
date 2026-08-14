#!/usr/bin/env bash
#
# End-to-end demo test: bring up Kafka + Deephaven, publish a seeded FIX 4.2
# order flow with the Java generator, then assert the Deephaven cache through
# pydeephaven (doc 05 s6).
#
#   ./run_integration.sh                 # up -> generate -> pytest -> down -v
#   KEEP_STACK=1 ./run_integration.sh    # leave the stack running afterwards
#
# Knobs (all optional):
#   SEED=42 ORDERS=12 RATE=200           generator shape
#   BOOTSTRAP=localhost:19092            Kafka external listener (host side)
#   DH_URL=http://localhost:10000        Deephaven web/gRPC endpoint
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
cleanup() {
  local rc=$?
  if [[ "$KEEP_STACK" == "1" ]]; then
    log "KEEP_STACK=1 -- leaving the stack up (IDE: $DH_URL/ide)"
    printf '    tear down later with: %s -f %s down -v\n' "${COMPOSE_CMD[*]}" "$COMPOSE_FILE"
  else
    log "tearing down the stack"
    "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
trap cleanup EXIT

log "starting stack (kafka + deephaven)"
"${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" up -d

# `up -d` returns once containers are created; wait for Deephaven to actually serve.
# depends_on: service_healthy already gates deephaven on a healthy Kafka (verified
# honored by podman-compose 1.6.0), so this loop only waits on the JVM + app mode.
log "waiting for Deephaven at $DH_URL (timeout ${STACK_TIMEOUT}s)"
deadline=$(( $(date +%s) + STACK_TIMEOUT ))
until code=$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$DH_URL" 2>/dev/null) && [[ "$code" != "000" ]]; do
  if (( $(date +%s) > deadline )); then
    "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" logs --tail 60 deephaven || true
    die "Deephaven did not come up within ${STACK_TIMEOUT}s"
  fi
  sleep 3
done
echo "    Deephaven responding (HTTP $code)"

# The app-mode loader reports its own failures; surface them early rather than
# letting the test fail with a confusing "table not found".
if "$CONTAINER_CLI" logs fix42-deephaven 2>&1 | grep -q '\[fix42-loader\] ERROR'; then
  warn "the app-mode loader reported an error:"
  "$CONTAINER_CLI" logs fix42-deephaven 2>&1 | grep -A 12 '\[fix42-loader\] ERROR' >&2 || true
  warn "continuing -- pytest will report precisely which globals are missing"
fi

# ---------------------------------------------------------------------------
# 3. Python client venv
# ---------------------------------------------------------------------------
if [[ ! -x "$VENV/bin/python" ]]; then
  log "creating client venv at $VENV"
  python3 -m venv "$VENV" || die "python3 -m venv failed -- is python3 installed?"
fi
log "installing client requirements"
"$VENV/bin/pip" install --quiet --disable-pip-version-check -r "$HERE/requirements.txt" \
  || die "pip install failed"

# ---------------------------------------------------------------------------
# 4. Publish the seeded order flow
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
# which would corrupt the expectations above. Enable only if your generator
# derives its id counters from --seed.
if [[ "$IT_TARGETED_SCENARIOS" == "1" ]]; then
  warn "IT_TARGETED_SCENARIOS=1: extra runs may collide on chain ids -- see comment above"
  for scenario in fill_bust dk_trade amend_ack; do
    run_generator "$scenario" 1 "$((SEED + 1))" "$OUT/expected-$scenario.json"
  done
fi

# ---------------------------------------------------------------------------
# 5. Assert
# ---------------------------------------------------------------------------
log "running pytest"
cd "$HERE"
IT_OUT_DIR="$OUT" DH_CONTAINER="${DH_CONTAINER:-fix42-deephaven}" CONTAINER_CLI="$CONTAINER_CLI" \
  "$VENV/bin/python" -m pytest test_e2e.py -v ${PYTEST_ARGS:+$PYTEST_ARGS}

log "integration test passed"
