#!/usr/bin/env bash
#
# Order tree over AMPS -- the integration test (docs/14-order-tree-reconciliation.md s13).
#
# Brings up docker/docker-compose.order-tree-amps.yml -- AMPS (otr-amps, from your local
# AMPS image) and Deephaven (otr-amps-deephaven, the order-tree app with OTR_SOURCE=amps) --
# then runs test_order_tree_amps_e2e.py from a host venv. The test publishes native order
# rows to the three AMPS SOW topics, mutates them (fills, deletes, child-first arrival,
# a malformed message), restarts Deephaven, drops its connection and restarts AMPS, and
# after every step asserts the engine's otr_recon equals the python reference row for row.
#
#   bash order-tree-recon/e2e/run_e2e.sh                 # down -v -> up -> pytest -> down -v
#   KEEP_STACK=1 bash order-tree-recon/e2e/run_e2e.sh    # leave the stack (and the book) up
#   PYTEST_ARGS="-k 'mock_book or ladder'" bash ...      # a subset
#
# Knobs (all optional):
#   SEED=42 FAMILIES=24            the mock book the test publishes
#   DH_PORT=10020                  Deephaven host port
#   OTR_AMPS_PORT=39007            AMPS host port (also OTR_AMPS_WS_PORT, OTR_AMPS_ADMIN_PORT)
#   AMPS_IMAGE=<image>             your AMPS image (default localhost/amps-demo:5.3.5.135)
#   DH_XMX=1g                      Deephaven heap
#   STACK_TIMEOUT=300              seconds for the stack (and a restarted server) to come up
#   SETTLE_TIMEOUT=60              seconds for the engine to converge after a publish
#
# The stack has its own compose project (order-tree-amps), container names and ports, so it
# runs beside the mock demo (docker-compose.order-tree.yml) without touching it. It always
# starts from `down -v`: the SOW is persisted on a volume, and a previous run's book would
# otherwise be the first snapshot.
set -euo pipefail
# Artifact sources -- container registries, pip index, ... -- from claude-code/repos.env and
# your override file (docs/15). Loaded before anything pulls an image or installs a package.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/repos.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$MODULE_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT/docker/docker-compose.order-tree-amps.yml"
VENV="$HERE/.venv-e2e"

SEED="${SEED:-42}"
FAMILIES="${FAMILIES:-24}"
DH_PORT="${DH_PORT:-10020}"
OTR_AMPS_PORT="${OTR_AMPS_PORT:-39007}"
KEEP_STACK="${KEEP_STACK:-0}"
PYTEST_ARGS="${PYTEST_ARGS:-}"
STACK_TIMEOUT="${STACK_TIMEOUT:-300}"
SETTLE_TIMEOUT="${SETTLE_TIMEOUT:-60}"
DH_CONTAINER="${DH_CONTAINER:-otr-amps-deephaven}"
AMPS_CONTAINER="${OTR_AMPS_CONTAINER:-otr-amps}"
# compose reads these for the port mappings and container names
export DH_PORT OTR_AMPS_PORT DH_CONTAINER OTR_AMPS_CONTAINER="$AMPS_CONTAINER"

# podman ships outside the default PATH on the macOS box this was developed on.
case ":$PATH:" in
  *:/opt/podman/bin:*) ;;
  *) [[ -d /opt/podman/bin ]] && PATH="/opt/podman/bin:$PATH" ;;
esac
export PATH

log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m[warn] %s\033[0m\n' "$*" >&2; }
die()  { printf '\033[1;31m[fail] %s\033[0m\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Container tooling: podman compose, then podman-compose, then docker compose.
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

if [[ "$CONTAINER_CLI" == "podman" ]] && ! podman info >/dev/null 2>&1; then
  die "podman is not responding -- start the podman machine (podman machine start) and retry"
fi
[[ -f "$COMPOSE_FILE" ]] || die "missing compose file: $COMPOSE_FILE"

AMPS_IMAGE_IN_USE="${AMPS_IMAGE:-localhost/amps-demo:5.3.5.135}"
"$CONTAINER_CLI" image inspect "$AMPS_IMAGE_IN_USE" >/dev/null 2>&1 \
  || die "AMPS image $AMPS_IMAGE_IN_USE not found locally. AMPS has no public image: build or load yours and set AMPS_IMAGE=<image>."

compose() { "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" "$@"; }

# A Deephaven log is far larger than a pipe buffer, so `logs | grep -q` gets the producer
# killed by SIGPIPE the moment grep matches, and pipefail turns that into "no match".
# Capture the log, then grep the captured string.
dh_log()     { "$CONTAINER_CLI" logs "$1" 2>&1 || true; }
dh_log_has() { local logs; logs="$(dh_log "$1")"; grep -qE "$2" <<<"$logs"; }
current_cid() { "$CONTAINER_CLI" container inspect --format '{{.Id}}' "$1" 2>/dev/null || true; }

# ---------------------------------------------------------------------------
# 2. Stack lifecycle. The EXIT trap tears down only containers THIS run started: after an
#    outside recreate the project name points at someone else's stack.
# ---------------------------------------------------------------------------
ALL_CONTAINERS=("$AMPS_CONTAINER" "$DH_CONTAINER")
CAPTURED_CIDS=()

teardown_is_ours() {
  local i want now
  (( ${#CAPTURED_CIDS[@]} )) || return 0
  for i in "${!ALL_CONTAINERS[@]}"; do
    want="${CAPTURED_CIDS[$i]:-}"
    [[ -n "$want" ]] || continue
    now="$(current_cid "${ALL_CONTAINERS[$i]}")"
    [[ -z "$now" || "$now" == "$want" ]] || return 1
  done
  return 0
}

cleanup() {
  local rc=$?
  if [[ "$KEEP_STACK" == "1" ]]; then
    log "KEEP_STACK=1 -- leaving the stack up"
    printf '    dashboard : http://localhost:%s/iframe/widget/?name=order_tree_dashboard\n' "$DH_PORT"
    printf '    AMPS      : tcp://localhost:%s/amps/json  (admin http://localhost:%s)\n' "$OTR_AMPS_PORT" "${OTR_AMPS_ADMIN_PORT:-38085}"
    printf '    tear down : %s -f %s down -v\n' "${COMPOSE_CMD[*]}" "$COMPOSE_FILE"
  elif teardown_is_ours; then
    log "tearing down the stack"
    compose down -v >/dev/null 2>&1 || true
  else
    warn "skipping teardown: the order-tree-amps containers were recreated by another process"
  fi
  exit "$rc"
}
trap cleanup EXIT

log "clearing any previous stack and its volume (a stale SOW would be the first snapshot)"
compose down -v >/dev/null 2>&1 || true

# Built explicitly so a Dockerfile error is not mistaken for a slow boot. The image is the
# one the remote-URI stack uses too (same Dockerfile, same tag): a cached rebuild is quick.
log "building the Deephaven+AMPS-client image (first run pulls the base image)"
compose build deephaven || die "image build failed -- see docker/deephaven-amps.Dockerfile"

log "starting the stack (amps + deephaven)"
compose up -d
for name in "${ALL_CONTAINERS[@]}"; do
  CAPTURED_CIDS+=("$(current_cid "$name")")
done

APP_READY='\[order-tree\] exported:'
APP_FAILED='\[order-tree-recon\] ERROR'
log "waiting for the app in $DH_CONTAINER (timeout ${STACK_TIMEOUT}s)"
deadline=$(( $(date +%s) + STACK_TIMEOUT ))
until dh_log_has "$DH_CONTAINER" "$APP_READY|$APP_FAILED"; do
  [[ -n "$(current_cid "$DH_CONTAINER")" ]] || die "$DH_CONTAINER vanished while starting"
  if (( $(date +%s) > deadline )); then
    dh_log "$DH_CONTAINER" | tail -40 >&2
    die "the app did not export its tables within ${STACK_TIMEOUT}s"
  fi
  sleep 3
done
if dh_log_has "$DH_CONTAINER" "$APP_FAILED"; then
  dh_log "$DH_CONTAINER" | grep -E -A 15 "$APP_FAILED" >&2 || true
  die "the app failed to start (log above)"
fi
dh_log "$DH_CONTAINER" | grep '^\[order-tree\]' | grep -v 'exported:' || true

# ---------------------------------------------------------------------------
# 3. Client venv: pydeephaven (reads the tables) + the AMPS client (publishes).
# ---------------------------------------------------------------------------
if ! VENV_PY="$(venv_python "$VENV")"; then
  log "creating client venv at $VENV"
  HOST_PY="$(find_python3 3.10)" || die "no Python 3.10+ found -- install one or set PYTHON=/path/to/python"
  "$HOST_PY" -m venv "$VENV" || die "python -m venv failed ($HOST_PY)"
  VENV_PY="$(venv_python "$VENV")" || die "the venv at $VENV has no python"
fi
log "installing client requirements (pydeephaven 42.4, amps-python-client 5.3.5.7, pytest)"
"$VENV_PY" -m pip install --quiet --disable-pip-version-check -r "$HERE/requirements.txt" \
  || die "pip install failed"

# ---------------------------------------------------------------------------
# 4. Assert
# ---------------------------------------------------------------------------
log "running pytest"
cd "$HERE"
export OTRE2E_DH_PORT="$DH_PORT"
export OTRE2E_AMPS_URI="tcp://localhost:$OTR_AMPS_PORT/amps/json"
export OTRE2E_SEED="$SEED" OTRE2E_FAMILIES="$FAMILIES"
export OTRE2E_DH_CONTAINER="$DH_CONTAINER" OTRE2E_AMPS_CONTAINER="$AMPS_CONTAINER"
export OTRE2E_SETTLE_TIMEOUT="$SETTLE_TIMEOUT" OTRE2E_RESTART_TIMEOUT="$STACK_TIMEOUT"
# eval so PYTEST_ARGS keeps its quoting: PYTEST_ARGS="-k 'mock_book or ladder'"
eval "\"\$VENV_PY\" -m pytest test_order_tree_amps_e2e.py -v $PYTEST_ARGS"

log "order-tree AMPS integration test passed"
