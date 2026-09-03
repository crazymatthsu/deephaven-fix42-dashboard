#!/usr/bin/env bash
#
# Remote-URI multi-server end-to-end test (docs/10-deephaven-remote-uri.md s12).
#
# Brings up the whole fleet from docker/docker-compose.remote-uri.yml -- one AMPS
# broker, two Deephaven leaves (DH1 = OMS-A, DH2 = the other three hubs) and the
# collector -- publishes the four correlated FIX 4.2 drop-copy tapes to AMPS with the
# Java generator's --multi-oms --amps-uri mode, then asserts the leaves' exports, the
# collector's cross-server linking, the exposure lookup, the remote call and the
# restart recovery through pydeephaven.
#
#   bash run_e2e.sh                  # down -v -> build -> up -> generate -> pytest -> down -v
#   KEEP_STACK=1 bash run_e2e.sh     # leave the fleet running afterwards
#
# Knobs (all optional):
#   SEED=42 ORDERS=12 CHILDREN=3 RATE=200      generator shape
#   AMPS_URI=tcp://localhost:29007/amps/fix    where the generator publishes (host side)
#   COLLECTOR_URL=http://localhost:10010       the collector's web/gRPC endpoint
#   DH1_URL=http://localhost:10011             leaf endpoints
#   DH2_URL=http://localhost:10012
#   STACK_TIMEOUT=420                          seconds to wait for the fleet (image build
#                                              plus three JVMs on a cold machine)
#   BANNER_TIMEOUT=240                         seconds to wait for the three app banners
#   PYTEST_ARGS="-k restart"                   extra pytest flags
#   AMPS_IMAGE=<image>                         override the AMPS broker image
#   DH_XMX_LEAF / DH_XMX_COLLECTOR             JVM heaps (see the compose file's memory note)
#
# This suite is self-contained: it uses its own compose project (fix42-remote-uri),
# its own container names and its own ports, so it cannot disturb the single-server
# fix42-dashboard stack -- but it must not run *beside* it on a 6 GB podman machine
# (three JVMs plus AMPS; see docker/docker-compose.remote-uri.yml).
#
# It always starts from `down -v`. The AMPS journal is a journal: a previous run's
# seed would still be in it, replayed into this run's leaves from the EPOCH bookmark,
# and every count-based assertion below would fail on families nobody generated.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$MODULE_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT/docker/docker-compose.remote-uri.yml"
VENV="$HERE/.venv-e2e"
EXPECTED="$HERE/expected_remote_uri.json"

SEED="${SEED:-42}"
ORDERS="${ORDERS:-12}"
CHILDREN="${CHILDREN:-3}"
RATE="${RATE:-200}"
AMPS_URI="${AMPS_URI:-tcp://localhost:29007/amps/fix}"
COLLECTOR_URL="${COLLECTOR_URL:-http://localhost:10010}"
DH1_URL="${DH1_URL:-http://localhost:10011}"
DH2_URL="${DH2_URL:-http://localhost:10012}"
KEEP_STACK="${KEEP_STACK:-0}"
PYTEST_ARGS="${PYTEST_ARGS:-}"
STACK_TIMEOUT="${STACK_TIMEOUT:-420}"
BANNER_TIMEOUT="${BANNER_TIMEOUT:-240}"

# Container names, per service. Unlike the single-server suites this stack has four
# containers and the checks below are per container, so every wait names the one it
# is waiting for -- "the stack did not come up" is useless with three JVMs.
AMPS_CONTAINER="${AMPS_CONTAINER:-rx-amps}"
DH1_CONTAINER="${DH1_CONTAINER:-rx-dh1}"
DH2_CONTAINER="${DH2_CONTAINER:-rx-dh2}"
COLLECTOR_CONTAINER="${COLLECTOR_CONTAINER:-rx-collector}"

# podman ships outside the default PATH on the macOS box this was developed on.
case ":$PATH:" in
  *:/opt/podman/bin:*) ;;
  *) [[ -d /opt/podman/bin ]] && PATH="/opt/podman/bin:$PATH" ;;
esac
export PATH

# The pytest module takes ports, the waits take URLs; derive one from the other so a
# COLLECTOR_URL/DH*_URL override moves both together.
url_port() {
  local port="${1##*:}"
  [[ "$port" =~ ^[0-9]+$ ]] && printf '%s' "$port" || printf '%s' "$2"
}

log()  { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m[warn] %s\033[0m\n' "$*" >&2; }
die()  { printf '\033[1;31m[fail] %s\033[0m\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Container tooling: prefer `podman compose`, fall back to podman-compose,
#    then docker compose.
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

compose() { "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" "$@"; }

# A Deephaven log is far larger than a pipe buffer, so `logs | grep -q` gets the
# producer killed by SIGPIPE (exit 141) the moment grep exits on its first match --
# and `set -o pipefail` then reports the *pipeline* as failed, i.e. "no match", which
# silently inverts every check below. Capture the log, then grep the captured string.
dh_log()      { "$CONTAINER_CLI" logs "$1" 2>&1 || true; }
dh_log_has()  { local logs; logs="$(dh_log "$1")"; grep -qE "$2" <<<"$logs"; }
dh_log_show() { local logs; logs="$(dh_log "$1")"; grep -E -A "${3:-12}" "$2" <<<"$logs" || true; }

container_exists() { "$CONTAINER_CLI" container inspect "$1" >/dev/null 2>&1; }
current_cid()      { "$CONTAINER_CLI" container inspect --format '{{.Id}}' "$1" 2>/dev/null || true; }

# ---------------------------------------------------------------------------
# 2. Stack lifecycle.
#
# The EXIT trap must not blindly `down -v`: the compose project name is shared, so
# after a mid-run reclaim that teardown would land on whoever replaced our stack. It
# therefore tears down only when the containers still carry the ids THIS run started
# (captured after `up`; a restart -- the suite's own restart test included --
# preserves the id, only an outside recreate changes it). Decision table: no ids
# captured -> tear down (reap our own partial `up`); every captured id still matches
# or has vanished -> tear down; any id differs -> skip, because something else owns
# those containers now.
# ---------------------------------------------------------------------------
ALL_CONTAINERS=("$AMPS_CONTAINER" "$DH1_CONTAINER" "$DH2_CONTAINER" "$COLLECTOR_CONTAINER")
CAPTURED_CIDS=()

stack_vanished() {
  die "container $1 no longer exists -- it was removed while this suite waited $2. Another process (or session) likely tore down or reclaimed the fix42-remote-uri stack; re-run once it is free."
}

teardown_is_ours() {
  local i name want now
  (( ${#CAPTURED_CIDS[@]} )) || return 0
  for i in "${!ALL_CONTAINERS[@]}"; do
    name="${ALL_CONTAINERS[$i]}"
    want="${CAPTURED_CIDS[$i]:-}"
    [[ -n "$want" ]] || continue
    now="$(current_cid "$name")"
    [[ -z "$now" || "$now" == "$want" ]] || return 1
  done
  return 0
}

cleanup() {
  local rc=$?
  if [[ "$KEEP_STACK" == "1" ]]; then
    log "KEEP_STACK=1 -- leaving the fleet up"
    printf '    collector IDE : %s/ide   (Panels -> remote_uri_dashboard)\n' "$COLLECTOR_URL"
    printf '    leaves        : %s  %s\n' "$DH1_URL" "$DH2_URL"
    printf '    tear down later with: %s -f %s down -v\n' "${COMPOSE_CMD[*]}" "$COMPOSE_FILE"
  elif teardown_is_ours; then
    log "tearing down the fleet"
    compose down -v >/dev/null 2>&1 || true
  else
    warn "skipping teardown: the fix42-remote-uri containers are not the ones this run started (recreated by another process). If nothing replaced them this leaks a stack -- the next run's 'down -v' reaps it."
  fi
  exit "$rc"
}
trap cleanup EXIT

log "clearing any previous fleet and its volumes (a stale AMPS journal replays a previous seed)"
compose down -v >/dev/null 2>&1 || true

# The image is derived (server 42.4 + amps-python-client) and all three Deephaven
# services share the ONE tag localhost/fix42-deephaven-amps:42.4, so it is built once
# here, explicitly: `up -d` would build it too, but silently and inside the same step
# that waits for three JVMs, which makes a Dockerfile error look like a slow boot.
log "building the derived Deephaven+AMPS image (first run pulls the base image)"
compose build dh1 || die "image build failed -- see docker/deephaven-amps.Dockerfile"

log "starting the fleet (amps + dh1 + dh2 + collector)"
compose up -d

# Pin this run's container ids -- the EXIT trap's teardown condition.
for name in "${ALL_CONTAINERS[@]}"; do
  CAPTURED_CIDS+=("$(current_cid "$name")")
done
[[ -n "${CAPTURED_CIDS[3]:-}" ]] || warn "could not capture the $COLLECTOR_CONTAINER container id; teardown will proceed unconditionally"

# `up -d` returns once containers are created; wait for each server to actually serve.
wait_for_http() {
  local url="$1" container="$2" deadline code
  log "waiting for $container at $url (timeout ${STACK_TIMEOUT}s)"
  deadline=$(( $(date +%s) + STACK_TIMEOUT ))
  until code=$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$url" 2>/dev/null) && [[ "$code" != "000" ]]; do
    container_exists "$container" || stack_vanished "$container" "for HTTP on $url"
    if (( $(date +%s) > deadline )); then
      warn "last 50 log lines from $container:"
      dh_log "$container" | tail -50 >&2
      die "$container did not serve $url within ${STACK_TIMEOUT}s"
    fi
    sleep 3
  done
  echo "    $container responding (HTTP $code)"
}

wait_for_http "$DH1_URL" "$DH1_CONTAINER"
wait_for_http "$DH2_URL" "$DH2_CONTAINER"
wait_for_http "$COLLECTOR_URL" "$COLLECTOR_CONTAINER"

# HTTP 302 means the web server is up, which happens well BEFORE app mode has wired
# the AMPS subscriptions on a leaf or resolved four exports per leaf on the collector.
# Wait for each banner (or an error) so the assertions see the final state. The
# collector's is last on purpose: it only prints once every leaf has exported.
APP_FAILED='\[remote-uri\] FAILED|Traceback \(most recent call last\)'

wait_for_banner() {
  local container="$1" banner="$2" deadline
  log "waiting for '$banner' in $container"
  deadline=$(( $(date +%s) + BANNER_TIMEOUT ))
  until dh_log_has "$container" "$banner|$APP_FAILED"; do
    container_exists "$container" || stack_vanished "$container" "for its app-mode banner"
    if (( $(date +%s) > deadline )); then
      warn "no banner from $container after ${BANNER_TIMEOUT}s -- continuing; pytest will report what is missing"
      return 0
    fi
    sleep 3
  done
  if dh_log_has "$container" "$APP_FAILED"; then
    warn "$container reported an app-mode error:"
    dh_log_show "$container" "$APP_FAILED" 15 >&2
    warn "continuing -- pytest will report precisely which globals are missing"
    return 0
  fi
  dh_log_show "$container" "$banner" 12
}

wait_for_banner "$DH1_CONTAINER" "Remote-URI leaf DH1 -- ready"
wait_for_banner "$DH2_CONTAINER" "Remote-URI leaf DH2 -- ready"
wait_for_banner "$COLLECTOR_CONTAINER" "Remote-URI collector -- ready"

# ---------------------------------------------------------------------------
# 3. Python client venv
# ---------------------------------------------------------------------------
if [[ ! -x "$VENV/bin/python" ]]; then
  log "creating client venv at $VENV"
  python3 -m venv "$VENV" || die "python3 -m venv failed -- is python3 installed?"
fi
log "installing client requirements (pydeephaven 42.4 + pytest)"
"$VENV/bin/pip" install --quiet --disable-pip-version-check -r "$HERE/requirements.txt" \
  || die "pip install failed"

# ---------------------------------------------------------------------------
# 4. Publish the four correlated drop-copy tapes to AMPS
# ---------------------------------------------------------------------------
[[ -x "$ROOT/gradlew" ]] || die "missing $ROOT/gradlew -- build the Java modules first (see README)"

rm -f "$EXPECTED"
log "generator: --multi-oms --amps-uri $AMPS_URI seed=$SEED orders=$ORDERS children=$CHILDREN rate=$RATE"
( cd "$ROOT" && ./gradlew --console=plain --quiet :fix-mock-generator:run \
    --args="--multi-oms --amps-uri $AMPS_URI --seed $SEED --orders $ORDERS --children $CHILDREN --rate $RATE --emit-expected $EXPECTED" ) \
  || die "generator run failed (--multi-oms --amps-uri)"
[[ -s "$EXPECTED" ]] || die "generator did not write $EXPECTED"
echo "    expected export: $EXPECTED ($(wc -l < "$EXPECTED" | tr -d ' ') lines)"

# ---------------------------------------------------------------------------
# 5. Assert
# ---------------------------------------------------------------------------
log "running pytest"
cd "$HERE"
RXE2E_EXPECTED="$EXPECTED" \
RXE2E_COLLECTOR_PORT="$(url_port "$COLLECTOR_URL" 10010)" \
RXE2E_LEAF_PORTS="DH1:$(url_port "$DH1_URL" 10011),DH2:$(url_port "$DH2_URL" 10012)" \
RXE2E_CONTAINERS="$DH1_CONTAINER,$DH2_CONTAINER,$COLLECTOR_CONTAINER" \
CONTAINER_CLI="$CONTAINER_CLI" \
  "$VENV/bin/python" -m pytest test_remote_uri_e2e.py -v ${PYTEST_ARGS:+$PYTEST_ARGS}

log "remote-URI e2e passed"
