#!/usr/bin/env bash
#
# Multi-OMS drop-copy blotter end-to-end test (docs/09-multi-oms-blotter.md s10).
#
# Brings up Kafka + Deephaven with DH_APP=multi-oms-blotter, publishes four
# correlated FIX 4.2 drop-copy tapes with the Java generator's --multi-oms mode,
# then asserts orders_recon against the generator's own expected export through
# pydeephaven.
#
#   bash run_e2e.sh                  # down -v -> up -> generate -> pytest -> down -v
#   KEEP_STACK=1 bash run_e2e.sh     # leave the stack running afterwards
#
# Knobs (all optional):
#   SEED=42 ORDERS=12 CHILDREN=3 RATE=200   generator shape
#   BOOTSTRAP=localhost:19092               Kafka external listener (host side)
#   DH_URL=http://localhost:10000           Deephaven web/gRPC endpoint
#   STACK_TIMEOUT=300                       seconds to wait for Deephaven
#   PYTEST_ARGS="-k restart"                extra pytest flags
#
# This suite is self-contained: it never touches integration-test/, and it always
# starts from `down -v`. Dirty topics from a previous run leave extra families
# behind, and while the per-key assertions would survive that, the break_summary
# totals would not (doc 09 s10).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$MODULE_DIR/.." && pwd)"
COMPOSE_FILE="$ROOT/docker/docker-compose.yml"
VENV="$HERE/.venv-e2e"
EXPECTED="$HERE/expected_multi_oms.json"

SEED="${SEED:-42}"
ORDERS="${ORDERS:-12}"
CHILDREN="${CHILDREN:-3}"
RATE="${RATE:-200}"
BOOTSTRAP="${BOOTSTRAP:-localhost:19092}"
DH_URL="${DH_URL:-http://localhost:10000}"
KEEP_STACK="${KEEP_STACK:-0}"
PYTEST_ARGS="${PYTEST_ARGS:-}"
STACK_TIMEOUT="${STACK_TIMEOUT:-300}"
DH_CONTAINER_NAME="${DH_CONTAINER:-fix42-deephaven}"

# The compose stack runs one app out of docker/apps/, chosen by DH_APP. Pin it to
# this module's app: a DH_APP left over in the caller's shell would silently start
# the single-tape dashboard and every assertion below would fail on missing tables.
export DH_APP=multi-oms-blotter

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
log "compose command: ${COMPOSE_CMD[*]}  (cli: $CONTAINER_CLI)  app: $DH_APP"

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

# The Deephaven log is far larger than a pipe buffer, so `logs | grep -q` gets the
# producer killed by SIGPIPE (exit 141) the moment grep exits on its first match --
# and `set -o pipefail` then reports the *pipeline* as failed, i.e. "no match", which
# silently inverts every check below. Capture the log, then grep the captured string.
dh_log() { "$CONTAINER_CLI" logs "$DH_CONTAINER_NAME" 2>&1 || true; }
dh_log_has() { local logs; logs="$(dh_log)"; grep -qE "$1" <<<"$logs"; }
dh_log_show() { local logs; logs="$(dh_log)"; grep -E -A "${2:-12}" "$1" <<<"$logs" || true; }

# ---------------------------------------------------------------------------
# 2. Stack lifecycle. Only ever the two services this suite needs -- `down -v`
#    is scoped to the compose project, so unrelated containers on the same
#    machine are untouched.
# ---------------------------------------------------------------------------
cleanup() {
  local rc=$?
  if [[ "$KEEP_STACK" == "1" ]]; then
    log "KEEP_STACK=1 -- leaving the stack up (IDE: $DH_URL/ide -> multi_oms_blotter)"
    printf '    tear down later with: DH_APP=%s %s -f %s down -v\n' \
      "$DH_APP" "${COMPOSE_CMD[*]}" "$COMPOSE_FILE"
  else
    log "tearing down the stack"
    compose down -v >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
trap cleanup EXIT

log "clearing any previous stack and its volumes (dirty topics poison the assertions)"
compose down -v >/dev/null 2>&1 || true

# kafka-ui is deliberately not started: nothing in this suite reads it and it only
# slows the boot down.
log "starting stack (kafka + deephaven)"
compose up -d kafka deephaven

# `up -d` returns once containers are created; wait for Deephaven to actually serve.
log "waiting for Deephaven at $DH_URL (timeout ${STACK_TIMEOUT}s)"
deadline=$(( $(date +%s) + STACK_TIMEOUT ))
until code=$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "$DH_URL" 2>/dev/null) && [[ "$code" != "000" ]]; do
  if (( $(date +%s) > deadline )); then
    warn "last 50 log lines from $DH_CONTAINER_NAME:"
    dh_log | tail -50 >&2
    die "Deephaven did not come up within ${STACK_TIMEOUT}s"
  fi
  sleep 3
done
echo "    Deephaven responding (HTTP $code)"

# HTTP 302 means the web server is up, which happens a beat BEFORE app mode has
# finished wiring four Kafka consumers and four state machines. Wait for the banner
# (or an error) so the checks below see the final state rather than a half-built app.
BANNER="Multi-OMS Drop-Copy Blotter -- ready"
APP_FAILED="\[$DH_APP\] ERROR|\[multi-oms\] FAILED|Traceback \(most recent call last\)"

log "waiting for the app-mode banner"
banner_deadline=$(( $(date +%s) + 120 ))
until dh_log_has "$BANNER|$APP_FAILED"; do
  if (( $(date +%s) > banner_deadline )); then
    warn "no [multi-oms] banner after 120s -- continuing; pytest will report what is missing"
    break
  fi
  sleep 3
done

# The app-mode loader reports its own failures; surface them early rather than
# letting pytest fail with a confusing "table not found".
if dh_log_has "$APP_FAILED"; then
  warn "the app-mode script reported an error:"
  dh_log_show "$APP_FAILED" 12 >&2
  warn "continuing -- pytest will report precisely which globals are missing"
fi

# The banner is the quickest confirmation that all four hubs wired up; echo it so a
# failing run carries it in the same log.
if dh_log_has "$BANNER"; then
  log "app banner"
  dh_log_show "$BANNER" 12
else
  warn "no '$BANNER' line in the server log -- the app may still be wiring"
fi

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
# 4. Publish the four correlated drop-copy tapes
# ---------------------------------------------------------------------------
[[ -x "$ROOT/gradlew" ]] || die "missing $ROOT/gradlew -- build the Java modules first (see README)"

rm -f "$EXPECTED"
log "generator: --multi-oms seed=$SEED orders=$ORDERS children=$CHILDREN rate=$RATE"
( cd "$ROOT" && ./gradlew --console=plain --quiet :fix-mock-generator:run \
    --args="--multi-oms --bootstrap-servers $BOOTSTRAP --seed $SEED --orders $ORDERS --children $CHILDREN --rate $RATE --emit-expected $EXPECTED" ) \
  || die "generator run failed (--multi-oms)"
[[ -s "$EXPECTED" ]] || die "generator did not write $EXPECTED"
echo "    expected export: $EXPECTED ($(wc -l < "$EXPECTED" | tr -d ' ') lines)"

# ---------------------------------------------------------------------------
# 5. Assert
# ---------------------------------------------------------------------------
log "running pytest"
cd "$HERE"
MOMS_EXPECTED="$EXPECTED" DH_CONTAINER="$DH_CONTAINER_NAME" CONTAINER_CLI="$CONTAINER_CLI" \
  "$VENV/bin/python" -m pytest test_blotter_e2e.py -v ${PYTEST_ARGS:+$PYTEST_ARGS}

log "multi-OMS e2e passed"
