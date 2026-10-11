#!/usr/bin/env bash
#
# Bring the order tree reconciliation demo up (default), tear it down, or show its status/logs.
#
#   bash order-tree-recon/scripts/run_demo.sh            # up -d, wait for healthy, print URLs
#   bash order-tree-recon/scripts/run_demo.sh down       # down -v
#   bash order-tree-recon/scripts/run_demo.sh status     # container + health
#   bash order-tree-recon/scripts/run_demo.sh logs       # the app's log lines
#   OTR_SOURCE=amps bash order-tree-recon/scripts/run_demo.sh [up|down|status|logs|publish]
#
# Knobs (all optional): DH_PORT (10000), DH_XMX (2g), DH_CONTAINER (otr-deephaven) and the
# OTR_* variables of docs/14-order-tree-reconciliation.md section 9 -- e.g.
#   OTR_MOCK_FAMILIES=200 OTR_SEED=7 bash order-tree-recon/scripts/run_demo.sh
#
# OTR_SOURCE=amps runs the AMPS-fed stack instead (docker/docker-compose.order-tree-amps.yml:
# AMPS from your local image + Deephaven on :10020, container otr-amps-deephaven, doc 14
# section 13). `up` then publishes the mock book (OTR_SEED / OTR_MOCK_FAMILIES) into the SOW
# topics from inside the Deephaven container; OTR_AMPS_PUBLISH=0 skips that, and
# `publish` re-publishes it on a running stack.
set -euo pipefail
# Artifact sources -- container registries, pip index, ... -- from claude-code/repos.env and
# your override file (docs/15). Loaded before anything pulls an image or installs a package.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/repos.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
SOURCE="${OTR_SOURCE:-mock}"
case "$SOURCE" in
  mock)
    COMPOSE_FILE="$ROOT/docker/docker-compose.order-tree.yml"
    CONTAINER="${DH_CONTAINER:-otr-deephaven}"
    PORT="${DH_PORT:-10000}"
    ;;
  amps)
    COMPOSE_FILE="$ROOT/docker/docker-compose.order-tree-amps.yml"
    CONTAINER="${DH_CONTAINER:-otr-amps-deephaven}"
    PORT="${DH_PORT:-10020}"
    ;;
  *)
    echo "OTR_SOURCE=$SOURCE: expected mock or amps" >&2
    exit 2
    ;;
esac
export DH_CONTAINER="$CONTAINER" DH_PORT="$PORT"
COMPOSE=(podman compose -f "$COMPOSE_FILE")

cmd="${1:-up}"

# The mock book into the AMPS SOW topics, published from inside the Deephaven container
# (its image has the AMPS client; the host needs nothing). --clear first: a re-publish
# replaces the book instead of adding a second seed's families to it.
publish_book() {
  podman exec "$CONTAINER" python -m order_tree_recon.amps_publish \
    --uri "tcp://amps:9007/amps/json" --prefix "${OTR_AMPS_TOPIC_PREFIX:-otr.}" \
    --seed "${OTR_SEED:-42}" --families "${OTR_MOCK_FAMILIES:-24}" --clear
}

wait_healthy() {
  local deadline=$(( $(date +%s) + ${DH_WAIT_SECS:-240} ))
  local state=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    state="$(podman inspect --format '{{.State.Health.Status}}' "$CONTAINER" 2>/dev/null || echo missing)"
    case "$state" in
      healthy) return 0 ;;
      missing|exited|unhealthy)
        if [ "$state" != "missing" ] && [ "$(podman inspect --format '{{.State.Status}}' "$CONTAINER" 2>/dev/null)" = "exited" ]; then
          echo "[order-tree] container exited; last log lines:" >&2
          podman logs --tail 40 "$CONTAINER" >&2 || true
          return 1
        fi
        ;;
    esac
    sleep 3
  done
  echo "[order-tree] timed out waiting for $CONTAINER to become healthy (last state: $state)" >&2
  return 1
}

case "$cmd" in
  up)
    echo "[order-tree] starting $CONTAINER on :$PORT (source: $SOURCE, $(basename "$COMPOSE_FILE"))"
    "${COMPOSE[@]}" up -d
    wait_healthy
    if [ "$SOURCE" = amps ] && [ "${OTR_AMPS_PUBLISH:-1}" != 0 ]; then
      publish_book
    fi
    LOG="$(podman logs "$CONTAINER" 2>&1 || true)"
    if printf '%s\n' "$LOG" | grep -q 'entrypoint loaded OK'; then
      echo "[order-tree] app loaded"
    else
      echo "[order-tree] WARNING: the app has not reported 'entrypoint loaded OK' yet; check: $0 logs"
    fi
    printf '%s\n' "$LOG" | grep '^\[order-tree\]' | tail -5 || true
    echo
    echo "  IDE:        http://localhost:$PORT/ide                (Panels ▸ order_tree_dashboard)"
    echo "  Dashboard:  http://localhost:$PORT/iframe/widget/?name=order_tree_dashboard"
    echo "  Tables:     http://localhost:$PORT/iframe/table/?name=otr_exposure"
    echo
    echo "  console:    r = order_tree('DUNE', 'MSFT', 'BUY'); r['ladder']; find_tree('OR-00013.2')"
    ;;
  down)
    "${COMPOSE[@]}" down -v
    ;;
  publish)
    [ "$SOURCE" = amps ] || { echo "publish needs OTR_SOURCE=amps" >&2; exit 2; }
    publish_book
    ;;
  status)
    podman ps -a --filter "name=$CONTAINER" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
    if [ "$SOURCE" = amps ]; then
      podman ps -a --filter "name=^${OTR_AMPS_CONTAINER:-otr-amps}\$" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
    fi
    ;;
  logs)
    podman logs "$CONTAINER" 2>&1 | grep -E '^\[order-tree\]|Traceback|Error|error' | tail -60 || true
    ;;
  *)
    echo "usage: [OTR_SOURCE=mock|amps] $0 [up|down|status|logs|publish]" >&2
    exit 2
    ;;
esac
