#!/usr/bin/env bash
#
# Bring the order tree reconciliation demo up (default), tear it down, or show its status/logs.
#
#   bash order-tree-recon/scripts/run_demo.sh            # up -d, wait for healthy, print URLs
#   bash order-tree-recon/scripts/run_demo.sh down       # down -v
#   bash order-tree-recon/scripts/run_demo.sh status     # container + health
#   bash order-tree-recon/scripts/run_demo.sh logs       # the app's log lines
#
# Knobs (all optional): DH_PORT (10000), DH_XMX (2g), DH_CONTAINER (otr-deephaven) and the
# OTR_* variables of docs/14-order-tree-reconciliation.md section 3 -- e.g.
#   OTR_MOCK_FAMILIES=200 OTR_SEED=7 bash order-tree-recon/scripts/run_demo.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
COMPOSE_FILE="$ROOT/docker/docker-compose.order-tree.yml"
CONTAINER="${DH_CONTAINER:-otr-deephaven}"
PORT="${DH_PORT:-10000}"
COMPOSE=(podman compose -f "$COMPOSE_FILE")

cmd="${1:-up}"

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
    echo "[order-tree] starting $CONTAINER on :$PORT (compose project order-tree-recon)"
    "${COMPOSE[@]}" up -d
    wait_healthy
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
  status)
    podman ps -a --filter "name=$CONTAINER" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
    ;;
  logs)
    podman logs "$CONTAINER" 2>&1 | grep -E '^\[order-tree\]|Traceback|Error|error' | tail -60 || true
    ;;
  *)
    echo "usage: $0 [up|down|status|logs]" >&2
    exit 2
    ;;
esac
