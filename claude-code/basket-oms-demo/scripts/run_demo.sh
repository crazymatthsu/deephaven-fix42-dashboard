#!/usr/bin/env bash
#
# Bring the basket OMS demo up (default), tear it down, or show its status/logs.
#
#   bash basket-oms-demo/scripts/run_demo.sh            # up -d, wait for healthy, print URLs
#   bash basket-oms-demo/scripts/run_demo.sh down       # down -v
#   bash basket-oms-demo/scripts/run_demo.sh status     # container + health
#   bash basket-oms-demo/scripts/run_demo.sh logs       # the app's log lines
#
# Knobs (all optional): DH_PORT (10000), DH_XMX (2g), DH_CONTAINER (oms-deephaven) and the
# OMS_* variables of docs/13-basket-oms-demo.md §6 -- e.g.
#   OMS_MOCK_BASKETS=8 OMS_SIM_INTERVAL_MS=300 bash basket-oms-demo/scripts/run_demo.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
COMPOSE_FILE="$ROOT/docker/docker-compose.basket-oms.yml"
CONTAINER="${DH_CONTAINER:-oms-deephaven}"
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
          echo "[basket-oms] container exited; last log lines:" >&2
          podman logs --tail 40 "$CONTAINER" >&2 || true
          return 1
        fi
        ;;
    esac
    sleep 3
  done
  echo "[basket-oms] timed out waiting for $CONTAINER to become healthy (last state: $state)" >&2
  return 1
}

case "$cmd" in
  up)
    echo "[basket-oms] starting $CONTAINER on :$PORT (compose project basket-oms-demo)"
    "${COMPOSE[@]}" up -d
    wait_healthy
    LOG="$(podman logs "$CONTAINER" 2>&1 || true)"
    if printf '%s\n' "$LOG" | grep -q 'entrypoint loaded OK'; then
      echo "[basket-oms] app loaded"
    else
      echo "[basket-oms] WARNING: the app has not reported 'entrypoint loaded OK' yet; check: $0 logs"
    fi
    printf '%s\n' "$LOG" | grep '^\[basket-oms\]' | tail -5 || true
    echo
    echo "  IDE:        http://localhost:$PORT/ide                (Panels ▸ basket_oms_dashboard)"
    echo "  Dashboard:  http://localhost:$PORT/iframe/widget/?name=basket_oms_dashboard"
    echo "  Tables:     http://localhost:$PORT/iframe/table/?name=oms_orders_view"
    echo
    echo "  console:    oms_status(); oms_new_basket('Demo', ['AAPL BUY 1000', 'MSFT SELL 500 LMT 415.20'], route='NYSE')"
    ;;
  down)
    "${COMPOSE[@]}" down -v
    ;;
  status)
    podman ps -a --filter "name=$CONTAINER" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
    ;;
  logs)
    podman logs "$CONTAINER" 2>&1 | grep -E '^\[basket-oms\]|Traceback|Error|error' | tail -60 || true
    ;;
  *)
    echo "usage: $0 [up|down|status|logs]" >&2
    exit 2
    ;;
esac
