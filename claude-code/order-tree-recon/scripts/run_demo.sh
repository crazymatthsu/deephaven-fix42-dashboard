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
# OTR_* variables of docs/14-order-tree-reconciliation.md sections 3 and 12 -- e.g.
#   OTR_MOCK_FAMILIES=200 OTR_SEED=7 bash order-tree-recon/scripts/run_demo.sh
#   OTR_SOURCE=fix-files bash order-tree-recon/scripts/run_demo.sh      # the sample FIX tapes
#   OTR_SOURCE=fix-files OTR_FIX_HOST_DIR=/abs/path/to/your/logs bash order-tree-recon/scripts/run_demo.sh
set -euo pipefail
# Artifact sources -- container registries, pip index, ... -- from claude-code/repos.env and
# your override file (docs/15). Loaded before anything pulls an image or installs a package.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/repos.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
COMPOSE_FILE="$ROOT/docker/docker-compose.order-tree.yml"
CONTAINER="${DH_CONTAINER:-otr-deephaven}"
PORT="${DH_PORT:-10000}"
COMPOSE=(podman compose -f "$COMPOSE_FILE")

# A relative OTR_FIX_HOST_DIR would resolve against docker/, not the caller's directory.
if [ -n "${OTR_FIX_HOST_DIR:-}" ]; then
  [ -d "$OTR_FIX_HOST_DIR" ] || { echo "[order-tree] OTR_FIX_HOST_DIR=$OTR_FIX_HOST_DIR is not a directory" >&2; exit 2; }
  OTR_FIX_HOST_DIR="$(cd "$OTR_FIX_HOST_DIR" && pwd)"
  # Git Bash: /c/Users/... -> C:/Users/..., the form a Windows podman understands.
  if command -v cygpath >/dev/null 2>&1; then OTR_FIX_HOST_DIR="$(cygpath -m "$OTR_FIX_HOST_DIR")"; fi
  export OTR_FIX_HOST_DIR
fi

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
    printf '%s\n' "$LOG" | grep '^\[order-tree\]' | tail -9 || true
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
