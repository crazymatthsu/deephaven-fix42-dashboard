#!/usr/bin/env bash
#
# Bring the market-data demo up end to end with podman:
#
#   bash scripts/run_demo.sh                 # local parquet files -> Deephaven
#   MD_SOURCE=s3 bash scripts/run_demo.sh    # + MinIO: upload the files, Deephaven reads S3
#   bash scripts/run_demo.sh down            # tear the stack down (keeps the data dir)
#
# Steps: (1) generate the mock data if the data dir is empty, (2) build the derived image
# and start the stack, (3) for s3: wait for MinIO, create the bucket and upload the tree,
# (4) wait for the Deephaven banner and print the URLs.
#
# Knobs (all optional): MD_SOURCE (local|s3), MD_DATA_DIR, MD_S3_BUCKET, MD_S3_PREFIX,
# DH_PORT, COMPOSE (podman compose | podman-compose | docker compose), plus every MD_*
# variable the compose file forwards.
set -euo pipefail

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="$(cd "$MODULE_DIR/.." && pwd)"          # claude-code/
COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.market-data.yml"
DATA_DIR="${MD_DATA_DIR:-$MODULE_DIR/data}"
SOURCE="${MD_SOURCE:-local}"
BUCKET="${MD_S3_BUCKET:-market-data}"
PREFIX="${MD_S3_PREFIX:-ohlc}"
MINIO_PORT="${MD_MINIO_PORT:-9000}"
DH_PORT="${DH_PORT:-10000}"
CONTAINER="${DH_CONTAINER:-md-deephaven}"

log() { echo "[run_demo] $*"; }

# -- pick a compose implementation ---------------------------------------------------
if [ -n "${COMPOSE:-}" ]; then
  COMPOSE_CMD="$COMPOSE"
elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
  COMPOSE_CMD="podman compose"
elif command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_CMD="podman-compose"
elif command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
else
  echo "ERROR: need podman (with compose) or docker compose on PATH" >&2
  exit 1
fi
RUNTIME_CMD="${COMPOSE_CMD%% *}"   # podman | podman-compose | docker
[ "$RUNTIME_CMD" = "podman-compose" ] && RUNTIME_CMD="podman"

# ${arr[@]+"${arr[@]}"} below: expanding an EMPTY array under `set -u` is an error on
# bash 3.2 (macOS default); this idiom expands to nothing instead.
PROFILE_ARGS=()
[ "$SOURCE" = "s3" ] && PROFILE_ARGS=(--profile s3)

if [ "${1:-}" = "down" ]; then
  log "stopping the stack"
  MD_SOURCE="$SOURCE" $COMPOSE_CMD -f "$COMPOSE_FILE" ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} down -v
  exit 0
fi

case "$SOURCE" in
  local|s3) ;;
  *) echo "ERROR: MD_SOURCE must be local or s3 (got '$SOURCE')" >&2; exit 1 ;;
esac

# -- 1. mock data --------------------------------------------------------------------
if [ -z "$(find "$DATA_DIR" -name '*.parquet' -print -quit 2>/dev/null)" ]; then
  log "no parquet files under $DATA_DIR -- generating the mock universe (last 30 days)"
  MD_LOCAL_ROOT="$DATA_DIR" bash "$MODULE_DIR/scripts/generate_mock_data.sh" --quiet
else
  log "using existing parquet files under $DATA_DIR"
fi

# -- 2. stack up ---------------------------------------------------------------------
log "starting the stack with: $COMPOSE_CMD (MD_SOURCE=$SOURCE)"
export MD_SOURCE="$SOURCE" MD_DATA_DIR="$DATA_DIR" MD_S3_BUCKET="$BUCKET" MD_S3_PREFIX="$PREFIX"
$COMPOSE_CMD -f "$COMPOSE_FILE" ${PROFILE_ARGS[@]+"${PROFILE_ARGS[@]}"} up -d --build

# -- 3. s3: seed the bucket ----------------------------------------------------------
if [ "$SOURCE" = "s3" ]; then
  log "waiting for MinIO on localhost:$MINIO_PORT"
  for _ in $(seq 1 60); do
    if curl -sf "http://localhost:$MINIO_PORT/minio/health/live" >/dev/null 2>&1; then break; fi
    sleep 1
  done
  VENV_PYTHON="$MODULE_DIR/.venv/bin/python"
  [ -x "$VENV_PYTHON" ] || VENV_PYTHON="$MODULE_DIR/.venv/Scripts/python.exe"
  "$VENV_PYTHON" -m pip install --quiet --disable-pip-version-check --editable "$MODULE_DIR[s3]"
  log "uploading $DATA_DIR to s3://$BUCKET/$PREFIX (MinIO)"
  "$VENV_PYTHON" -m market_data_demo upload --root "$DATA_DIR" --bucket "$BUCKET" --prefix "$PREFIX" \
    --endpoint "http://localhost:$MINIO_PORT" \
    --access-key "${MD_S3_ACCESS_KEY_ID:-minioadmin}" --secret-key "${MD_S3_SECRET_ACCESS_KEY:-minioadmin}" --quiet
  # Deephaven scanned an empty bucket at startup: restart it so the inventory sees the upload.
  log "restarting $CONTAINER so it re-scans the bucket"
  $RUNTIME_CMD restart "$CONTAINER" >/dev/null
fi

# -- 4. wait for the banner ----------------------------------------------------------
log "waiting for the Deephaven app banner"
for _ in $(seq 1 90); do
  if $RUNTIME_CMD logs "$CONTAINER" 2>&1 | grep -q "Market Data Demo -- ready"; then break; fi
  sleep 2
done
$RUNTIME_CMD logs "$CONTAINER" 2>&1 | grep -A 12 "Market Data Demo -- ready" || {
  echo "WARNING: banner not seen yet; check: $RUNTIME_CMD logs -f $CONTAINER" >&2
}

echo
log "IDE        : http://localhost:$DH_PORT/ide          (Panels ▸ market_data_dashboard)"
log "dashboard  : http://localhost:$DH_PORT/iframe/widget/?name=market_data_dashboard"
[ "$SOURCE" = "s3" ] && log "MinIO      : http://localhost:${MD_MINIO_CONSOLE_PORT:-9001}  (minioadmin / minioadmin)"
log "tear down  : bash scripts/run_demo.sh down"
