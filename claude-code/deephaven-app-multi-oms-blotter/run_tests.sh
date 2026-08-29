#!/usr/bin/env bash
#
# Create/refresh the module virtualenv and run the pytest suite.
#
# Invoked by `./gradlew :deephaven-app-multi-oms-blotter:pytest` (see
# build.gradle.kts) and usable standalone:  bash run_tests.sh [extra pytest args]
#
# The suite is deliberately deephaven-free: everything it exercises (topology
# validation, the sticky link map, key/name sanitizing, row augmentation, ingest
# configuration and the paging math) is pure python, so it runs on a bare host
# interpreter with nothing but pytest installed.
#
# Override the interpreter with PYTHON=/path/to/python3.
set -euo pipefail

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$MODULE_DIR"

PYTHON_BIN="${PYTHON:-python3}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "ERROR: '$PYTHON_BIN' was not found on PATH." >&2
  echo "       The :deephaven-app-multi-oms-blotter module needs Python 3.10+ to run its unit tests." >&2
  echo "       Install python3 (e.g. 'brew install python@3.12') or set PYTHON=/path/to/python3." >&2
  exit 1
fi

if ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)'; then
  echo "ERROR: $("$PYTHON_BIN" --version 2>&1) is too old." >&2
  echo "       multi_oms requires Python 3.10+. Set PYTHON=/path/to/python3.10+." >&2
  exit 1
fi

VENV_DIR="$MODULE_DIR/.venv"
if [ ! -d "$VENV_DIR" ]; then
  echo "[multi-oms-blotter] creating virtualenv at $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

if [ -x "$VENV_DIR/bin/python" ]; then
  VENV_PYTHON="$VENV_DIR/bin/python"
elif [ -x "$VENV_DIR/Scripts/python.exe" ]; then
  VENV_PYTHON="$VENV_DIR/Scripts/python.exe"
else
  echo "ERROR: virtualenv at $VENV_DIR looks broken (no python executable)." >&2
  echo "       Remove it and re-run: rm -rf '$VENV_DIR'" >&2
  exit 1
fi

"$VENV_PYTHON" -m pip install --quiet --disable-pip-version-check --editable ".[test]"

exec "$VENV_PYTHON" -m pytest tests/ -q "$@"
