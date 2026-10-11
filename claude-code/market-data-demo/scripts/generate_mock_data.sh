#!/usr/bin/env bash
#
# Generate the mock parquet market data into market-data-demo/data (or MD_LOCAL_ROOT).
#
#   bash scripts/generate_mock_data.sh                                  # demo universe, last 30 days
#   bash scripts/generate_mock_data.sh --symbols AAPL,MSFT --start 2026-08-03 --end 2026-09-04
#   ./gradlew :market-data-demo:generateMockData                         # the same, through gradle
#
# Uses the module's own virtualenv (.venv, created on demand -- the same one run_tests.sh
# uses), so nothing is installed globally. Every argument is passed through to
# `python -m market_data_demo generate`.
set -euo pipefail
# Artifact sources -- container registries, pip index, ... -- from claude-code/repos.env and
# your override file (docs/15). Loaded before anything pulls an image or installs a package.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/repos.sh"

MODULE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$MODULE_DIR"

# A working Python 3.10+: $PYTHON, else python3 / python / `py -3` -- Windows ships no
# python3 (find_python3 is in scripts/repos.sh, sourced above; docs/15 section 10).
if ! PYTHON_BIN="$(find_python3 3.10)"; then
  echo "ERROR: the :market-data-demo module needs Python 3.10+ to generate mock data." >&2
  exit 1
fi
VENV_DIR="$MODULE_DIR/.venv"
if [ ! -d "$VENV_DIR" ]; then
  echo "[market-data-demo] creating virtualenv at $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi
if [ -x "$VENV_DIR/bin/python" ]; then
  VENV_PYTHON="$VENV_DIR/bin/python"
else
  VENV_PYTHON="$VENV_DIR/Scripts/python.exe"
fi

"$VENV_PYTHON" -m pip install --quiet --disable-pip-version-check --editable ".[s3,test]"

exec "$VENV_PYTHON" -m market_data_demo generate "$@"
