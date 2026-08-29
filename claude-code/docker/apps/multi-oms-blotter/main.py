"""App entrypoint: the multi-OMS drop-copy blotter.

Runs the full pipeline from `deephaven-app-multi-oms-blotter/src/multi_oms/app.py` --
one Kafka blink table per OMS hub -> one FIX state machine per hub -> cross-hub
linking + per-edge reconciliation DAG -> query API -> `deephaven.ui` dashboard.

Two source directories are on the path, not one:

  /moms-scripts  = deephaven-app-multi-oms-blotter/src  (the multi_oms package)
  /scripts       = deephaven-scripts/src                (fix42cache, used UNCHANGED)

`globals()` is passed on purpose: the entrypoint's tables must land in *this*
module's namespace to become server-side globals. See /dh-app-lib/loader.py.
"""

import sys

sys.path.insert(0, "/dh-app-lib")
# The loader puts `scripts_dir` on sys.path itself, but only one directory; this app
# needs both, and app-mode scripts run with __file__ UNSET so the entrypoint cannot
# always bootstrap from its own location.
for _candidate in ("/moms-scripts", "/scripts"):
    if _candidate not in sys.path:
        sys.path.insert(0, _candidate)

from loader import load  # noqa: E402  (the path inserts above have to run first)

load(
    "/moms-scripts/multi_oms/app.py",
    globals(),
    app_name="multi-oms-blotter",
    scripts_dir="/moms-scripts",
)
