"""App entrypoint: one remote-URI leaf (doc 10 section 5).

Runs `deephaven-remote-uri/src/remote_uri/app.py` with REMOTEURI_ROLE=leaf --
one AMPS bookmark subscription per hub this server owns -> one FIX state machine per
hub (fix42cache, unchanged) -> the leaf DAG -> the four exports the collector
resolves remotely (rx_orders, rx_id_index, rx_exposure, rx_leaf_stats).

Three source directories are on the path, in this precedence order:

  /remote-scripts = deephaven-remote-uri/src              (the remote_uri package)
  /moms-scripts   = deephaven-app-multi-oms-blotter/src   (multi_oms, UNCHANGED)
  /scripts        = deephaven-scripts/src                 (fix42cache + dh_app, UNCHANGED)

`globals()` is passed on purpose: the entrypoint's tables must land in *this*
module's namespace to become server-side globals -- which is also what makes them
reachable as dh+plain://<host>:10000/scope/<name> from the collector.
See /dh-app-lib/loader.py.
"""

import sys

sys.path.insert(0, "/dh-app-lib")
# The loader puts `scripts_dir` on sys.path itself, but only one directory; this app
# needs three, and app-mode scripts run with __file__ UNSET so the entrypoint cannot
# always bootstrap from its own location. Reversed, because insert(0, ...) puts the
# last one first: the final order is /remote-scripts, /moms-scripts, /scripts.
for _candidate in ("/scripts", "/moms-scripts", "/remote-scripts"):
    if _candidate not in sys.path:
        sys.path.insert(0, _candidate)

from loader import load  # noqa: E402  (the path inserts above have to run first)

load(
    "/remote-scripts/remote_uri/app.py",
    globals(),
    app_name="remote-uri-leaf",
    scripts_dir="/remote-scripts",
)
