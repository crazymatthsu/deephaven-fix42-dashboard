"""App entrypoint: the FIX 4.2 order-state dashboard.

Runs the full pipeline from `deephaven-scripts/src/dh_app/app.py` -- ingest
(Kafka or AMPS, see FIX42_SOURCE) -> state machine -> DAG -> query API ->
`deephaven.ui` dashboard.

`globals()` is passed on purpose: the entrypoint's tables must land in *this*
module's namespace to become server-side globals. See /dh-app-lib/loader.py.
"""

import sys

sys.path.insert(0, "/dh-app-lib")

from loader import load  # noqa: E402  (the path insert above has to run first)

load("/scripts/dh_app/app.py", globals(), app_name="fix42-dashboard")
