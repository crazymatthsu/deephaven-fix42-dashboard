"""The smallest app that still demonstrates the DH_APP switch.

Publishes one table, `example_heartbeat`, and nothing else. Bringing this up
instead of `fix42-dashboard` shows the selector works: the FIX globals are
absent and this one is present.

No import from `/scripts` on purpose -- an app template should not need the rest
of the repo to prove the wiring.
"""

import os

from deephaven import empty_table

example_heartbeat = empty_table(1).update(
    [
        "App = `" + os.environ.get("DH_APP", "example-minimal") + "`",
        "StartedAt = now()",
        "Note = `swap apps with DH_APP=<folder under docker/apps>`",
    ]
)

print("[example-minimal] published example_heartbeat", flush=True)
