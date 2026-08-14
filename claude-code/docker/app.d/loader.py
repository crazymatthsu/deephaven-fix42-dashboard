"""Application-Mode loader for the FIX 4.2 dashboard.

Deephaven's Application Mode evaluates this file inside the server's Python
console session. Everything this script leaves in ``globals()`` becomes a
server-side global, which is exactly how tables and the ``fix42_dashboard``
component surface in the web IDE's Panels menu (and how ``pydeephaven``'s
``session.open_table(...)`` finds them in the integration test).

Why a loader instead of pointing ``file_0`` straight at the real entrypoint
(both forms were verified to work on ghcr.io/deephaven/server:42.4):

  1. App-mode scripts run with ``__file__`` UNSET and ``__name__ == "__main__"``.
     A ``dh_app/app.py`` that bootstraps ``sys.path`` from its own location --
     the conventional pattern, and the one doc 05 s4 describes -- would raise
     NameError. This loader defines ``__file__`` before executing it, so the
     entrypoint works under app mode *and* when run by hand in the IDE console.
  2. It puts ``/scripts`` on ``sys.path`` itself, so ``import fix42cache`` and
     ``import dh_app...`` resolve regardless of how the entrypoint is written
     (belt and braces with PYTHONPATH=/scripts set in docker-compose.yml).
  3. A missing entrypoint produces one actionable log line instead of a stack
     trace from deep inside the app-mode injector.

Execution is into this module's own ``globals()`` on purpose -- ``runpy`` and
``exec(code, {})`` would build the tables in a throwaway namespace and nothing
would appear in the IDE.
"""

import os
import sys
import traceback

SCRIPTS_DIR = os.environ.get("FIX42_SCRIPTS_DIR", "/scripts")
ENTRYPOINT = os.environ.get("FIX42_ENTRYPOINT", os.path.join(SCRIPTS_DIR, "dh_app", "app.py"))

if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

if not os.path.isfile(ENTRYPOINT):
    print(
        "[fix42-loader] ERROR: entrypoint not found: {ep}\n"
        "[fix42-loader] The deephaven container mounts deephaven-scripts/src at {sd}.\n"
        "[fix42-loader] Check that deephaven-scripts/src/dh_app/app.py exists on the host\n"
        "[fix42-loader] and that docker-compose.yml's '../deephaven-scripts/src:/scripts:ro,z'\n"
        "[fix42-loader] volume resolved. The server is still up: the IDE works, but no\n"
        "[fix42-loader] FIX tables or dashboard will be present.".format(ep=ENTRYPOINT, sd=SCRIPTS_DIR),
        flush=True,
    )
else:
    print("[fix42-loader] loading entrypoint {ep}".format(ep=ENTRYPOINT), flush=True)
    try:
        with open(ENTRYPOINT, "r", encoding="utf-8") as _fh:
            _source = _fh.read()
        # Give the entrypoint a real __file__ so sys.path bootstrapping inside it works.
        __file__ = ENTRYPOINT  # noqa: F841  (consumed by the exec'd module)
        exec(compile(_source, ENTRYPOINT, "exec"), globals())
        print("[fix42-loader] entrypoint loaded OK", flush=True)
    except Exception:  # noqa: BLE001 - app mode must not die on a script error
        print("[fix42-loader] ERROR: entrypoint raised:", flush=True)
        traceback.print_exc()
        print(
            "[fix42-loader] Server stays up so you can debug in the IDE console; "
            "re-run the script there after fixing it.",
            flush=True,
        )
