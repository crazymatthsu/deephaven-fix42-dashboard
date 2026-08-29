"""Shared Application-Mode helper for every app under ``docker/apps/``.

Mounted read-only at ``/dh-app-lib`` (see ``docker-compose.yml``), separately from
the selected app, so all apps share one copy instead of each carrying its own.

Deephaven's Application Mode evaluates an app's script inside the server's Python
console session. Everything left in that script's ``globals()`` becomes a
server-side global, which is how tables and ``deephaven.ui`` components surface in
the web IDE's Panels menu and how ``pydeephaven``'s ``session.open_table(...)``
finds them.

Why an app's ``main.py`` calls :func:`load` instead of pointing ``file_0`` straight
at the real entrypoint (both forms were verified to work on
``ghcr.io/deephaven/server:42.4``):

  1. App-mode scripts run with ``__file__`` UNSET and ``__name__ == "__main__"``.
     An entrypoint that bootstraps ``sys.path`` from its own location -- the
     conventional pattern, and the one doc 05 section 4 describes -- would raise
     NameError. :func:`load` defines ``__file__`` in the target namespace first, so
     the entrypoint works under app mode *and* when run by hand in the IDE console.
  2. It puts the scripts directory on ``sys.path`` itself, so ``import fix42cache``
     and ``import dh_app...`` resolve regardless of how the entrypoint is written
     (belt and braces with ``PYTHONPATH=/scripts`` set in ``docker-compose.yml``).
  3. A missing entrypoint produces one actionable log line instead of a stack trace
     from deep inside the app-mode injector, and a raising entrypoint leaves the
     server up so the IDE console can be used to debug it.

The ``namespace`` argument is load-bearing: an app's ``main.py`` must pass its own
``globals()``. ``runpy`` or ``exec(code, {})`` would build the tables in a throwaway
namespace and nothing would appear in the IDE.
"""

import os
import sys
import traceback

__all__ = ["DEFAULT_SCRIPTS_DIR", "SCRIPTS_DIR", "load"]

#: Where ``deephaven-scripts/src`` is mounted inside the container.
DEFAULT_SCRIPTS_DIR = "/scripts"

#: Overridable for a container that mounts the sources elsewhere.
SCRIPTS_DIR = os.environ.get("DH_SCRIPTS_DIR", DEFAULT_SCRIPTS_DIR)


def load(entrypoint, namespace, app_name=None, scripts_dir=None):
    """Execute ``entrypoint`` into ``namespace``, reporting failures legibly.

    Args:
        entrypoint: Absolute container path of the python file to run.
        namespace: The dict to execute into -- pass the caller's ``globals()``.
        app_name: Label for log lines; defaults to ``DH_APP`` or the app directory.
        scripts_dir: Directory to put on ``sys.path``; defaults to
            :data:`SCRIPTS_DIR`.

    Returns:
        True if the entrypoint ran to completion, False otherwise. Never raises:
        Application Mode must not die on a script error, or the server comes up
        with no IDE to debug it from.
    """
    label = app_name or os.environ.get("DH_APP") or "app"
    src = scripts_dir or SCRIPTS_DIR

    if src and src not in sys.path:
        sys.path.insert(0, src)

    if not os.path.isfile(entrypoint):
        print(
            "[{label}] ERROR: entrypoint not found: {ep}\n"
            "[{label}] The deephaven container mounts deephaven-scripts/src at {sd}.\n"
            "[{label}] Check that the file exists on the host and that\n"
            "[{label}] docker-compose.yml's '../deephaven-scripts/src:/scripts:ro,z'\n"
            "[{label}] volume resolved. The server is still up: the IDE works, but this\n"
            "[{label}] app published nothing.".format(label=label, ep=entrypoint, sd=src),
            flush=True,
        )
        return False

    print("[{label}] loading entrypoint {ep}".format(label=label, ep=entrypoint), flush=True)
    try:
        with open(entrypoint, "r", encoding="utf-8") as handle:
            source = handle.read()
        # Give the entrypoint a real __file__ so sys.path bootstrapping inside it works.
        namespace["__file__"] = entrypoint
        exec(compile(source, entrypoint, "exec"), namespace)
    except Exception:  # noqa: BLE001 - app mode must not die on a script error
        print("[{label}] ERROR: entrypoint raised:".format(label=label), flush=True)
        traceback.print_exc()
        print(
            "[{label}] Server stays up so you can debug in the IDE console; "
            "re-run the script there after fixing it.".format(label=label),
            flush=True,
        )
        return False

    print("[{label}] entrypoint loaded OK".format(label=label), flush=True)
    return True
