"""Entry point: wires ingest -> pipeline -> DAG -> query API -> dashboard.

Works both in **Application Mode** (``/app.d/dashboard.app`` pointing at this file,
doc 04 section 7) and when pasted/exec'd in a Deephaven console.  Everything it
builds is published as a module-level global so it appears in the web IDE's Panels
menu and is reachable from ``pydeephaven`` (``session.open_table("order_state_latest")``).

Re-running the script is safe: the wired runtime is memoized on the ``dh_app``
package object, which lives in ``sys.modules``, so a second execution re-exports the
same tables instead of subscribing a second listener to Kafka.
"""

from __future__ import annotations

import os
import sys

# ---------------------------------------------------------------------------------
# sys.path bootstrap -- must run before importing dh_app / fix42cache, because this
# file is executed as a loose script inside the container (/scripts/dh_app/app.py),
# not as an installed package.
# ---------------------------------------------------------------------------------
try:
    _APP_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:  # pragma: no cover - exec'd from a string, no __file__
    _APP_DIR = "/scripts/dh_app"
_SRC_DIR = os.path.dirname(_APP_DIR) or "/scripts"

for _candidate in (_SRC_DIR, "/scripts"):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import traceback  # noqa: E402
from typing import Any, Callable, Dict, List, MutableMapping, Optional  # noqa: E402

import dh_app  # noqa: E402
from dh_app import ingest  # noqa: E402
from dh_app.dag import build_derived  # noqa: E402
from dh_app.dashboard import build_dashboard  # noqa: E402
from dh_app.ingest import build_fix_raw  # noqa: E402
from dh_app.pipeline import Pipeline  # noqa: E402
from dh_app.query_api import make_query_api  # noqa: E402

__all__ = ["Runtime", "main", "export"]

#: Attribute on the ``dh_app`` package holding the wired runtime across re-execs.
_RUNTIME_ATTR = "_FIX42_RUNTIME"

#: Module-level idempotence flag (doc 04 section 9.1 -- also keeps strong refs alive).
_STARTED = False


class Runtime:
    """Everything the wiring produced; kept alive by a module-level global.

    Deliberately a plain class rather than a ``dataclass``: Application Mode may
    exec this file with a ``__name__`` that is not registered in ``sys.modules``,
    which breaks ``dataclasses``' type introspection at class-creation time.
    """

    def __init__(
        self,
        fix_raw: Any,
        pipeline: Pipeline,
        streams: Optional[Dict[str, Any]] = None,
        tables: Optional[Dict[str, Any]] = None,
        api: Optional[Dict[str, Callable[..., Any]]] = None,
        dashboard: Optional[Any] = None,
    ) -> None:
        """Store the wired objects (also the strong references keeping them alive)."""
        self.fix_raw = fix_raw
        self.pipeline = pipeline
        self.streams: Dict[str, Any] = streams or {}
        self.tables: Dict[str, Any] = tables or {}
        self.api: Dict[str, Callable[..., Any]] = api or {}
        self.dashboard = dashboard

    @property
    def table_names(self) -> List[str]:
        """Every global table name this runtime exports, sorted."""
        return sorted(set(self.streams) | set(self.tables) | {"fix_raw"})


def _wire() -> Runtime:
    """Build the whole DAG once.

    Returns:
        The populated :class:`Runtime`.
    """
    fix_raw = build_fix_raw()
    pipeline = Pipeline()
    streams = pipeline.start(fix_raw)
    tables = build_derived(streams)
    api = make_query_api(tables)
    dashboard = build_dashboard(tables)
    return Runtime(
        fix_raw=fix_raw,
        pipeline=pipeline,
        streams=streams,
        tables=tables,
        api=api,
        dashboard=dashboard,
    )


def main() -> Runtime:
    """Wire the application, or return the already-wired runtime.

    Idempotent by design: Application Mode plus a console ``exec`` of the same file
    must not double-subscribe the Kafka listener.

    Returns:
        The :class:`Runtime` holding tables, query API and dashboard.
    """
    global _STARTED

    existing = getattr(dh_app, _RUNTIME_ATTR, None)
    if existing is not None:
        _STARTED = True
        print("[fix42] pipeline already running -- reusing the existing tables")
        return existing

    try:
        runtime = _wire()
    except Exception:  # noqa: BLE001 - surface startup failures in the server log
        print("[fix42] FAILED to start the FIX 4.2 dashboard application:")
        traceback.print_exc()
        raise

    setattr(dh_app, _RUNTIME_ATTR, runtime)
    _STARTED = True
    return runtime


def export(namespace: MutableMapping[str, Any], runtime: Runtime) -> None:
    """Publish every table, query-API function and the dashboard into ``namespace``.

    Args:
        namespace: Usually ``globals()`` of this module -- Application Mode exports
            those to the web IDE and to ``pydeephaven``.
        runtime: The wired runtime.
    """
    namespace["fix_raw"] = runtime.fix_raw
    namespace["fix42_pipeline"] = runtime.pipeline
    namespace.update(runtime.streams)
    namespace.update(runtime.tables)
    namespace.update(runtime.api)
    namespace["fix42_dashboard"] = runtime.dashboard


def _print_banner(runtime: Runtime) -> None:
    """Print a concise startup summary to the server log."""
    lines = [
        "=" * 78,
        "FIX 4.2 Order State Dashboard -- ready",
        f"  kafka bootstrap : {ingest.kafka_bootstrap()}",
        f"  topic           : {ingest.kafka_topic()} (seek to beginning)",
        f"  tables          : {', '.join(runtime.table_names)}",
        f"  query api       : {', '.join(sorted(runtime.api))}",
        f"  dashboard       : {'fix42_dashboard' if runtime.dashboard else 'unavailable (deephaven.ui missing) -- use the table panels'}",
        "=" * 78,
    ]
    print("\n".join(lines))


_RUNTIME = main()
export(globals(), _RUNTIME)
_print_banner(_RUNTIME)
