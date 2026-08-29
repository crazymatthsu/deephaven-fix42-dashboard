"""Entry point: wires per-hub ingest -> folds -> DAG -> query API -> dashboard.

Works both in **Application Mode** (``docker/apps/multi-oms-blotter/multi-oms-blotter.app``
pointing at this file through the shared loader) and when pasted/exec'd in a
Deephaven console.  Everything it builds is published as a module-level global so it
appears in the web IDE's Panels menu and is reachable from ``pydeephaven``
(``session.open_table("orders_recon")``).

Re-running the script is safe: the wired runtime is memoized on the ``multi_oms``
package object, which lives in ``sys.modules``, so a second execution re-exports the
same tables instead of opening a second subscription per hub topic.
"""

from __future__ import annotations

import os
import sys

# ---------------------------------------------------------------------------------
# sys.path bootstrap -- must run before importing multi_oms / fix42cache, because
# this file is executed as a loose script inside the container
# (/moms-scripts/multi_oms/app.py), not as an installed package. Two directories
# matter: this module's own src dir, and /scripts for the unmodified fix42cache.
# ---------------------------------------------------------------------------------
try:
    _APP_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:  # pragma: no cover - exec'd from a string, no __file__
    _APP_DIR = "/moms-scripts/multi_oms"
_SRC_DIR = os.path.dirname(_APP_DIR) or "/moms-scripts"

for _candidate in (_SRC_DIR, "/moms-scripts", "/scripts"):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import traceback  # noqa: E402
from typing import Any, Callable, Dict, List, MutableMapping, Optional  # noqa: E402

import multi_oms  # noqa: E402
from multi_oms import config, ingest  # noqa: E402
from multi_oms.dag import build_derived  # noqa: E402
from multi_oms.dashboard import build_dashboard  # noqa: E402
from multi_oms.linking import sanitize_hub  # noqa: E402
from multi_oms.pipeline import MultiOmsPipeline  # noqa: E402
from multi_oms.query_api import make_query_api  # noqa: E402

__all__ = ["Runtime", "main", "export", "raw_global_name"]

#: Attribute on the ``multi_oms`` package holding the wired runtime across re-execs.
_RUNTIME_ATTR = "_MULTIOMS_RUNTIME"

#: Module-level idempotence flag (doc 04 section 9.1 -- also keeps strong refs alive).
_STARTED = False


def raw_global_name(hub_name: str) -> str:
    """Global under which one hub's raw blink table is exported.

    ``"OMS-B-parent"`` -> ``"oms_raw_oms_b_parent"``: sanitized to
    ``[A-Za-z0-9_]`` and lowercased, so the name is a legal python identifier
    whatever the topology calls its hubs.
    """
    return "oms_raw_" + sanitize_hub(hub_name).lower()


class Runtime:
    """Everything the wiring produced; kept alive by a module-level global.

    Deliberately a plain class rather than a ``dataclass``: Application Mode may
    exec this file with a ``__name__`` that is not registered in ``sys.modules``,
    which breaks ``dataclasses``' type introspection at class-creation time.
    """

    def __init__(
        self,
        topology: Any,
        raw: Dict[str, Any],
        pipeline: MultiOmsPipeline,
        streams: Optional[Dict[str, Any]] = None,
        tables: Optional[Dict[str, Any]] = None,
        api: Optional[Dict[str, Callable[..., Any]]] = None,
        dashboard: Optional[Any] = None,
    ) -> None:
        """Store the wired objects (also the strong references keeping them alive)."""
        self.topology = topology
        self.raw: Dict[str, Any] = raw
        self.pipeline = pipeline
        self.streams: Dict[str, Any] = streams or {}
        self.tables: Dict[str, Any] = tables or {}
        self.api: Dict[str, Callable[..., Any]] = api or {}
        self.dashboard = dashboard

    @property
    def table_names(self) -> List[str]:
        """Every global table name this runtime exports, sorted."""
        published = {name for name in self.tables if not name.startswith("_")}
        raw_names = {raw_global_name(name) for name in self.raw}
        return sorted(published | set(self.streams) | raw_names)

    @property
    def notes(self) -> List[str]:
        """Build-time notes from the DAG (which optional API routes were taken)."""
        notes = []
        how = self.tables.get("_chain_summary_how")
        if how:
            notes.append(f"chain_summary via {how}")
        notes.append(
            "orders_tree available" if "orders_tree" in self.tables else "orders_tree UNAVAILABLE"
        )
        return notes


def _wire() -> Runtime:
    """Build the whole multi-hub DAG once.

    Returns:
        The populated :class:`Runtime`.

    Raises:
        ValueError: If the topology or a tuning knob is misconfigured -- a startup
            failure, never a silent fallback (doc 09 section 3).
    """
    topology = config.load_topology()
    raw = ingest.build_all_raw(topology)
    pipeline = MultiOmsPipeline(topology)
    streams = pipeline.start(raw)
    tables = build_derived(topology, streams)
    api = make_query_api(tables)
    dashboard = build_dashboard(topology, tables)
    return Runtime(
        topology=topology,
        raw=raw,
        pipeline=pipeline,
        streams=streams,
        tables=tables,
        api=api,
        dashboard=dashboard,
    )


def main() -> Runtime:
    """Wire the application, or return the already-wired runtime.

    Idempotent by design: Application Mode plus a console ``exec`` of the same file
    must not double-subscribe any hub's listener.

    Returns:
        The :class:`Runtime` holding tables, query API and dashboard.
    """
    global _STARTED

    existing = getattr(multi_oms, _RUNTIME_ATTR, None)
    if existing is not None:
        _STARTED = True
        print("[multi-oms] pipeline already running -- reusing the existing tables")
        return existing

    try:
        runtime = _wire()
    except Exception:  # noqa: BLE001 - surface startup failures in the server log
        print("[multi-oms] FAILED to start the multi-OMS drop-copy blotter:")
        traceback.print_exc()
        raise

    setattr(multi_oms, _RUNTIME_ATTR, runtime)
    _STARTED = True
    return runtime


def export(namespace: MutableMapping[str, Any], runtime: Runtime) -> None:
    """Publish every table, query-API function and the dashboard into ``namespace``.

    Args:
        namespace: Usually ``globals()`` of this module -- Application Mode exports
            those to the web IDE and to ``pydeephaven``.
        runtime: The wired runtime.

    Note:
        Keys of ``runtime.tables`` that start with ``_`` are build-time notes, not
        tables, and are not exported.
    """
    for hub_name, table in runtime.raw.items():
        namespace[raw_global_name(hub_name)] = table
    namespace["multi_oms_pipeline"] = runtime.pipeline
    namespace["multi_oms_topology"] = runtime.topology
    namespace.update(runtime.streams)
    namespace.update(
        {name: table for name, table in runtime.tables.items() if not name.startswith("_")}
    )
    namespace.update(runtime.api)
    namespace["multi_oms_blotter"] = runtime.dashboard


def _print_banner(runtime: Runtime) -> None:
    """Print a concise startup summary to the server log."""
    topology = runtime.topology
    dashboard_status = (
        "multi_oms_blotter"
        if runtime.dashboard
        else "unavailable (deephaven.ui missing) -- use the table panels"
    )
    lines = [
        "=" * 78,
        "Multi-OMS Drop-Copy Blotter -- ready",
        f"  source          : {ingest.source_description(topology)}",
        f"  hubs            : {len(topology)}",
        topology.describe(),
        f"  raw blinks      : {', '.join(raw_global_name(name) for name in topology.names)}",
        f"  tolerances      : qty={config.qty_tolerance()} notional={config.notional_tolerance()}"
        f" page={config.page_size()}",
        f"  tables          : {', '.join(runtime.table_names)}",
        f"  query api       : {', '.join(sorted(runtime.api))}",
        f"  notes           : {'; '.join(runtime.notes)}",
        f"  dashboard       : {dashboard_status}",
        "=" * 78,
    ]
    print("\n".join(lines))


_RUNTIME = main()
export(globals(), _RUNTIME)
_print_banner(_RUNTIME)
