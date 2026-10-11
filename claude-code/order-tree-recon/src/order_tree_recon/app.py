"""Entry point: source tables -> per-level tables -> order-tree DAG -> query API -> dashboard.

Works in **Application Mode** (``docker/apps/order-tree-recon/`` points at this file
through the shared loader) and when exec'd in a Deephaven console. Everything it builds
is published as a module-level global, so it shows up in the IDE's Panels menu and is
reachable from ``pydeephaven``.

Two sources (``OTR_SOURCE``):

* ``mock`` (default) -- generated native Raptor / algo / router tables and the adapters
  in :mod:`order_tree_recon.sources`. To run over your own order *tables*, replace the
  native tables and edit the adapters (doc 14 section 3).
* ``amps`` -- every system's order states as FIX messages from its AMPS SOW topics
  (``sow_and_subscribe``; :mod:`order_tree_recon.amps_sow`), mapped by a JSON tag config
  (:mod:`order_tree_recon.fixstate`; doc 14 section 12).
* ``fix-files`` -- the same FIX order-state messages from files in ``OTR_FIX_DIR`` (SOW
  dumps or recorded subscriptions; the demo reads ``fix-samples/``).

For both FIX sources each system's SOW mirror -- its current records, one row per order,
with a ``T<tag>`` column per mapped tag -- is published as ``fix_<system>_sow``.

Either way the DAG, the API and the dashboard are unchanged.

Re-running the script is safe: the built globals are memoised on the package object
(which lives in ``sys.modules``), so a second execution re-exports the same tables.
"""

from __future__ import annotations

import os
import sys

try:
    _APP_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:  # pragma: no cover - exec'd from a string, no __file__
    _APP_DIR = "/otr-scripts/order_tree_recon"
_SRC_DIR = os.path.dirname(_APP_DIR) or "/otr-scripts"
for _candidate in (_SRC_DIR, "/otr-scripts"):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import traceback  # noqa: E402
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple  # noqa: E402

import order_tree_recon  # noqa: E402
from order_tree_recon import config, dag, mockdata, query_api, sources  # noqa: E402

__all__ = ["DASHBOARD_NAME", "build", "main"]

DASHBOARD_NAME = "order_tree_dashboard"
_RUNTIME_ATTR = "_ORDER_TREE_GLOBALS"


def _mock_source(topology: config.Topology, tol: float,
                 environ: Optional[Mapping[str, str]]) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    data = mockdata.generate(config.seed(environ), config.mock_families(environ))
    native = sources.native_tables(data)
    level_tables = sources.adapters(native["raptor_orders"], native["algo_orders"], native["router_orders"])
    tables = dag.build_order_tree(topology, level_tables, tol)
    banner = [
        f"mock: seed={config.seed(environ)} families={config.mock_families(environ)} -> "
        f"{len(data.raptor)} raptor / {len(data.algo)} algo / {len(data.router)} router orders"
    ]
    return native, tables, banner


def _fix_tables(topology: config.Topology, tol: float, fix_config: Any,
                events: Mapping[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    from order_tree_recon import fixstate_dag

    level_tables, extra_ids, current = fixstate_dag.fix_level_tables(topology, fix_config, events)
    tables = dag.build_order_tree(topology, level_tables, tol, extra_ids=extra_ids)
    return {f"fix_{system.lower()}_sow": table for system, table in current.items()}, tables


def _fix_files_source(topology: config.Topology, tol: float,
                      environ: Optional[Mapping[str, str]]) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    from order_tree_recon import fixstate, fixstate_dag

    directory = config.fix_dir(environ)
    config_path = config.fix_config_path(environ)
    fix_config = fixstate.load_fix_config(config_path, topology)
    events, files = fixstate_dag.file_event_tables(fix_config, directory)
    published, tables = _fix_tables(topology, tol, fix_config, events)
    banner = [f"fix-files: {directory}, mapping {config_path or '(built-in DEFAULT_FIX_CONFIG)'}"]
    for system in fix_config.systems:
        names = ", ".join(os.path.basename(f) for f in files[system.system]) or "NO FILES MATCHED"
        banner.append(f"  {system.system:<7} {list(fix_config.files[system.system])} -> {names}; "
                      f"{events[system.system].size} messages")
    if not any(files.values()):
        banner.append(f"WARNING: no file in {directory} matches any 'files' glob -- every table is empty")
    return published, tables, banner


def _amps_source(topology: config.Topology, tol: float,
                 environ: Optional[Mapping[str, str]]) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    from order_tree_recon import amps_sow, fixstate

    config_path = config.fix_config_path(environ)
    fix_config = fixstate.load_fix_config(config_path, topology)
    settings = amps_sow.AmpsSettings.from_env(environ)
    source = amps_sow.start_source(fix_config, settings)  # tables now; AMPS connects in the background
    published, tables = _fix_tables(topology, tol, fix_config, source.tables)
    published["amps_source"] = source
    banner = [f"amps: {settings.describe()}; mapping {config_path or '(built-in DEFAULT_FIX_CONFIG: demo topic names)'}"]
    for system in fix_config.systems:
        topics = ", ".join(fix_config.topics[system.system]) or "NO TOPICS CONFIGURED"
        filter_ = fix_config.filters.get(system.system)
        banner.append(f"  {system.system:<7} sow_and_subscribe {topics}" + (f" filter {filter_}" if filter_ else ""))
    banner.append("tables fill as each snapshot arrives; amps_source.status() shows epochs and counts")
    return published, tables, banner


def build(environ: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    """Build every global: the source tables, ``otr_*`` tables, the API, the dashboard."""
    topology = config.default_topology()
    tol = config.qty_tolerance(environ)
    kind = config.source(environ)
    if kind == "amps":
        published, tables, banner = _amps_source(topology, tol, environ)
    elif kind == "fix-files":
        published, tables, banner = _fix_files_source(topology, tol, environ)
    else:
        published, tables, banner = _mock_source(topology, tol, environ)
    api = query_api.make_query_api(tables)

    built: Dict[str, Any] = {**published, **tables, **api}
    try:
        from order_tree_recon.dashboard import build_dashboard

        dashboard = build_dashboard(tables)
        if dashboard is not None:
            built[DASHBOARD_NAME] = dashboard
    except Exception as exc:  # noqa: BLE001 - tables and API still work without it
        print(f"[order-tree] WARNING: dashboard not built ({type(exc).__name__}: {exc})", flush=True)
        traceback.print_exc()

    print("[order-tree] topology:", flush=True)
    for line in topology.describe():
        print(f"[order-tree]   {line}", flush=True)
    for line in banner:
        print(f"[order-tree] {line}", flush=True)
    print(f"[order-tree] {tables['otr_nodes'].size} orders; qty tolerance {tol}", flush=True)
    print(f"[order-tree] tree panel: {'otr_tree' if 'otr_tree' in tables else 'UNAVAILABLE'}; "
          f"dashboard: {DASHBOARD_NAME if DASHBOARD_NAME in built else 'UNAVAILABLE'}", flush=True)
    return built


def main(namespace: Optional[MutableMapping[str, Any]] = None) -> Dict[str, Any]:
    built: Optional[Dict[str, Any]] = getattr(order_tree_recon, _RUNTIME_ATTR, None)
    if built is None:
        built = build()
        setattr(order_tree_recon, _RUNTIME_ATTR, built)
    else:
        print("[order-tree] already built; re-exporting", flush=True)
    target = globals() if namespace is None else namespace
    target.update(built)
    print(f"[order-tree] exported: {', '.join(built)}", flush=True)
    return built


if __name__ == "__main__":
    main(globals())
