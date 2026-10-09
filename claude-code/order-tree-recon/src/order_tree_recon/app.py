"""Entry point: mock native tables -> adapters -> order-tree DAG -> query API -> dashboard.

Works in **Application Mode** (``docker/apps/order-tree-recon/`` points at this file
through the shared loader) and when exec'd in a Deephaven console. Everything it builds
is published as a module-level global, so it shows up in the IDE's Panels menu and is
reachable from ``pydeephaven``.

To run it over real tables, replace the three ``native`` tables below with your own and
edit :func:`order_tree_recon.sources.adapters` -- the DAG, the API and the dashboard are
unchanged (doc 14 section 3).

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
from typing import Any, Dict, Mapping, MutableMapping, Optional  # noqa: E402

import order_tree_recon  # noqa: E402
from order_tree_recon import config, dag, mockdata, query_api, sources  # noqa: E402

__all__ = ["DASHBOARD_NAME", "build", "main"]

DASHBOARD_NAME = "order_tree_dashboard"
_RUNTIME_ATTR = "_ORDER_TREE_GLOBALS"


def build(environ: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    """Build every global: native tables, ``otr_*`` tables, the API, the dashboard."""
    topology = config.default_topology()
    tol = config.qty_tolerance(environ)
    data = mockdata.generate(config.seed(environ), config.mock_families(environ))

    native = sources.native_tables(data)
    level_tables = sources.adapters(native["raptor_orders"], native["algo_orders"], native["router_orders"])
    tables = dag.build_order_tree(topology, level_tables, tol)
    api = query_api.make_query_api(tables)

    built: Dict[str, Any] = {**native, **tables, **api}
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
    print(
        f"[order-tree] mock: seed={config.seed(environ)} families={config.mock_families(environ)} -> "
        f"{len(data.raptor)} raptor / {len(data.algo)} algo / {len(data.router)} router orders; "
        f"qty tolerance {tol}",
        flush=True,
    )
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
