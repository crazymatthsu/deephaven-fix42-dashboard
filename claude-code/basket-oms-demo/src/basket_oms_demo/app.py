"""Entry point: config -> core -> table bridge -> seed -> mock venue -> console API -> dashboard.

Works both in **Application Mode** (``docker/apps/basket-oms-demo/basket-oms-demo.app``
pointing at this file through the shared loader) and when exec'd in a Deephaven console.
Everything it builds is published as a module-level global (doc 13 §2, §3), so it
appears in the web IDE's Panels menu and is reachable from ``pydeephaven``.

Re-running the script is safe: the wired runtime is memoised on the ``basket_oms_demo``
package object (which lives in ``sys.modules``), so a second execution re-exports the
same core and tables instead of seeding a second set.
"""

from __future__ import annotations

import os
import sys

# ---------------------------------------------------------------------------------
# sys.path bootstrap -- this file is executed as a loose script inside the container
# (/oms-scripts/basket_oms_demo/app.py), not as an installed package.
# ---------------------------------------------------------------------------------
try:
    _APP_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:  # pragma: no cover - exec'd from a string, no __file__
    _APP_DIR = "/oms-scripts/basket_oms_demo"
_SRC_DIR = os.path.dirname(_APP_DIR) or "/oms-scripts"

for _candidate in (_SRC_DIR, "/oms-scripts"):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import traceback  # noqa: E402
from typing import Any, Callable, Dict, List, MutableMapping, Optional, Sequence, Union  # noqa: E402

import basket_oms_demo  # noqa: E402
from basket_oms_demo.config import Config, load_config  # noqa: E402
from basket_oms_demo.core import OmsCore, OmsError  # noqa: E402
from basket_oms_demo.mockdata import ref_prices, seed_demo  # noqa: E402
from basket_oms_demo.simulator import Simulator  # noqa: E402
from basket_oms_demo.tables import TableBridge  # noqa: E402
from basket_oms_demo.venue import MockVenue  # noqa: E402

__all__ = ["Runtime", "build_runtime", "make_api", "export", "main"]

#: Attribute on the package holding the wired runtime across re-execs.
_RUNTIME_ATTR = "_BASKET_OMS_RUNTIME"

#: Name of the dashboard global (doc 13 §2).
DASHBOARD_NAME = "basket_oms_dashboard"


class Runtime:
    """Everything the wiring produced; kept alive by a module-level global.

    A plain class rather than a ``dataclass`` on purpose: Application Mode may exec this
    file with a ``__name__`` that is not registered in ``sys.modules``, which breaks
    ``dataclasses``' type introspection at class-creation time.
    """

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.bridge = TableBridge()
        self.core = OmsCore(trader=cfg.trader, venues=cfg.venues, listener=self.bridge)
        self.venue = MockVenue(self.core, ref_prices(), seed=cfg.seed + 1)
        self.simulator = Simulator(self.venue, cfg.sim_interval_ms)
        self.tables: Dict[str, Any] = self.bridge.globals()
        self.api: Dict[str, Callable[..., Any]] = {}
        self.dashboard: Optional[Any] = None
        self.dashboard_error: Optional[str] = None

    def seed(self) -> None:
        seed_demo(self.core, self.cfg.mock_baskets, self.cfg.seed)
        self.venue.publish_quotes()

    def start(self) -> None:
        if self.cfg.sim_enabled:
            self.simulator.start()


def build_runtime(cfg: Optional[Config] = None) -> Runtime:
    runtime = Runtime(cfg or load_config())
    runtime.seed()
    runtime.api = make_api(runtime)
    try:
        from basket_oms_demo.dashboard import build_dashboard

        runtime.dashboard = build_dashboard(runtime)
    except Exception as exc:  # noqa: BLE001 - the tables and the API still work without the dashboard
        runtime.dashboard_error = f"{type(exc).__name__}: {exc}"
        print(f"[basket-oms] WARNING: dashboard not built ({runtime.dashboard_error})", flush=True)
        traceback.print_exc()
    runtime.start()
    return runtime


# ---------------------------------------------------------------------------------
# Console API (doc 13 §3)
# ---------------------------------------------------------------------------------


def make_api(runtime: Runtime) -> Dict[str, Callable[..., Any]]:
    core, cfg = runtime.core, runtime.cfg

    def oms_new_basket(
        name: str,
        lines: Union[str, Sequence[str]],
        strategy: str = "DMA",
        trader: Optional[str] = None,
        route: Optional[str] = None,
    ) -> str:
        """Create a basket from ticket lines; ``route=<venue>`` routes it straight away. Returns the BasketId."""
        basket = core.create_basket(name, lines, strategy=strategy, trader=trader)
        if route:
            core.route_basket(basket.basket_id, route, user=trader or cfg.trader)
        print(f"[basket-oms] {basket.basket_id} {basket.name!r}: {len(core.orders_in(basket.basket_id))} orders")
        return basket.basket_id

    def oms_add_orders(basket_id: str, lines: Union[str, Sequence[str]], trader: Optional[str] = None) -> List[str]:
        return [o.order_id for o in core.add_orders(basket_id, lines, trader=trader)]

    def oms_route(order_id: str, venue: str, user: Optional[str] = None) -> str:
        return core.route(order_id, venue, user=user).status

    def oms_route_basket(basket_id: str, venue: str, user: Optional[str] = None) -> List[str]:
        return [o.order_id for o in core.route_basket(basket_id, venue, user=user)]

    def oms_modify(
        order_id: str,
        qty: Optional[int] = None,
        limit_px: Optional[float] = None,
        tif: Optional[str] = None,
        user: Optional[str] = None,
    ) -> str:
        return core.modify(order_id, qty=qty, limit_px=limit_px, tif=tif, user=user).note

    def oms_cancel(order_id: str, user: Optional[str] = None) -> str:
        return core.cancel(order_id, user=user).status

    def oms_cancel_basket(basket_id: str, user: Optional[str] = None) -> List[str]:
        return [o.order_id for o in core.cancel_basket(basket_id, user=user)]

    def oms_split(
        order_id: str,
        slices: Optional[int] = None,
        qtys: Optional[Sequence[int]] = None,
        user: Optional[str] = None,
    ) -> List[str]:
        return [o.order_id for o in core.split(order_id, slices=slices, qtys=qtys, user=user)]

    def oms_sim(enabled: Optional[Union[bool, str]] = None) -> Any:
        """``True`` / ``False`` starts / stops the mock venue; ``"step"`` runs one step; ``None`` reports."""
        sim = runtime.simulator
        if enabled is None:
            return {"running": sim.running, "steps": sim.steps, "errors": sim.errors, "interval_ms": int(sim.interval * 1000)}
        if isinstance(enabled, str) and enabled.lower() == "step":
            report = sim.step_once()
            return None if report is None else {"acks": report.acks, "fills": report.fills, "rejects": report.rejects, "cancels": report.cancels}
        if enabled:
            sim.start()
        else:
            sim.stop()
        return sim.running

    def oms_status() -> Dict[str, Any]:
        counts = core.counts()
        sim = runtime.simulator
        status = {
            **counts,
            "sim_running": sim.running,
            "sim_steps": sim.steps,
            "sim_errors": sim.errors,
            "venues": list(core.venues),
            "trader": cfg.trader,
            "dashboard": DASHBOARD_NAME if runtime.dashboard is not None else f"not built: {runtime.dashboard_error}",
        }
        for key, value in status.items():
            print(f"  {key:<18} {value}")
        return status

    return {
        "oms_new_basket": oms_new_basket,
        "oms_add_orders": oms_add_orders,
        "oms_route": oms_route,
        "oms_route_basket": oms_route_basket,
        "oms_modify": oms_modify,
        "oms_cancel": oms_cancel,
        "oms_cancel_basket": oms_cancel_basket,
        "oms_split": oms_split,
        "oms_sim": oms_sim,
        "oms_status": oms_status,
    }


# ---------------------------------------------------------------------------------
# Export + main
# ---------------------------------------------------------------------------------


def export(namespace: MutableMapping[str, Any], runtime: Runtime) -> List[str]:
    """Publish the tables, the API and the dashboard into ``namespace``; returns the names."""
    names: List[str] = []
    for name, table in runtime.tables.items():
        namespace[name] = table
        names.append(name)
    for name, func in runtime.api.items():
        namespace[name] = func
        names.append(name)
    if runtime.dashboard is not None:
        namespace[DASHBOARD_NAME] = runtime.dashboard
        names.append(DASHBOARD_NAME)
    namespace["oms_runtime"] = runtime
    namespace["OmsError"] = OmsError
    return names


def main(namespace: Optional[MutableMapping[str, Any]] = None) -> Runtime:
    runtime: Optional[Runtime] = getattr(basket_oms_demo, _RUNTIME_ATTR, None)
    if runtime is None:
        runtime = build_runtime()
        setattr(basket_oms_demo, _RUNTIME_ATTR, runtime)
        print(
            f"[basket-oms] seeded {runtime.core.counts()['baskets']} baskets / "
            f"{runtime.core.counts()['orders']} orders; mock venue "
            f"{'running' if runtime.simulator.running else 'stopped'} "
            f"(every {runtime.cfg.sim_interval_ms} ms); trader {runtime.cfg.trader}",
            flush=True,
        )
    else:
        print("[basket-oms] runtime already wired; re-exporting", flush=True)
    target = globals() if namespace is None else namespace
    names = export(target, runtime)
    print(f"[basket-oms] exported: {', '.join(names)}", flush=True)
    return runtime


if __name__ == "__main__":
    main(globals())
