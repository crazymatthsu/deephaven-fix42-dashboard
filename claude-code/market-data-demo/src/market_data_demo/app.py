"""Entry point: store -> inventory -> reader -> tables -> query API -> dashboard.

Works both in **Application Mode** (``docker/apps/market-data-demo/market-data-demo.app``
pointing at this file through the shared loader) and when exec'd in a Deephaven console.
Everything it builds is published as a module-level global, so it appears in the web
IDE's Panels menu and is reachable from ``pydeephaven``.

Re-running the script is safe: the wired runtime is memoized on the ``market_data_demo``
package object (which lives in ``sys.modules``), so a second execution re-exports the
same objects instead of re-scanning the store.
"""

from __future__ import annotations

import os
import sys

# ---------------------------------------------------------------------------------
# sys.path bootstrap -- must run before importing market_data_demo, because this file
# is executed as a loose script inside the container (/md-scripts/market_data_demo/app.py),
# not as an installed package.
# ---------------------------------------------------------------------------------
try:
    _APP_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:  # pragma: no cover - exec'd from a string, no __file__
    _APP_DIR = "/md-scripts/market_data_demo"
_SRC_DIR = os.path.dirname(_APP_DIR) or "/md-scripts"

for _candidate in (_SRC_DIR, "/md-scripts"):
    if _candidate and os.path.isdir(_candidate) and _candidate not in sys.path:
        sys.path.insert(0, _candidate)

import traceback  # noqa: E402
from typing import Any, Callable, Dict, List, MutableMapping, Optional  # noqa: E402

import market_data_demo  # noqa: E402
from market_data_demo import config as md_config  # noqa: E402
from market_data_demo.dashboard import build_dashboard, initial_symbols, preset_range  # noqa: E402
from market_data_demo.derived import daily_summary  # noqa: E402
from market_data_demo.query_api import make_query_api  # noqa: E402
from market_data_demo.reader import BarReader  # noqa: E402
from market_data_demo.store import summarize_inventory  # noqa: E402

__all__ = ["Runtime", "main", "export"]

#: Attribute on the package holding the wired runtime across re-execs.
_RUNTIME_ATTR = "_MARKET_DATA_RUNTIME"

#: Name of the dashboard global.
DASHBOARD_NAME = "market_data_dashboard"


def _inventory_tables(inventory: Any) -> Dict[str, Any]:
    """The two inventory tables from an :class:`InventorySummary`."""
    from deephaven import new_table
    from deephaven.column import int_col, string_col

    symbols = new_table(
        [
            string_col("Symbol", [row["Symbol"] for row in inventory.symbol_rows]),
            string_col("FirstDay", [row["FirstDay"].isoformat() for row in inventory.symbol_rows]),
            string_col("LastDay", [row["LastDay"].isoformat() for row in inventory.symbol_rows]),
            int_col("Days", [row["Days"] for row in inventory.symbol_rows]),
            int_col("Files", [row["Files"] for row in inventory.symbol_rows]),
        ]
    )
    days = new_table(
        [
            string_col("Day", [row["Day"].isoformat() for row in inventory.day_rows]),
            int_col("Symbols", [row["Symbols"] for row in inventory.day_rows]),
            string_col("SymbolList", [row["SymbolList"] for row in inventory.day_rows]),
        ]
    )
    return {"md_inventory_symbols": symbols, "md_inventory_days": days}


class Runtime:
    """Everything the wiring produced; kept alive by a module-level global.

    A plain class rather than a ``dataclass`` on purpose: Application Mode may exec this
    file with a ``__name__`` that is not registered in ``sys.modules``, which breaks
    ``dataclasses``' type introspection at class-creation time.
    """

    def __init__(self, cfg: Any, store: Any) -> None:
        self.cfg = cfg
        self.store = store
        self.inventory = summarize_inventory([])
        self.reader = BarReader(store, cfg)
        self.tables: Dict[str, Any] = {}
        self.api: Dict[str, Callable[..., Any]] = {}
        self.dashboard: Optional[Any] = None
        self.default_result: Optional[Any] = None

    def scan(self) -> None:
        """(Re)build the inventory from the store."""
        days = self.store.available_days()
        refs = self.store.list_files(days[0], days[-1]) if days else []
        self.inventory = summarize_inventory(refs)
        self.tables.update(_inventory_tables(self.inventory))

    def load_defaults(self) -> None:
        """Pre-load the initial selection so ``md_bars`` / ``md_daily_summary`` exist as globals."""
        from market_data_demo.derived import empty_bars

        symbols = initial_symbols(self.cfg, self.inventory.symbols)
        period = preset_range(self.inventory.days, self.cfg.default_days)
        if symbols and period is not None:
            self.default_result = self.reader.read(period[0], period[1], symbols)
            bars = self.default_result.table
        else:
            self.default_result = None
            bars = empty_bars()
        self.tables["md_bars"] = bars
        self.tables["md_daily_summary"] = daily_summary(bars)

    def refresh(self) -> None:
        """Re-scan the store and drop the file cache (``md_refresh()``)."""
        self.reader.clear_cache()
        self.scan()
        self.load_defaults()

    @property
    def table_names(self) -> List[str]:
        return sorted(self.tables)

    def describe(self) -> str:
        inv = self.inventory
        span = f"{inv.first_day} .. {inv.last_day}" if inv.first_day else "(empty)"
        return (
            f"source={self.store.describe()} symbols={len(inv.symbols)} days={len(inv.days)} "
            f"files={len(inv.refs)} span={span}"
        )


def _wire() -> Runtime:
    """Build the whole runtime once.

    Raises:
        ValueError: On a misconfiguration -- a startup failure, never a silent fallback.
    """
    cfg = md_config.load_config()
    store = md_config.make_store(cfg)
    runtime = Runtime(cfg, store)
    runtime.scan()
    runtime.load_defaults()
    runtime.api = make_query_api(runtime)
    runtime.dashboard = build_dashboard(
        runtime.reader,
        runtime.inventory,
        cfg,
        inventory_symbols_table=runtime.tables["md_inventory_symbols"],
        inventory_days_table=runtime.tables["md_inventory_days"],
    )
    return runtime


def main() -> Runtime:
    """Wire the application, or return the already-wired runtime (idempotent)."""
    existing = getattr(market_data_demo, _RUNTIME_ATTR, None)
    if existing is not None:
        print("[market-data] already wired -- reusing the existing runtime")
        return existing
    try:
        runtime = _wire()
    except Exception:  # noqa: BLE001 - surface startup failures in the server log
        print("[market-data] FAILED to start the market data demo:")
        traceback.print_exc()
        raise
    setattr(market_data_demo, _RUNTIME_ATTR, runtime)
    return runtime


def export(namespace: MutableMapping[str, Any], runtime: Runtime) -> None:
    """Publish the tables, the ``md_*`` functions and the dashboard into ``namespace``."""
    namespace.update(runtime.tables)
    namespace.update(runtime.api)
    namespace["market_data_runtime"] = runtime
    if runtime.dashboard is not None:
        namespace[DASHBOARD_NAME] = runtime.dashboard


def _print_banner(runtime: Runtime) -> None:
    cfg = runtime.cfg
    inv = runtime.inventory
    dashboard_status = DASHBOARD_NAME if runtime.dashboard is not None else "unavailable (deephaven.ui missing) -- use the table panels"
    default_line = runtime.default_result.status() if runtime.default_result is not None else "nothing pre-loaded (empty inventory)"
    lines = [
        "=" * 78,
        "Market Data Demo -- ready",
        f"  source          : {cfg.describe()}",
        f"  inventory       : {len(inv.symbols)} symbols, {len(inv.days)} days, {len(inv.refs)} files"
        + (f" ({inv.first_day} .. {inv.last_day})" if inv.first_day else ""),
        f"  symbols         : {', '.join(inv.symbols[:16])}{' ...' if len(inv.symbols) > 16 else ''}",
        f"  default load    : {default_line}",
        f"  defaults        : interval={cfg.default_interval} chart={cfg.default_chart} days={cfg.default_days} hide_gaps={cfg.hide_gaps}",
        f"  tables          : {', '.join(runtime.table_names)}",
        f"  query api       : {', '.join(sorted(runtime.api))}",
        f"  dashboard       : {dashboard_status}",
        "=" * 78,
    ]
    print("\n".join(lines), flush=True)


_RUNTIME = main()
export(globals(), _RUNTIME)
_print_banner(_RUNTIME)
