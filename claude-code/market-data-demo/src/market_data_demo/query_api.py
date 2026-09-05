"""Console helpers exported as globals (doc 11 section 9). Server side only.

Each function is a thin wrapper over the reader and :mod:`market_data_demo.derived`, so
what the dashboard shows can be reproduced -- and taken further -- from the IDE console
or from ``pydeephaven``'s ``run_script``::

    md_load("AAPL, MSFT", "2026-08-03", "2026-08-07")          # 1-minute bars
    md_load(["NVDA"], "2026-08-03", "2026-09-04", interval="1h")
    md_daily("AAPL", "2026-08-01", "2026-09-04")               # per-day OHLC + return %
    md_chart("candlestick", "AAPL", "2026-09-01", "2026-09-04") # a figure
    md_symbols(); md_days(); md_refresh()
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Callable, Dict, Iterable, Optional, Union

from market_data_demo.charts import build_charts
from market_data_demo.derived import daily_summary, normalized, resample
from market_data_demo.layout import parse_symbols, to_date

__all__ = ["make_query_api"]

SymbolsLike = Union[None, str, Iterable[str]]


def _period(start: Any, end: Any, inventory: Any) -> tuple:
    first = to_date(start)
    last = to_date(end)
    days = list(inventory.days)
    if first is None:
        first = days[0] if days else dt.date.today()
    if last is None:
        last = days[-1] if days else first
    if last < first:
        first, last = last, first
    return (first, last)


def make_query_api(runtime: Any) -> Dict[str, Callable[..., Any]]:
    """Build the ``md_*`` functions over a wired :class:`~market_data_demo.app.Runtime`."""

    def md_load(symbols: SymbolsLike = None, start: Any = None, end: Any = None, interval: str = "1m") -> Any:
        """Bars for ``symbols`` (default: every symbol) over ``[start, end]`` at ``interval``."""
        first, last = _period(start, end, runtime.inventory)
        names = parse_symbols(symbols) or None
        result = runtime.reader.read(first, last, names)
        if result.errors:
            print("[market-data] " + result.status())
        return resample(result.table, interval)

    def md_daily(symbols: SymbolsLike = None, start: Any = None, end: Any = None) -> Any:
        """Daily OHLC / volume / return-percent summary."""
        return daily_summary(md_load(symbols, start, end))

    def md_normalized(symbols: SymbolsLike = None, start: Any = None, end: Any = None, interval: str = "1m") -> Any:
        """Bars with ``PctChange`` relative to each symbol's first close in the period."""
        return normalized(md_load(symbols, start, end, interval))

    def md_chart(kind: str = "candlestick", symbols: SymbolsLike = None, start: Any = None, end: Any = None,
                 interval: str = "1m", hide_gaps: bool = True) -> Any:
        """One figure (the first symbol for candlestick/ohlc; every symbol otherwise)."""
        first, last = _period(start, end, runtime.inventory)
        names = parse_symbols(symbols) or list(runtime.inventory.symbols[:1])
        bars = md_load(names, first, last, interval)
        norm = normalized(bars) if kind == "normalized" else None
        charts = build_charts(kind, bars, names, interval=interval, hide_gaps=hide_gaps, first_day=first, normalized_table=norm)
        for note in charts.notes:
            print("[market-data] " + note)
        return charts.figures[0][1] if charts.figures else None

    def md_files(symbols: SymbolsLike = None, start: Any = None, end: Any = None) -> Any:
        """The files a query would read, as a table (Day, Symbol, Path)."""
        from deephaven import new_table
        from deephaven.column import string_col

        first, last = _period(start, end, runtime.inventory)
        refs = runtime.store.list_files(first, last, parse_symbols(symbols) or None)
        return new_table(
            [
                string_col("Day", [ref.day.isoformat() for ref in refs]),
                string_col("Symbol", [ref.symbol for ref in refs]),
                string_col("Path", [ref.path for ref in refs]),
            ]
        )

    def md_symbols() -> Any:
        """The inventory per symbol (first/last day, day and file counts)."""
        return runtime.tables["md_inventory_symbols"]

    def md_days() -> Any:
        """The inventory per day (how many symbols each day has)."""
        return runtime.tables["md_inventory_days"]

    def md_refresh() -> Any:
        """Re-scan the store (new files landed), drop the file cache, return the symbol inventory."""
        runtime.refresh()
        return runtime.tables["md_inventory_symbols"]

    def md_status() -> str:
        """Where the data comes from and what the inventory looks like."""
        return runtime.describe()

    return {
        "md_load": md_load,
        "md_daily": md_daily,
        "md_normalized": md_normalized,
        "md_chart": md_chart,
        "md_files": md_files,
        "md_symbols": md_symbols,
        "md_days": md_days,
        "md_refresh": md_refresh,
        "md_status": md_status,
    }
