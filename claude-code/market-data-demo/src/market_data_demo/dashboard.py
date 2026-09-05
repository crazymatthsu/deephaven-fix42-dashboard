"""The ``deephaven.ui`` dashboard (doc 11 section 8).

::

    +------------------------------------------------------+---------------------------+
    | Symbols [list]  Period [range picker] 1D 5D 1M 3M All | Available symbols (table) |
    | Interval v  Chart v  [x] hide gaps   status line      |                           |
    +------------------------------------------------------+---------------------------+
    | Chart -- candlestick/OHLC: one tab per symbol; line/area/normalized/volume: one   |
    +----------------------------------------------+---------------------------------+
    | Bars (resampled)                             | Daily summary (click -> that day)|
    +----------------------------------------------+---------------------------------+

State is four scalars -- the symbol tuple, the period, the interval, the chart type --
and everything else is ``ui.use_memo`` over them: the reader loads the files for
``(symbols, period)``, resampling depends on ``interval`` as well, and the figures on
the chart type too. Changing the chart type therefore re-plots without re-reading.

Version tolerance, exactly like the other apps: ``deephaven.ui`` is imported lazily and
:func:`build_dashboard` returns ``None`` when it is missing; every optional control is
built inside :func:`_safe` / :func:`_first`, so a widget this plugin version rejects
degrades to a plainer one instead of costing the dashboard. The pure helpers at the top
(period presets, selection coercion) are unit-tested on the host.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from market_data_demo.charts import CHART_TYPES, PER_SYMBOL_CHARTS, build_charts
from market_data_demo.config import Config
from market_data_demo.derived import INTERVALS, daily_summary, normalized, resample
from market_data_demo.layout import parse_symbols, to_date

__all__ = [
    "PRESETS",
    "preset_range",
    "coerce_selection",
    "coerce_range",
    "initial_symbols",
    "build_dashboard",
]

#: Quick-period buttons: label -> number of most recent *available* days (None = all).
PRESETS: Tuple[Tuple[str, Optional[int]], ...] = (
    ("1D", 1),
    ("5D", 5),
    ("1M", 21),
    ("3M", 63),
    ("All", None),
)

_ROW_KWARGS = ("row", "row_data", "data", "item")
_CELL_KEYS = ("value", "text", "raw_value")


# --------------------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------------------


def preset_range(available: Sequence[dt.date], count: Optional[int]) -> Optional[Tuple[dt.date, dt.date]]:
    """The last ``count`` available days (``None`` = every day) as ``(start, end)``."""
    days = sorted(available)
    if not days:
        return None
    if count is None or count >= len(days):
        return (days[0], days[-1])
    return (days[-max(1, count)], days[-1])


def coerce_selection(value: Any, universe: Sequence[str]) -> List[str]:
    """Normalize what a multi-select control hands back into an ordered symbol list.

    ``"all"`` (list_view's select-all token) means the whole universe; a string is
    parsed as a comma list; any iterable of keys is filtered to known symbols so a stale
    key from a previous inventory cannot survive.
    """
    known = list(universe)
    if value is None:
        return []
    if isinstance(value, str):
        if value.strip().lower() == "all":
            return known
        picked = parse_symbols(value)
    else:
        try:
            picked = parse_symbols([str(item) for item in value])
        except (TypeError, ValueError):
            return []
    if not known:
        return picked
    return [symbol for symbol in known if symbol in picked]


def coerce_range(value: Any) -> Optional[Tuple[dt.date, dt.date]]:
    """``{"start": ..., "end": ...}`` (or an object with those attributes) -> ``(start, end)``."""
    if value is None:
        return None
    if isinstance(value, Mapping):
        start, end = value.get("start"), value.get("end")
    elif isinstance(value, (tuple, list)) and len(value) == 2:
        start, end = value
    else:
        start, end = getattr(value, "start", None), getattr(value, "end", None)
    first, last = to_date(start), to_date(end)
    if first is None or last is None:
        return None
    if last < first:
        first, last = last, first
    return (first, last)


def initial_symbols(cfg: Config, universe: Sequence[str], fallback: int = 3) -> List[str]:
    """The initial selection: ``MD_DEFAULT_SYMBOLS`` that exist, else the first few."""
    configured = [symbol for symbol in cfg.default_symbols if symbol in universe]
    if configured:
        return configured
    return list(universe[:fallback])


def _extract_row(args: Sequence[Any], kwargs: Mapping[str, Any]) -> Optional[Any]:
    for name in _ROW_KWARGS:
        candidate = kwargs.get(name)
        if candidate is not None:
            return candidate
    positional = list(args)
    for candidate in reversed(positional):
        if isinstance(candidate, Mapping):
            return candidate
    for candidate in reversed(positional):
        if candidate is not None and not isinstance(candidate, (bool, int, float, str, bytes)):
            return candidate
    return None


def _lookup(container: Any, key: str) -> Any:
    if container is None:
        return None
    if isinstance(container, Mapping):
        return container.get(key)
    try:
        return container[key]
    except Exception:  # noqa: BLE001
        return getattr(container, key, None)


def _cell_value(row: Any, column: str) -> Any:
    cell = _lookup(row, column)
    if cell is None:
        return None
    if isinstance(cell, (str, bytes, int, float)):
        return cell.decode() if isinstance(cell, bytes) else cell
    for key in _CELL_KEYS:
        nested = _lookup(cell, key)
        if nested is not None:
            return nested
    return cell


def _safe(factory: Callable[[], Any]) -> Optional[Any]:
    try:
        return factory()
    except Exception:  # noqa: BLE001 - optional garnish only
        return None


def _first(*factories: Callable[[], Any]) -> Optional[Any]:
    for factory in factories:
        built = _safe(factory)
        if built is not None:
            return built
    return None


# --------------------------------------------------------------------------------------
# The dashboard
# --------------------------------------------------------------------------------------


def build_dashboard(
    reader: Any,
    inventory: Any,
    cfg: Config,
    inventory_symbols_table: Optional[Any] = None,
    inventory_days_table: Optional[Any] = None,
) -> Optional[Any]:
    """Build the market-data dashboard.

    Args:
        reader: A :class:`~market_data_demo.reader.BarReader`.
        inventory: A :class:`~market_data_demo.store.InventorySummary` -- the symbol
            universe and the available days the controls are built from.
        cfg: The configuration (initial selection, interval, chart type, gap hiding).
        inventory_symbols_table: Optional table shown in the "Available symbols" panel.
        inventory_days_table: Optional table shown in the "Available days" panel.

    Returns:
        A ``ui.dashboard`` element, or ``None`` without ``deephaven.ui``.
    """
    try:
        import deephaven.ui as ui
    except ImportError:  # pragma: no cover - server without the ui plugin
        print("[market-data] deephaven.ui not available; use the table panels and the md_* functions")
        return None

    universe: List[str] = list(inventory.symbols)
    available_days: List[dt.date] = list(inventory.days)
    default_symbols = initial_symbols(cfg, universe)
    default_range = preset_range(available_days, cfg.default_days)
    interval_keys = list(INTERVALS)
    chart_keys = list(CHART_TYPES)

    @ui.component
    def market_data_component() -> Any:
        symbols, set_symbols = ui.use_state(tuple(default_symbols))
        period, set_period = ui.use_state(default_range)
        interval, set_interval = ui.use_state(cfg.default_interval)
        chart_kind, set_chart_kind = ui.use_state(cfg.default_chart)
        hide_gaps, set_hide_gaps = ui.use_state(bool(cfg.hide_gaps))
        typed, set_typed = ui.use_state("")
        reload_token, set_reload_token = ui.use_state(0)

        start: Optional[dt.date] = period[0] if period else None
        end: Optional[dt.date] = period[1] if period else None
        symbol_list = list(symbols)
        load_key = (
            tuple(symbol_list),
            start.isoformat() if start else "",
            end.isoformat() if end else "",
            reload_token,
        )

        def do_load() -> Any:
            if start is None or end is None or not symbol_list:
                from market_data_demo.reader import LoadResult
                from market_data_demo.derived import empty_bars

                return LoadResult(table=empty_bars(), start=start, end=end, symbols=symbol_list)
            return reader.read(start, end, symbol_list)

        result = ui.use_memo(do_load, [load_key])
        bars = result.table
        resampled = ui.use_memo(lambda: _guard(lambda: resample(bars, interval), bars), [load_key, interval])
        summary = ui.use_memo(lambda: _guard(lambda: daily_summary(bars), bars.head(0)), [load_key])
        norm_table = ui.use_memo(
            lambda: _guard(lambda: normalized(resampled), None) if chart_kind == "normalized" else None,
            [load_key, interval, chart_kind],
        )
        charts = ui.use_memo(
            lambda: _guard(
                lambda: build_charts(
                    chart_kind,
                    resampled,
                    symbol_list,
                    interval=interval,
                    hide_gaps=hide_gaps,
                    first_day=start,
                    normalized_table=norm_table,
                ),
                None,
            ),
            [load_key, interval, chart_kind, hide_gaps],
        )

        # -- handlers --------------------------------------------------------------
        def on_symbols(value: Any) -> None:
            set_symbols(tuple(coerce_selection(value, universe)))

        def on_typed_submit(*_: Any) -> None:
            names = parse_symbols(typed)
            if names:
                merged = list(symbols) + [name for name in names if name not in symbols]
                set_symbols(tuple(merged))
            set_typed("")

        def on_range(value: Any) -> None:
            coerced = coerce_range(value)
            if coerced is not None:
                set_period(coerced)

        def apply_preset(count: Optional[int]) -> Callable[[], None]:
            def apply() -> None:
                chosen = preset_range(available_days, count)
                if chosen is not None:
                    set_period(chosen)

            return apply

        def on_summary_row(*args: Any, **kwargs: Any) -> None:
            try:
                row = _extract_row(args, kwargs)
                day = to_date(_cell_value(row, "TradeDate"))
            except Exception:  # noqa: BLE001 - a UI callback must never raise
                return
            if day is not None:
                set_period((day, day))

        def on_interval(value: Any) -> None:
            if value in INTERVALS:
                set_interval(str(value))

        def on_chart(value: Any) -> None:
            if value in CHART_TYPES:
                set_chart_kind(str(value))

        # -- controls --------------------------------------------------------------
        symbol_picker = _first(
            lambda: ui.list_view(
                *[ui.item(symbol, key=symbol) for symbol in universe],
                selection_mode="MULTIPLE",
                selected_keys=list(symbols),
                on_change=on_symbols,
                aria_label="Symbols",
                height="size-1600",
                min_width="size-1600",
            ),
            lambda: ui.checkbox_group(
                *[ui.checkbox(symbol, value=symbol) for symbol in universe],
                label="Symbols",
                value=list(symbols),
                on_change=on_symbols,
                orientation="horizontal",
            ),
        )
        typed_box = _first(
            lambda: ui.flex(
                ui.text_field(
                    label="Add symbols",
                    value=typed,
                    on_change=set_typed,
                    description="comma separated",
                    width="size-2000",
                ),
                ui.button("Add", on_press=on_typed_submit),
                direction="row",
                gap="size-100",
                align_items="end",
            ),
            lambda: ui.flex(
                ui.text_field(label="Add symbols", value=typed, on_change=set_typed),
                ui.button("Add", on_press=on_typed_submit),
                direction="row",
            ),
        )
        range_value = (
            {"start": start.isoformat(), "end": end.isoformat()} if start and end else None
        )
        # ISO date strings (no time part) make the picker day-granular by themselves.
        range_picker = _first(
            lambda: ui.date_range_picker(
                label="Period",
                value=range_value,
                on_change=on_range,
                min_value=available_days[0].isoformat() if available_days else None,
                max_value=available_days[-1].isoformat() if available_days else None,
            ),
            lambda: ui.date_range_picker(label="Period", value=range_value, on_change=on_range),
            lambda: ui.flex(
                ui.date_picker(label="From", value=start.isoformat() if start else None,
                               on_change=lambda v: on_range({"start": v, "end": end})),
                ui.date_picker(label="To", value=end.isoformat() if end else None,
                               on_change=lambda v: on_range({"start": start, "end": v})),
                direction="row", gap="size-100",
            ),
        )
        preset_buttons = [
            _first(
                lambda label=label, count=count: ui.button(
                    label, on_press=apply_preset(count), variant="secondary", style="outline"
                ),
                lambda label=label, count=count: ui.button(label, on_press=apply_preset(count)),
            )
            for label, count in PRESETS
        ]
        interval_picker = _first(
            lambda: ui.picker(
                *[ui.item(key, key=key) for key in interval_keys],
                label="Bar interval",
                selected_key=interval,
                on_change=on_interval,
            ),
            lambda: ui.picker(*interval_keys, label="Bar interval", selected_key=interval, on_change=on_interval),
        )
        chart_picker = _first(
            lambda: ui.picker(
                *[ui.item(CHART_TYPES[key], key=key) for key in chart_keys],
                label="Chart",
                selected_key=chart_kind,
                on_change=on_chart,
            ),
            lambda: ui.radio_group(
                *[ui.radio(CHART_TYPES[key], value=key) for key in chart_keys],
                label="Chart",
                value=chart_kind,
                on_change=on_chart,
                orientation="horizontal",
            ),
        )
        gaps_box = _safe(lambda: ui.checkbox("hide gaps", is_selected=hide_gaps, on_change=lambda v: set_hide_gaps(bool(v))))
        reload_button = _safe(lambda: ui.button("Reload", on_press=lambda: set_reload_token(reload_token + 1)))
        clear_button = _safe(lambda: ui.button("Clear symbols", on_press=lambda: set_symbols(())))

        status_lines = [result.status()]
        if charts is not None and charts.notes:
            status_lines.append("chart notes: " + " | ".join(charts.notes))

        row_one = [element for element in (symbol_picker, typed_box, range_picker) if element is not None]
        presets = [element for element in preset_buttons if element is not None]
        row_two = [element for element in (interval_picker, chart_picker, gaps_box, reload_button, clear_button) if element is not None]

        controls = ui.flex(
            ui.flex(*row_one, direction="row", gap="size-150", wrap=True, align_items="end"),
            ui.flex(*presets, direction="row", gap="size-75", align_items="center"),
            ui.flex(*row_two, direction="row", gap="size-150", wrap=True, align_items="end"),
            *[ui.text(line) for line in status_lines],
            direction="column",
            gap="size-100",
        )

        # -- chart panel -------------------------------------------------------------
        chart_body = _chart_body(ui, charts, chart_kind, symbol_list)

        # -- layout ------------------------------------------------------------------
        top_panels = [ui.panel(controls, title="Market data -- controls")]
        if inventory_symbols_table is not None:
            top_panels.append(ui.panel(ui.table(inventory_symbols_table), title="Available symbols"))
        if inventory_days_table is not None:
            top_panels.append(ui.panel(ui.table(inventory_days_table), title="Available days"))

        summary_table = _first(
            lambda: ui.table(summary, on_row_press=on_summary_row, always_fetch_columns=["TradeDate"]),
            lambda: ui.table(summary, on_row_press=on_summary_row),
            lambda: ui.table(summary),
        )

        return ui.column(
            ui.row(*top_panels, height=30),
            ui.row(ui.panel(chart_body, title="Chart"), height=42),
            ui.row(
                ui.panel(ui.table(resampled), title=f"Bars ({interval})"),
                ui.panel(summary_table, title="Daily summary (click a row to zoom to that day)"),
                height=28,
            ),
        )

    return ui.dashboard(market_data_component())


def _guard(factory: Callable[[], Any], fallback: Any) -> Any:
    """Run a table/figure builder, returning ``fallback`` instead of raising."""
    try:
        return factory()
    except Exception as exc:  # noqa: BLE001 - render the fallback, log the cause
        print(f"[market-data] {type(exc).__name__}: {exc}")
        return fallback


def _chart_body(ui: Any, charts: Any, chart_kind: str, symbol_list: Sequence[str]) -> Any:
    """Lay the figures out: tabs per symbol for the financial plots, one figure otherwise."""
    if not symbol_list:
        return ui.text("Select at least one symbol.")
    if charts is None or charts.is_empty:
        note = "; ".join(charts.notes) if charts is not None and charts.notes else "no data for this selection"
        return ui.text(f"Nothing to plot: {note}")
    figures = charts.figures
    if len(figures) == 1:
        return figures[0][1]
    if chart_kind in PER_SYMBOL_CHARTS:
        tabbed = _first(
            lambda: ui.tabs(*[ui.tab(figure, title=title, key=title) for title, figure in figures]),
            lambda: ui.tabs(
                ui.tab_list(*[ui.item(title, key=title) for title, _ in figures]),
                ui.tab_panels(*[ui.item(figure, key=title) for title, figure in figures]),
            ),
        )
        if tabbed is not None:
            return tabbed
    return ui.flex(*[figure for _, figure in figures], direction="column", gap="size-100")
