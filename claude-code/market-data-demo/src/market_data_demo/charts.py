"""``deephaven.plot.express`` figures for the loaded bars (doc 11 section 7).

The chart registry (:data:`CHART_TYPES`) is pure so the UI and the configuration
validator can import it on the host; :func:`build_charts` needs a Deephaven server.

Candlestick and OHLC figures cannot be split ``by="Symbol"`` in plotly-express 0.20
(there is no ``by`` on the financial plots), so with several symbols selected those two
render **one figure per symbol**; the line/area/normalized/volume charts overlay every
symbol on one figure.
"""

from __future__ import annotations

import datetime as dt
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

from market_data_demo.mockgen import us_eastern_offset

__all__ = [
    "CHART_TYPES",
    "PER_SYMBOL_CHARTS",
    "ChartSet",
    "gap_rangebreaks",
    "build_charts",
]

#: Chart type key -> label shown in the UI, in display order.
CHART_TYPES: "OrderedDict[str, str]" = OrderedDict(
    [
        ("candlestick", "Candlestick"),
        ("ohlc", "OHLC bars"),
        ("line", "Line (close)"),
        ("area", "Area (close)"),
        ("normalized", "Normalized % change"),
        ("volume", "Volume"),
    ]
)

#: Chart types that render one figure per symbol.
PER_SYMBOL_CHARTS = ("candlestick", "ohlc")


@dataclass
class ChartSet:
    """What :func:`build_charts` produced: ``(title, figure)`` pairs plus any notes."""

    kind: str
    figures: List[Any] = field(default_factory=list)  # list of (title, figure)
    notes: List[str] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.figures


def gap_rangebreaks(first_day: Optional[dt.date]) -> List[Dict[str, Any]]:
    """Plotly ``rangebreaks`` hiding weekends and the overnight gap.

    Times are plotted in UTC, so the session bounds are the New York session shifted by
    that day's UTC offset: 13:30-20:00Z in daylight time, 14:30-21:00Z otherwise. One
    offset is used for the whole range -- a period straddling a DST change shows a
    one-hour sliver at one end, which is the accepted price of a pure client-side hide.
    """
    offset_hours = -us_eastern_offset(first_day or dt.date.today()).total_seconds() / 3600.0
    session_open = 9.5 + offset_hours  # 13.5 or 14.5
    session_close = 16.0 + offset_hours  # 20.0 or 21.0
    return [
        {"bounds": ["sat", "mon"]},
        {"bounds": [session_close, session_open], "pattern": "hour"},
    ]


def _title(kind: str, symbols: Sequence[str], interval: str) -> str:
    label = CHART_TYPES.get(kind, kind)
    names = ", ".join(symbols) if symbols else "(no symbols)"
    return f"{label} -- {names} ({interval})"


def _figure_callback(
    first_day: Optional[dt.date], hide_gaps: bool, title: Optional[str] = None
) -> Optional[Callable[[Any], None]]:
    """The ``unsafe_update_figure`` hook: x-axis rangebreaks and/or a layout title.

    The financial plots (``dx.candlestick`` / ``dx.ohlc``) take no ``title`` argument in
    plotly-express 0.20, so their title is set here on the plotly figure instead.
    """
    breaks = gap_rangebreaks(first_day) if hide_gaps else None
    if breaks is None and title is None:
        return None

    def apply(figure: Any) -> None:
        if breaks is not None:
            figure.update_xaxes(rangebreaks=breaks)
        if title is not None:
            figure.update_layout(title=title)

    return apply


def build_charts(
    kind: str,
    bars: Any,
    symbols: Sequence[str],
    interval: str = "1m",
    hide_gaps: bool = True,
    first_day: Optional[dt.date] = None,
    normalized_table: Optional[Any] = None,
) -> ChartSet:
    """Build the figures for ``kind`` over ``bars`` (already resampled to ``interval``).

    Args:
        kind: A key of :data:`CHART_TYPES`.
        bars: The (resampled) bars table.
        symbols: Selected symbols, in display order.
        interval: For titles only.
        hide_gaps: Hide overnight/weekend gaps on the x axis.
        first_day: First day of the period (picks the DST offset for the gap bounds).
        normalized_table: Required for ``kind == "normalized"`` -- see
            :func:`market_data_demo.derived.normalized`.

    Every figure is built defensively: a plotting option this plugin version rejects is
    dropped (and noted) rather than costing the chart.
    """
    import deephaven.plot.express as dx

    if kind not in CHART_TYPES:
        raise ValueError(f"unknown chart type {kind!r}: expected one of {', '.join(CHART_TYPES)}")
    result = ChartSet(kind=kind)
    names = [str(s).upper() for s in symbols]

    def attempt(factory: Callable[[Dict[str, Any]], Any], extras: Dict[str, Any], label: str) -> Optional[Any]:
        """Call ``factory`` with ``extras`` and retry without them if rejected."""
        try:
            return factory(extras)
        except Exception as exc:  # noqa: BLE001 - fall back to a plainer figure
            if extras:
                result.notes.append(f"{label}: retried without {sorted(extras)} ({type(exc).__name__})")
                try:
                    return factory({})
                except Exception as inner:  # noqa: BLE001
                    result.notes.append(f"{label}: failed ({type(inner).__name__}: {inner})")
                    return None
            result.notes.append(f"{label}: failed ({type(exc).__name__}: {exc})")
            return None

    def hook_kwargs(title: Optional[str] = None) -> Dict[str, Any]:
        hook = _figure_callback(first_day, hide_gaps, title)
        return {"unsafe_update_figure": hook} if hook is not None else {}

    if kind in PER_SYMBOL_CHARTS:
        plotter = dx.candlestick if kind == "candlestick" else dx.ohlc
        for symbol in names:
            subset = bars.where(f"Symbol == `{symbol}`")

            def factory(extras: Dict[str, Any], subset: Any = subset) -> Any:
                return plotter(subset, x="Timestamp", open="Open", high="High", low="Low", close="Close", **extras)

            # No `title=` on the financial plots: it rides along in the figure hook.
            figure = attempt(factory, hook_kwargs(f"{symbol} ({interval})"), f"{kind} {symbol}")
            if figure is not None:
                result.figures.append((symbol, figure))
        return result

    title = _title(kind, names, interval)
    gap_kwargs = hook_kwargs()
    if kind == "line":
        factory = lambda extras: dx.line(bars, x="Timestamp", y="Close", by="Symbol", title=title, **extras)  # noqa: E731
    elif kind == "area":
        factory = lambda extras: dx.area(bars, x="Timestamp", y="Close", by="Symbol", title=title, **extras)  # noqa: E731
    elif kind == "normalized":
        if normalized_table is None:
            raise ValueError("normalized chart needs normalized_table")
        factory = lambda extras: dx.line(  # noqa: E731
            normalized_table, x="Timestamp", y="PctChange", by="Symbol", title=title, **extras
        )
    else:  # volume
        factory = lambda extras: dx.bar(bars, x="Timestamp", y="Volume", by="Symbol", title=title, **extras)  # noqa: E731

    figure = attempt(factory, gap_kwargs, kind)
    if figure is not None:
        result.figures.append((title, figure))
    return result
