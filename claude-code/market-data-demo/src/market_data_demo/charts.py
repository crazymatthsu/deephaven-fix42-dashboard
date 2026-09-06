"""``deephaven.plot.express`` figures for the loaded bars (doc 11 section 7).

The chart registry (:data:`CHART_TYPES`) is pure so the UI and the configuration
validator can import it on the host; :func:`build_charts` needs a Deephaven server.

Candlestick and OHLC figures cannot be split ``by="Symbol"`` in plotly-express 0.20
(there is no ``by`` on the financial plots), so with several symbols selected those two
render **one figure per symbol**; the line/area/normalized/volume charts overlay every
symbol on one figure.

**Hiding the gaps.** Weekends and the overnight hours are cut out of the x axis with a
Deephaven *business calendar* (``calendar=`` on the express plots), not with hand-written
plotly ``rangebreaks``. Two things make the calendar the only correct mechanism (both
verified on server 42.4 / plotly-express 0.20):

* the web plugin turns the calendar into rangebreaks in whatever time zone the viewer
  displays (the IDE defaults to ``America/New_York``, not UTC) -- fixed UTC hour bounds
  hid the morning half of every session instead of the night;
* it forces ``render_mode="svg"``: plotly ignores rangebreaks on WebGL traces, which is
  ``dx.line``'s default, and the line/area/normalized figures came up empty.

The mock data models no exchange holidays, so by default the figures use
:data:`DEMO_CALENDAR` (NYSE hours, weekends closed, no holidays), registered with the
engine on first use from :func:`demo_calendar_xml`; ``MD_CALENDAR`` names any other
registered calendar (``USNYSE_EXAMPLE`` for real data, which does observe holidays).
"""

from __future__ import annotations

import os
import tempfile
import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

__all__ = [
    "CHART_TYPES",
    "PER_SYMBOL_CHARTS",
    "DEMO_CALENDAR",
    "ChartSet",
    "demo_calendar_xml",
    "ensure_calendar",
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

#: The business calendar registered for gap hiding when ``MD_CALENDAR`` is blank.
DEMO_CALENDAR = "MARKET_DATA_DEMO"

#: What the demo calendar describes -- the mock generator's session (doc 11 section 3).
DEMO_CALENDAR_ZONE = "America/New_York"
DEMO_CALENDAR_OPEN = "09:30"
DEMO_CALENDAR_CLOSE = "16:00"


@dataclass
class ChartSet:
    """What :func:`build_charts` produced: ``(title, figure)`` pairs plus any notes."""

    kind: str
    figures: List[Any] = field(default_factory=list)  # list of (title, figure)
    notes: List[str] = field(default_factory=list)
    #: The business calendar the figures hide gaps with (``None``: gaps shown).
    calendar: Optional[str] = None

    @property
    def is_empty(self) -> bool:
        return not self.figures


def demo_calendar_xml(
    name: str = DEMO_CALENDAR, first_valid: str = "2000-01-01", last_valid: str = "2059-12-31"
) -> str:
    """The business-calendar XML ``deephaven.calendar.add_calendar`` accepts.

    The same shape as the engine's bundled ``USNYSE_EXAMPLE.calendar`` with the
    ``<holiday>`` entries left out: New York session hours, Saturday/Sunday weekends.
    """
    return (
        "<calendar>\n"
        f"    <name>{name}</name>\n"
        f"    <timeZone>{DEMO_CALENDAR_ZONE}</timeZone>\n"
        "    <language>en</language>\n"
        "    <country>US</country>\n"
        f"    <firstValidDate>{first_valid}</firstValidDate>\n"
        f"    <lastValidDate>{last_valid}</lastValidDate>\n"
        "    <description>Market data demo: NYSE session hours, weekends closed, no holidays "
        "(the mock generator models none).</description>\n"
        "    <default>\n"
        f"        <businessTime><open>{DEMO_CALENDAR_OPEN}</open><close>{DEMO_CALENDAR_CLOSE}</close></businessTime>\n"
        "        <weekend>Saturday</weekend>\n"
        "        <weekend>Sunday</weekend>\n"
        "    </default>\n"
        "</calendar>\n"
    )


_registered: Dict[str, Optional[str]] = {}
_registry_lock = threading.Lock()


def ensure_calendar(name: Optional[str] = None) -> Optional[str]:
    """Return the name of a calendar the engine knows, registering the demo one on first use.

    ``name`` blank -> :data:`DEMO_CALENDAR`, written from :func:`demo_calendar_xml` to a
    temp file and ``add_calendar``-ed once per process; anything else must already be
    registered (``deephaven.calendar.calendar_names()``). Returns ``None`` -- after one
    log line saying why -- when the calendar is unknown, so callers plot *without* gap
    hiding instead of failing. Without a running engine (host python) it returns ``None``
    silently and caches nothing.
    """
    wanted = (name or "").strip() or DEMO_CALENDAR
    try:
        from deephaven import calendar as dh_calendar
    except Exception:  # noqa: BLE001 - host python, or the server not started yet
        return None
    with _registry_lock:
        if wanted in _registered:
            return _registered[wanted]
        try:
            known = list(dh_calendar.calendar_names())
            if wanted not in known:
                if wanted != DEMO_CALENDAR:
                    print(
                        f"[market-data] MD_CALENDAR={wanted!r} is not a registered calendar "
                        f"(known: {', '.join(known)}); gaps will not be hidden",
                        flush=True,
                    )
                    _registered[wanted] = None
                    return None
                path = os.path.join(tempfile.gettempdir(), f"{DEMO_CALENDAR}.calendar")
                with open(path, "w", encoding="utf-8") as handle:
                    handle.write(demo_calendar_xml(DEMO_CALENDAR))
                dh_calendar.add_calendar(path)
                print(f"[market-data] registered business calendar {DEMO_CALENDAR} from {path}", flush=True)
            _registered[wanted] = wanted
        except Exception as exc:  # noqa: BLE001 - an engine without calendars: degrade, do not die
            print(
                f"[market-data] calendar {wanted!r} unavailable ({type(exc).__name__}: {exc}); "
                "gaps will not be hidden",
                flush=True,
            )
            _registered[wanted] = None
        return _registered[wanted]


def _title(kind: str, symbols: Sequence[str], interval: str) -> str:
    label = CHART_TYPES.get(kind, kind)
    names = ", ".join(symbols) if symbols else "(no symbols)"
    return f"{label} -- {names} ({interval})"


def _title_hook(title: str) -> Callable[[Any], None]:
    """``unsafe_update_figure`` setting a layout title.

    The financial plots (``dx.candlestick`` / ``dx.ohlc``) take no ``title`` argument in
    plotly-express 0.20, so their title is set on the plotly figure instead.
    """

    def apply(figure: Any) -> None:
        figure.update_layout(title=title)

    return apply


def build_charts(
    kind: str,
    bars: Any,
    symbols: Sequence[str],
    interval: str = "1m",
    hide_gaps: bool = True,
    calendar: Optional[str] = None,
    normalized_table: Optional[Any] = None,
) -> ChartSet:
    """Build the figures for ``kind`` over ``bars`` (already resampled to ``interval``).

    Args:
        kind: A key of :data:`CHART_TYPES`.
        bars: The (resampled) bars table.
        symbols: Selected symbols, in display order.
        interval: For titles only.
        hide_gaps: Hide overnight/weekend gaps on the x axis (through ``calendar``).
        calendar: Name of the business calendar to hide gaps with; blank -> the demo
            calendar (see :func:`ensure_calendar`). Ignored when ``hide_gaps`` is False.
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

    cal = ensure_calendar(calendar) if hide_gaps else None
    if hide_gaps and cal is None:
        result.notes.append("gaps not hidden: no business calendar available")
    result.calendar = cal

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

    if kind in PER_SYMBOL_CHARTS:
        plotter = dx.candlestick if kind == "candlestick" else dx.ohlc
        for symbol in names:
            subset = bars.where(f"Symbol == `{symbol}`")

            def factory(extras: Dict[str, Any], subset: Any = subset) -> Any:
                return plotter(subset, x="Timestamp", open="Open", high="High", low="Low", close="Close", **extras)

            # No `title=` on the financial plots: it rides along in the figure hook.
            extras: Dict[str, Any] = {"unsafe_update_figure": _title_hook(f"{symbol} ({interval})")}
            if cal:
                extras["calendar"] = cal
            figure = attempt(factory, extras, f"{kind} {symbol}")
            if figure is not None:
                result.figures.append((symbol, figure))
        return result

    title = _title(kind, names, interval)
    gap_kwargs: Dict[str, Any] = {"calendar": cal} if cal else {}
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

        def factory(extras: Dict[str, Any]) -> Any:
            # `dx.bar` has no `calendar=` in plotly-express 0.20; the figure's `calendar`
            # property carries the same thing to the web plugin (bars are SVG traces).
            figure = dx.bar(bars, x="Timestamp", y="Volume", by="Symbol", title=title)
            if extras.get("calendar"):
                figure.calendar = extras["calendar"]
            return figure

    figure = attempt(factory, gap_kwargs, kind)
    if figure is not None:
        result.figures.append((title, figure))
    return result
