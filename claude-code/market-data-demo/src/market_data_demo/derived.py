"""Derived tables over the loaded bars (doc 11 section 6).

Everything here is a declarative Deephaven table operation over a **static** bars table
(the reader's output), so a re-selection in the UI simply builds a new small DAG and
lets the old one be collected. ``deephaven`` is imported inside the functions: the
interval registry at the top is pure and unit-tested on the host.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, List, Optional, Sequence

__all__ = [
    "INTERVALS",
    "BAR_COLUMNS",
    "NY_ZONE",
    "interval_seconds",
    "interval_nanos",
    "resample",
    "daily_summary",
    "normalized",
    "empty_bars",
]

#: Bar intervals the UI offers, label -> seconds. ``1m`` is the native file resolution.
INTERVALS: "OrderedDict[str, int]" = OrderedDict(
    [
        ("1m", 60),
        ("5m", 300),
        ("15m", 900),
        ("30m", 1800),
        ("1h", 3600),
        ("1D", 86400),
    ]
)

#: Canonical column order of a bars table (files, merged table, resampled table).
BAR_COLUMNS = ("Timestamp", "Symbol", "Open", "High", "Low", "Close", "Volume", "VWAP", "TradeCount")

#: Trading day boundaries are New York days -- a 20:00Z close is still "today".
NY_ZONE = "America/New_York"


def interval_seconds(label: str) -> int:
    """Seconds per bar for an interval label (``ValueError`` on an unknown one)."""
    key = str(label).strip()
    if key not in INTERVALS:
        raise ValueError(f"unknown interval {label!r}: expected one of {', '.join(INTERVALS)}")
    return INTERVALS[key]


def interval_nanos(label: str) -> int:
    """Nanoseconds per bar, the unit ``lowerBin`` takes."""
    return interval_seconds(label) * 1_000_000_000


def empty_bars() -> Any:
    """A zero-row table with the canonical bar schema (what a query with no files yields)."""
    from deephaven import new_table
    from deephaven.column import datetime_col, double_col, long_col, string_col

    return new_table(
        [
            datetime_col("Timestamp", []),
            string_col("Symbol", []),
            double_col("Open", []),
            double_col("High", []),
            double_col("Low", []),
            double_col("Close", []),
            long_col("Volume", []),
            double_col("VWAP", []),
            long_col("TradeCount", []),
        ]
    )


def resample(bars: Any, interval: str) -> Any:
    """Roll one-minute bars up to ``interval``.

    ``Open`` is the first, ``Close`` the last, ``High``/``Low`` the extremes, ``Volume``
    and ``TradeCount`` sums, ``VWAP`` the volume-weighted mean of the minute VWAPs. The
    bar's ``Timestamp`` is the first minute in the bin (so a ``1D`` bar is stamped
    09:30 New York, not midnight UTC). ``1m`` returns the input untouched.
    """
    from deephaven import agg

    if interval_seconds(interval) == INTERVALS["1m"]:
        return bars
    nanos = interval_nanos(interval)
    binned = bars.sort(["Symbol", "Timestamp"]).update_view([f"Bin = lowerBin(Timestamp, {nanos}L)"])
    aggregations = [
        agg.first(["Timestamp", "Open"]),
        agg.max_("High"),
        agg.min_("Low"),
        agg.last("Close"),
        agg.sum_(["Volume", "TradeCount"]),
        agg.weighted_avg("Volume", "VWAP"),
        agg.count_("Bars"),
    ]
    rolled = binned.agg_by(aggregations, by=["Symbol", "Bin"])
    return rolled.view(list(BAR_COLUMNS) + ["Bars"]).sort(["Symbol", "Timestamp"])


def daily_summary(bars: Any) -> Any:
    """One row per symbol per New York trading day: OHLC, volume, day return in percent."""
    from deephaven import agg

    dated = bars.sort(["Symbol", "Timestamp"]).update_view(
        [f"TradeDate = toLocalDate(Timestamp, timeZone(`{NY_ZONE}`))"]
    )
    rolled = dated.agg_by(
        [
            agg.first("Open"),
            agg.max_("High"),
            agg.min_("Low"),
            agg.last("Close"),
            agg.sum_(["Volume", "TradeCount"]),
            agg.count_("Bars"),
        ],
        by=["Symbol", "TradeDate"],
    )
    return rolled.update_view(
        [
            "ReturnPct = Open == 0 ? NULL_DOUBLE : (Close / Open - 1.0) * 100.0",
            "RangePct = Open == 0 ? NULL_DOUBLE : (High - Low) / Open * 100.0",
        ]
    ).sort(["Symbol", "TradeDate"])


def normalized(bars: Any) -> Any:
    """Add ``PctChange``: each symbol's close relative to its first close in the table.

    Puts symbols with very different price levels on one axis (the "normalized" chart).
    """
    ordered = bars.sort(["Symbol", "Timestamp"])
    bases = ordered.first_by("Symbol").view(["Symbol", "BaseClose = Close"])
    return ordered.natural_join(bases, on=["Symbol"]).update_view(
        ["PctChange = BaseClose == 0 ? NULL_DOUBLE : (Close / BaseClose - 1.0) * 100.0"]
    )


def symbol_filter(symbols: Sequence[str]) -> Optional[str]:
    """``Symbol in `A`, `B``` clause for a symbol list (``None`` for an empty list)."""
    names = [str(s).strip().upper() for s in symbols if str(s).strip()]
    if not names:
        return None
    return "Symbol in " + ", ".join(f"`{name}`" for name in names)
