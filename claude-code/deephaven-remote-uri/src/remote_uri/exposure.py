"""The frozen exposure formulas and their python reference -- doc 10 section 7.

Two representations of the same arithmetic:

* the **query strings** the collector's ``update_view`` compiles
  (:data:`EXPOSURE_FORMULAS`) -- frozen text, because the e2e and the dashboard both
  read the columns they define;
* a **pure python reference** (:func:`order_exposure`, :func:`sum_exposure`) -- the
  oracle the unit suite and the e2e compare the engine's numbers against.

Keeping the two side by side in one module is deliberate: an edit to a formula that
is not mirrored in the reference fails ``tests/test_exposure.py`` immediately,
rather than silently redefining what "exposure" means.

Null handling matches doc 09's ``Notional``: a missing ``AvgPx``/``CumQty``/
``LeavesQty`` contributes ``0.0``, never a null that would poison a sum. ``MarkPx``
falls back to the order's own limit ``Price`` when the symbol has no quote, and to
``0.0`` when it has neither -- an unpriced open quantity is reported as zero
exposure, never as an error.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

__all__ = [
    "EXEC_NOTIONAL_FORMULA",
    "MARK_PX_FORMULA",
    "OPEN_NOTIONAL_FORMULA",
    "TOTAL_NOTIONAL_FORMULA",
    "SIGNED_EXPOSURE_FORMULA",
    "EXPOSURE_FORMULAS",
    "EXPOSURE_COLUMNS",
    "EXPOSURE_SUM_COLUMNS",
    "LEVEL_BY",
    "LEVEL_SORT",
    "SOURCE_BY",
    "BUY_SIDE",
    "as_double",
    "exec_notional",
    "mark_px",
    "open_notional",
    "signed_exposure",
    "order_exposure",
    "sum_exposure",
]

#: ``AvgPx * CumQty`` -- identical to doc 09's ``Notional`` on ``orders_linked``;
#: recomputed rather than renamed so the column carries doc 10's name and this
#: module owns the whole section 7 block.
EXEC_NOTIONAL_FORMULA = (
    "ExecNotional = (isNull(AvgPx) ? 0.0 : AvgPx) * (isNull(CumQty) ? 0.0 : CumQty)"
)
#: The market mid, else the order's own limit price, else zero.
MARK_PX_FORMULA = "MarkPx = isNull(Mid) ? (isNull(Price) ? 0.0 : Price) : Mid"
#: What can still execute, marked.
OPEN_NOTIONAL_FORMULA = "OpenNotional = (isNull(LeavesQty) ? 0.0 : LeavesQty) * MarkPx"
#: The order's notional exposure: done plus still-open.
TOTAL_NOTIONAL_FORMULA = "TotalNotional = ExecNotional + OpenNotional"
#: Buy +, sell / sell-short -.
SIGNED_EXPOSURE_FORMULA = "SignedExposure = (Side == `BUY` ? 1.0 : -1.0) * TotalNotional"

#: The five formulas, in dependency order -- one ``update_view`` on ``orders_marked``.
EXPOSURE_FORMULAS: Tuple[str, ...] = (
    EXEC_NOTIONAL_FORMULA,
    MARK_PX_FORMULA,
    OPEN_NOTIONAL_FORMULA,
    TOTAL_NOTIONAL_FORMULA,
    SIGNED_EXPOSURE_FORMULA,
)

#: The columns :data:`EXPOSURE_FORMULAS` adds, in the same order.
EXPOSURE_COLUMNS: Tuple[str, ...] = (
    "ExecNotional",
    "MarkPx",
    "OpenNotional",
    "TotalNotional",
    "SignedExposure",
)

#: Columns summed by both exposure aggregates (doc 10 section 7).
#: ``MarkPx`` is deliberately absent: summing a price is meaningless.
EXPOSURE_SUM_COLUMNS: Tuple[str, ...] = (
    "OrderQty",
    "CumQty",
    "LeavesQty",
    "ExecNotional",
    "OpenNotional",
    "TotalNotional",
    "SignedExposure",
)

#: ``exposure_by_level`` grouping: where the flow went, hop by hop.
LEVEL_BY: Tuple[str, ...] = ("RootOms", "RootAccount", "RootSymbol", "Oms", "HubDepth")
#: ``exposure_by_level`` ordering: families together, upstream -> downstream.
LEVEL_SORT: Tuple[str, ...] = ("RootOms", "RootAccount", "RootSymbol", "HubDepth", "Oms")
#: ``exposure_by_source`` grouping -- **these are "the" totals** for a lookup.
#: Summing across hubs would count one economic flow once per hop (doc 09's rule).
SOURCE_BY: Tuple[str, ...] = ("RootOms", "RootAccount", "RootSymbol")

#: The ``Side`` value that makes exposure positive (doc 01's tag-54 enum name).
BUY_SIDE = "BUY"


def as_double(value: Any) -> float:
    """Coerce a cell to a float the way the query formulas' ``isNull`` guards do.

    Args:
        value: A python value read from a table, an oracle JSON file, or ``None``.

    Returns:
        The float, or ``0.0`` for ``None``, NaN, an infinity or anything
        unconvertible -- the reference must never produce a NaN that would make
        every downstream comparison in the e2e vacuously false.
    """
    if value is None:
        return 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(number):
        return 0.0
    return number


def exec_notional(avg_px: Any, cum_qty: Any) -> float:
    """``(isNull(AvgPx) ? 0 : AvgPx) * (isNull(CumQty) ? 0 : CumQty)``."""
    return as_double(avg_px) * as_double(cum_qty)


def mark_px(mid: Any, price: Any) -> float:
    """The market mid, else the order's limit price, else ``0.0``.

    Args:
        mid: ``market_data_latest.Mid`` for the order's symbol, or ``None`` when the
            symbol has no quote yet.
        price: The order's own ``Price`` (tag 44); ``None``/``0`` for a market order.
    """
    if mid is None:
        return as_double(price)
    value = as_double(mid)
    # A non-numeric Mid degrades to the limit price rather than to zero: the
    # formula's ternary only tests for null, and a null Mid is the only way the
    # engine can produce a non-numeric one.
    return value if value else as_double(price)


def open_notional(leaves_qty: Any, mark: float) -> float:
    """``(isNull(LeavesQty) ? 0 : LeavesQty) * MarkPx``."""
    return as_double(leaves_qty) * as_double(mark)


def signed_exposure(side: Any, total: float) -> float:
    """``(Side == `BUY` ? +1 : -1) * TotalNotional``."""
    sign = 1.0 if str(side or "") == BUY_SIDE else -1.0
    return sign * as_double(total)


def order_exposure(order: Mapping[str, Any], mid: Optional[Any] = None) -> Dict[str, float]:
    """Compute one order's section 7 columns.

    Args:
        order: A row-like mapping carrying ``AvgPx``, ``CumQty``, ``LeavesQty``,
            ``Price`` and ``Side`` (missing keys read as null).
        mid: The symbol's market mid; ``None`` uses the order's ``Mid`` key if it
            has one, else falls back to ``Price`` per :func:`mark_px`.

    Returns:
        ``{"ExecNotional", "MarkPx", "OpenNotional", "TotalNotional",
        "SignedExposure"}`` -- exactly :data:`EXPOSURE_COLUMNS`.
    """
    quote = order.get("Mid") if mid is None else mid
    executed = exec_notional(order.get("AvgPx"), order.get("CumQty"))
    mark = mark_px(quote, order.get("Price"))
    still_open = open_notional(order.get("LeavesQty"), mark)
    total = executed + still_open
    return {
        "ExecNotional": executed,
        "MarkPx": mark,
        "OpenNotional": still_open,
        "TotalNotional": total,
        "SignedExposure": signed_exposure(order.get("Side"), total),
    }


def sum_exposure(
    orders: Iterable[Mapping[str, Any]],
    mids: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Aggregate :func:`order_exposure` over a set of orders.

    The python twin of ``agg_by([count_("Orders"), sum_(EXPOSURE_SUM_COLUMNS)])`` --
    the e2e's oracle for ``exposure_for`` (over ``Depth == 0`` rows) and for one
    group of ``exposure_by_level``.

    Args:
        orders: Row-like mappings; each is marked independently.
        mids: ``{symbol: mid}`` read from ``market_data_latest`` at assertion time;
            a symbol absent from it marks at the order's own ``Price``.

    Returns:
        ``{"Orders": count}`` plus one key per :data:`EXPOSURE_SUM_COLUMNS`.
    """
    quotes: Mapping[str, Any] = mids or {}
    totals: Dict[str, float] = {name: 0.0 for name in EXPOSURE_SUM_COLUMNS}
    count = 0
    for order in orders:
        count += 1
        marked = order_exposure(order, quotes.get(str(order.get("Symbol") or "")))
        totals["OrderQty"] += as_double(order.get("OrderQty"))
        totals["CumQty"] += as_double(order.get("CumQty"))
        totals["LeavesQty"] += as_double(order.get("LeavesQty"))
        for name in ("ExecNotional", "OpenNotional", "TotalNotional", "SignedExposure"):
            totals[name] += marked[name]
    # `Orders` is `agg.count_`'s long, so an int here -- every other key is a double.
    result: Dict[str, Any] = {"Orders": count}
    result.update(totals)
    return result
