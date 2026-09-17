"""The Deephaven bridge: input tables that mirror the core, and the derived views.

Server only (imports ``deephaven``). ``TableBridge`` implements the core's
:class:`~basket_oms_demo.core.Listener` protocol: every changed basket / order / quote is
**upserted** into a keyed input table, every execution / event is **appended** to an
append-only one. The derived tables (doc 13 §2 "Derived") are built once here and tick
on their own -- nothing else in the app touches a table after startup.

Column names and types are the contract's; ``COLUMNS`` keeps them in one place for the
embedded test and the dashboard.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

from deephaven import agg, dtypes, input_table, new_table
from deephaven.column import datetime_col, double_col, long_col, string_col
from deephaven.constants import NULL_DOUBLE, NULL_LONG
from deephaven.table import Table
from deephaven.time import to_j_instant

from basket_oms_demo.core import Basket, Event, Execution, Order, Quote

__all__ = ["COLUMNS", "TableBridge"]

#: The contract's column definitions (doc 13 §2), in display order.
COLUMNS: Dict[str, Dict[str, Any]] = {
    "oms_baskets": {
        "BasketId": dtypes.string,
        "Name": dtypes.string,
        "Strategy": dtypes.string,
        "Status": dtypes.string,
        "Trader": dtypes.string,
        "Created": dtypes.Instant,
        "Updated": dtypes.Instant,
        "Note": dtypes.string,
    },
    "oms_orders": {
        "OrderId": dtypes.string,
        "BasketId": dtypes.string,
        "ParentId": dtypes.string,
        "Symbol": dtypes.string,
        "Side": dtypes.string,
        "Qty": dtypes.int64,
        "Filled": dtypes.int64,
        "Leaves": dtypes.int64,
        "OrdType": dtypes.string,
        "LimitPx": dtypes.double,
        "TIF": dtypes.string,
        "Venue": dtypes.string,
        "Status": dtypes.string,
        "AvgPx": dtypes.double,
        "LastPx": dtypes.double,
        "LastQty": dtypes.int64,
        "Trader": dtypes.string,
        "Created": dtypes.Instant,
        "Updated": dtypes.Instant,
        "Note": dtypes.string,
    },
    "oms_events": {
        "Seq": dtypes.int64,
        "Time": dtypes.Instant,
        "Type": dtypes.string,
        "OrderId": dtypes.string,
        "BasketId": dtypes.string,
        "Detail": dtypes.string,
        "User": dtypes.string,
    },
    "oms_executions": {
        "ExecId": dtypes.string,
        "Time": dtypes.Instant,
        "OrderId": dtypes.string,
        "BasketId": dtypes.string,
        "Symbol": dtypes.string,
        "Side": dtypes.string,
        "LastQty": dtypes.int64,
        "LastPx": dtypes.double,
        "Venue": dtypes.string,
    },
    "oms_quotes": {
        "Symbol": dtypes.string,
        "Bid": dtypes.double,
        "Ask": dtypes.double,
        "Last": dtypes.double,
        "RefPx": dtypes.double,
        "ChgPct": dtypes.double,
        "Updated": dtypes.Instant,
    },
}

_LIVE = "(Status == `NEW` || Status == `ROUTED` || Status == `WORKING` || Status == `PARTIAL`)"
_DONE = "(Status == `FILLED` || Status == `CANCELLED` || Status == `REJECTED`)"


def _instant(value: Optional[dt.datetime]):
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return to_j_instant(value)


def _dbl(value: Optional[float]) -> float:
    return NULL_DOUBLE if value is None else float(value)


def _lng(value: Optional[int]) -> int:
    return NULL_LONG if value is None else int(value)


def _row(spec: Dict[str, Any], values: Dict[str, Any]) -> Table:
    """A one-row table in the exact column order/types of ``spec``."""
    cols = []
    for name, dtype in spec.items():
        value = values[name]
        if dtype == dtypes.string:
            cols.append(string_col(name, [None if value is None else str(value)]))
        elif dtype == dtypes.int64:
            cols.append(long_col(name, [_lng(value)]))
        elif dtype == dtypes.double:
            cols.append(double_col(name, [_dbl(value)]))
        elif dtype == dtypes.Instant:
            cols.append(datetime_col(name, [_instant(value)]))
        else:  # pragma: no cover - the contract only uses the four types above
            raise TypeError(f"unsupported column type for {name}: {dtype}")
    return new_table(cols)


class TableBridge:
    """Input tables + derived views; the core's listener (see the module docstring)."""

    def __init__(self) -> None:
        self.baskets = input_table(col_defs=COLUMNS["oms_baskets"], key_cols="BasketId")
        self.orders = input_table(col_defs=COLUMNS["oms_orders"], key_cols="OrderId")
        self.events = input_table(col_defs=COLUMNS["oms_events"])
        self.executions = input_table(col_defs=COLUMNS["oms_executions"])
        self.quotes = input_table(col_defs=COLUMNS["oms_quotes"], key_cols="Symbol")
        self.orders_view = self._build_orders_view()
        self.baskets_view = self._build_baskets_view()
        #: ``oms_orders`` without the editable attribute -- what the dashboard's tree is built from.
        self.orders_readonly = self.orders.without_attributes("InputTable")
        self.orders_tree = self.orders_readonly.tree(id_col="OrderId", parent_col="ParentId", promote_orphans=True)

    # -- derived views ------------------------------------------------------------------

    def _build_orders_view(self) -> Table:
        # ``without_attributes``: a derived table inherits the input table's ``InputTable``
        # attribute, which makes the web grid *editable* (typed cells would go straight
        # into ``oms_orders``, bypassing the core). The views are read-only projections.
        view = (
            self.orders.natural_join(self.quotes, on="Symbol", joins=["Bid", "Ask", "Last", "RefPx"])
            .update_view(
                [
                    "PctFilled = Qty > 0 ? 100.0 * Filled / Qty : 0.0",
                    "Notional = isNull(RefPx) ? NULL_DOUBLE : Qty * RefPx",
                    "FilledNotional = isNull(AvgPx) ? 0.0 : Filled * AvgPx",
                ]
            )
            .without_attributes("InputTable")
        )
        # Blotter column order: the trading columns first, bookkeeping last.
        return view.move_columns_down(["Updated", "Note", "ParentId", "Trader", "Created", "LastQty", "RefPx", "FilledNotional"]).move_columns(7, "PctFilled")

    def _build_baskets_view(self) -> Table:
        per_basket = (
            self.orders_view.where("Status != `SPLIT`")
            .update_view([f"LiveFlag = {_LIVE} ? 1L : 0L", f"DoneFlag = {_DONE} ? 1L : 0L", "NotionalOrZero = isNull(Notional) ? 0.0 : Notional"])
            .agg_by(
                [
                    agg.count_("Orders"),
                    agg.sum_(["Live = LiveFlag", "Done = DoneFlag", "Qty", "Filled", "Leaves", "Notional = NotionalOrZero", "FilledNotional"]),
                ],
                by="BasketId",
            )
        )
        return (
            self.baskets.natural_join(per_basket, on="BasketId")
            .update_view(
                [
                    "Orders = isNull(Orders) ? 0L : Orders",
                    "Live = isNull(Live) ? 0L : Live",
                    "Done = isNull(Done) ? 0L : Done",
                    "Qty = isNull(Qty) ? 0L : Qty",
                    "Filled = isNull(Filled) ? 0L : Filled",
                    "Leaves = isNull(Leaves) ? 0L : Leaves",
                    "PctFilled = Qty > 0 ? 100.0 * Filled / Qty : 0.0",
                    "Notional = isNull(Notional) ? 0.0 : Notional",
                    "FilledNotional = isNull(FilledNotional) ? 0.0 : FilledNotional",
                ]
            )
            .without_attributes("InputTable")
            .move_columns_down(["Trader", "Created", "Updated", "Note"])
        )

    def globals(self) -> Dict[str, Table]:
        """The contract's table globals (doc 13 §2)."""
        return {
            "oms_baskets": self.baskets,
            "oms_orders": self.orders,
            "oms_events": self.events,
            "oms_executions": self.executions,
            "oms_quotes": self.quotes,
            "oms_baskets_view": self.baskets_view,
            "oms_orders_view": self.orders_view,
            "oms_orders_tree": self.orders_tree,
        }

    # -- Listener -----------------------------------------------------------------------

    def on_basket(self, basket: Basket) -> None:
        self.baskets.add(
            _row(
                COLUMNS["oms_baskets"],
                {
                    "BasketId": basket.basket_id,
                    "Name": basket.name,
                    "Strategy": basket.strategy,
                    "Status": basket.status,
                    "Trader": basket.trader,
                    "Created": basket.created,
                    "Updated": basket.updated,
                    "Note": basket.note,
                },
            )
        )

    def on_order(self, order: Order) -> None:
        self.orders.add(
            _row(
                COLUMNS["oms_orders"],
                {
                    "OrderId": order.order_id,
                    "BasketId": order.basket_id,
                    "ParentId": order.parent_id,
                    "Symbol": order.symbol,
                    "Side": order.side,
                    "Qty": order.qty,
                    "Filled": order.filled,
                    "Leaves": order.leaves,
                    "OrdType": order.ord_type,
                    "LimitPx": order.limit_px,
                    "TIF": order.tif,
                    "Venue": order.venue,
                    "Status": order.status,
                    "AvgPx": order.avg_px,
                    "LastPx": order.last_px,
                    "LastQty": order.last_qty,
                    "Trader": order.trader,
                    "Created": order.created,
                    "Updated": order.updated,
                    "Note": order.note,
                },
            )
        )

    def on_execution(self, execution: Execution) -> None:
        self.executions.add(
            _row(
                COLUMNS["oms_executions"],
                {
                    "ExecId": execution.exec_id,
                    "Time": execution.time,
                    "OrderId": execution.order_id,
                    "BasketId": execution.basket_id,
                    "Symbol": execution.symbol,
                    "Side": execution.side,
                    "LastQty": execution.last_qty,
                    "LastPx": execution.last_px,
                    "Venue": execution.venue,
                },
            )
        )

    def on_event(self, event: Event) -> None:
        self.events.add(
            _row(
                COLUMNS["oms_events"],
                {
                    "Seq": event.seq,
                    "Time": event.time,
                    "Type": event.type,
                    "OrderId": event.order_id,
                    "BasketId": event.basket_id,
                    "Detail": event.detail,
                    "User": event.user,
                },
            )
        )

    def on_quote(self, quote: Quote) -> None:
        self.quotes.add(
            _row(
                COLUMNS["oms_quotes"],
                {
                    "Symbol": quote.symbol,
                    "Bid": quote.bid,
                    "Ask": quote.ask,
                    "Last": quote.last,
                    "RefPx": quote.ref_px,
                    "ChgPct": quote.chg_pct,
                    "Updated": quote.updated,
                },
            )
        )
