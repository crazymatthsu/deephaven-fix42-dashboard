"""Declarative derived nodes -- doc 03 section 2.4.

Everything here is a pure, incrementally-computed table operation over the five
blink streams published by :class:`dh_app.pipeline.Pipeline`.  No state, no python
callbacks: late/duplicate data is already resolved inside ``fix42cache``.

Node names returned by :func:`build_derived` are the frozen global names the
dashboard, the query API and the integration test bind to.
"""

from __future__ import annotations

from typing import Dict, Mapping

from deephaven import agg
from deephaven.stream import blink_to_append_only
from deephaven.table import Table

__all__ = ["DERIVED_TABLE_NAMES", "build_derived"]

#: Every global published by :func:`build_derived`, in dependency order.
DERIVED_TABLE_NAMES = (
    "order_state_latest",
    "executions",
    "executions_latest",
    "order_events",
    "fix_messages",
    "clordid_index",
    "execid_index",
    "status_summary",
    "symbol_summary",
    "open_orders",
    "account_list",
)


def build_derived(streams: Mapping[str, Table]) -> Dict[str, Table]:
    """Build the derived DAG from the publisher blink tables.

    Args:
        streams: The blink tables keyed by their doc 03 section 2.3 names --
            ``order_state_blink``, ``executions_blink``, ``order_events_blink``,
            ``fix_messages_blink`` (``ingest_errors`` is not derived from).

    Returns:
        A dict keyed by :data:`DERIVED_TABLE_NAMES`:

        ``order_state_latest``
            THE cache: ``last_by("OrderKey")`` over the blink snapshot stream, so
            memory is O(#orders) rather than O(#messages) (doc 02 section 1.1).
        ``executions`` / ``order_events`` / ``fix_messages``
            Append-only history for the panels and the audit trail.
        ``executions_latest``
            ``last_by("ExecID")`` -- current disposition after bust/correct/DK.
        ``clordid_index`` / ``execid_index``
            Alias -> ``OrderKey`` resolution for the query API.
        ``status_summary`` / ``symbol_summary`` / ``account_list``
            Dashboard summary bar and filter sources.
        ``open_orders``
            Non-terminal orders.
    """
    order_state_blink = streams["order_state_blink"]
    executions_blink = streams["executions_blink"]
    order_events_blink = streams["order_events_blink"]
    fix_messages_blink = streams["fix_messages_blink"]

    order_state_latest = order_state_blink.last_by("OrderKey")
    executions = blink_to_append_only(executions_blink)
    executions_latest = executions_blink.last_by("ExecID")
    order_events = blink_to_append_only(order_events_blink)
    fix_messages = blink_to_append_only(fix_messages_blink)

    clordid_index = (
        order_events.where("ClOrdID != ``").last_by("ClOrdID").view(["ClOrdID", "OrderKey"])
    )
    execid_index = executions_latest.where("ExecID != ``").view(["ExecID", "OrderKey"])

    status_summary = order_state_latest.count_by("Count", by=["OrdStatus"]).sort(["OrdStatus"])
    symbol_summary = order_state_latest.agg_by(
        [agg.count_("Orders"), agg.sum_(["CumQty", "OrderQty"])], by=["Symbol"]
    )
    open_orders = order_state_latest.where("!Terminal")
    account_list = order_state_latest.select_distinct(["Account"]).sort(["Account"])

    return {
        "order_state_latest": order_state_latest,
        "executions": executions,
        "executions_latest": executions_latest,
        "order_events": order_events,
        "fix_messages": fix_messages,
        "clordid_index": clordid_index,
        "execid_index": execid_index,
        "status_summary": status_summary,
        "symbol_summary": symbol_summary,
        "open_orders": open_orders,
        "account_list": account_list,
    }
