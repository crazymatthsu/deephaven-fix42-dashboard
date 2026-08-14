"""Single source of truth for the published table schemas.

Column names here are **frozen** by ``docs/01-fix42-messages-and-state-machine.md``
(section 4 for ``order_state``, section 6 for ``executions`` / ``order_events`` /
``fix_messages``).  They must match, character for character, the keys returned by
``fix42cache``'s ``OrderState.to_row()`` / ``ExecutionRow.to_row()`` /
``OrderEventRow.to_row()`` / ``MessageRow.to_row()``.

Each schema is an ordered ``dict`` mapping column name -> Deephaven dtype; the
insertion order *is* the publisher column order and is reused verbatim when
:mod:`dh_app.pipeline` builds per-cycle batches.
"""

from __future__ import annotations

from typing import Dict, Tuple

from deephaven import dtypes as dht

__all__ = [
    "ORDER_STATE_SCHEMA",
    "ORDER_STATE_COLUMNS",
    "EXECUTIONS_SCHEMA",
    "EXECUTIONS_COLUMNS",
    "ORDER_EVENTS_SCHEMA",
    "ORDER_EVENTS_COLUMNS",
    "FIX_MESSAGES_SCHEMA",
    "FIX_MESSAGES_COLUMNS",
    "INGEST_ERRORS_SCHEMA",
    "INGEST_ERRORS_COLUMNS",
    "ALL_SCHEMAS",
    "NULLABLE_BOOLEAN_COLUMNS",
    "ORDER_GRID_LEAD_COLUMNS",
    "order_grid_columns",
]


# --------------------------------------------------------------------------------------
# order_state -- doc 01 section 4 ("OrderState -- the cache value"), one row per chain
#                published after every message applied to that chain.
# --------------------------------------------------------------------------------------
ORDER_STATE_SCHEMA: Dict[str, dht.DType] = {
    "OrderKey": dht.string,
    "OrderID": dht.string,
    "ClOrdID": dht.string,
    "OrigClOrdID": dht.string,
    "RootClOrdID": dht.string,
    "ClOrdIDChain": dht.string,
    "Account": dht.string,
    "Symbol": dht.string,
    "Side": dht.string,
    "OrdType": dht.string,
    "TimeInForce": dht.string,
    "OrderQty": dht.double,
    "Price": dht.double,
    "StopPx": dht.double,
    "OrdStatus": dht.string,
    "PendingAction": dht.string,
    "PendingClOrdID": dht.string,
    "LastExecType": dht.string,
    "CumQty": dht.double,
    "LeavesQty": dht.double,
    "AvgPx": dht.double,
    "LastShares": dht.double,
    "LastPx": dht.double,
    "LastMkt": dht.string,
    "OrdRejReason": dht.string,
    "CxlRejReason": dht.string,
    "DKReason": dht.string,
    "Text": dht.string,
    "ExecCount": dht.long,
    "MsgCount": dht.long,
    "FirstSeenTs": dht.Instant,
    "LastUpdateTs": dht.Instant,
    "LastMsgType": dht.string,
    "Terminal": dht.bool_,
}

# --------------------------------------------------------------------------------------
# executions -- doc 01 section 6, one row per 35=8 (and per 35=Q); bust/correct/DK also
#               re-emit the referenced execution's row with its new FillStatus.
# --------------------------------------------------------------------------------------
EXECUTIONS_SCHEMA: Dict[str, dht.DType] = {
    "OrderKey": dht.string,
    "OrderID": dht.string,
    "ClOrdID": dht.string,
    "ExecID": dht.string,
    "ExecRefID": dht.string,
    "ExecTransType": dht.string,
    "ExecType": dht.string,
    "OrdStatus": dht.string,
    "LastShares": dht.double,
    "LastPx": dht.double,
    "CumQty": dht.double,
    "LeavesQty": dht.double,
    "AvgPx": dht.double,
    "LastMkt": dht.string,
    "Text": dht.string,
    "IsFill": dht.bool_,
    "FillStatus": dht.string,
    "TransactTime": dht.Instant,
    "IngestTs": dht.Instant,
}

# --------------------------------------------------------------------------------------
# order_events -- doc 01 section 6, one row per lifecycle event (order-history panel).
# --------------------------------------------------------------------------------------
ORDER_EVENTS_SCHEMA: Dict[str, dht.DType] = {
    "OrderKey": dht.string,
    "ClOrdID": dht.string,
    "OrigClOrdID": dht.string,
    "OrderID": dht.string,
    "EventType": dht.string,
    "MsgType": dht.string,
    "OrdStatus": dht.string,
    "OrderQty": dht.double,
    "Price": dht.double,
    "Detail": dht.string,
    "TransactTime": dht.Instant,
    "IngestTs": dht.Instant,
}

# --------------------------------------------------------------------------------------
# fix_messages -- doc 01 section 6: "OrderKey, MsgType, all section 2 tags as typed
#                 columns, RawFix, ChecksumOk, SeqNum, SendingTime, IngestTs".
#                 Tag columns follow the doc 01 section 2 vocabulary table order.
# --------------------------------------------------------------------------------------
FIX_MESSAGES_SCHEMA: Dict[str, dht.DType] = {
    "OrderKey": dht.string,
    "MsgType": dht.string,
    # ---- doc 01 section 2 tag vocabulary, typed ----
    "ClOrdID": dht.string,           # 11
    "OrigClOrdID": dht.string,       # 41
    "OrderID": dht.string,           # 37
    "ExecID": dht.string,            # 17
    "ExecRefID": dht.string,         # 19
    "ExecTransType": dht.string,     # 20  (enum name)
    "ExecType": dht.string,          # 150 (enum name)
    "OrdStatus": dht.string,         # 39  (enum name)
    "Account": dht.string,           # 1
    "Symbol": dht.string,            # 55
    "Side": dht.string,              # 54  (enum name)
    "OrderQty": dht.double,          # 38
    "OrdType": dht.string,           # 40  (enum name)
    "Price": dht.double,             # 44
    "TimeInForce": dht.string,       # 59  (enum name)
    "CumQty": dht.double,            # 14
    "LeavesQty": dht.double,         # 151
    "AvgPx": dht.double,             # 6
    "LastShares": dht.double,        # 32
    "LastPx": dht.double,            # 31
    "LastMkt": dht.string,           # 30
    "OrdRejReason": dht.string,      # 103
    "CxlRejReason": dht.string,      # 102
    "CxlRejResponseTo": dht.string,  # 434 (enum name)
    "DKReason": dht.string,          # 127
    "Text": dht.string,              # 58
    "TransactTime": dht.Instant,     # 60
    "HandlInst": dht.string,         # 21
    # ---- framing / bookkeeping ----
    "RawFix": dht.string,            # SOH rendered as '|' for display
    "ChecksumOk": dht.bool_,         # null when the message carries no tag 10
    "SeqNum": dht.long,              # 34
    "SendingTime": dht.Instant,      # 52
    "IngestTs": dht.Instant,
}

# --------------------------------------------------------------------------------------
# ingest_errors -- not a fix42cache row type; produced by dh_app when a message cannot
#                  be processed (doc 03 section 2.2 failure policy).
# --------------------------------------------------------------------------------------
INGEST_ERRORS_SCHEMA: Dict[str, dht.DType] = {
    "RawFix": dht.string,
    "Error": dht.string,
    "IngestTs": dht.Instant,
}


ORDER_STATE_COLUMNS: Tuple[str, ...] = tuple(ORDER_STATE_SCHEMA)
EXECUTIONS_COLUMNS: Tuple[str, ...] = tuple(EXECUTIONS_SCHEMA)
ORDER_EVENTS_COLUMNS: Tuple[str, ...] = tuple(ORDER_EVENTS_SCHEMA)
FIX_MESSAGES_COLUMNS: Tuple[str, ...] = tuple(FIX_MESSAGES_SCHEMA)
INGEST_ERRORS_COLUMNS: Tuple[str, ...] = tuple(INGEST_ERRORS_SCHEMA)

#: All publisher schemas keyed by the doc 03 section 2.3 blink-table name.
ALL_SCHEMAS: Dict[str, Dict[str, dht.DType]] = {
    "order_state_blink": ORDER_STATE_SCHEMA,
    "executions_blink": EXECUTIONS_SCHEMA,
    "order_events_blink": ORDER_EVENTS_SCHEMA,
    "fix_messages_blink": FIX_MESSAGES_SCHEMA,
    "ingest_errors": INGEST_ERRORS_SCHEMA,
}

#: Boolean columns that are genuinely tri-state.  Every other boolean column is
#: contractually always populated, so a missing value defaults to ``False`` -- this
#: keeps ``order_state_latest.where("!Terminal")`` (doc 03 section 2.4) null-safe.
NULLABLE_BOOLEAN_COLUMNS = frozenset({"ChecksumOk"})

#: Preferred left-hand columns of the master orders grid (doc 03 section 2.6).
ORDER_GRID_LEAD_COLUMNS: Tuple[str, ...] = (
    "OrderKey",
    "OrderID",
    "ClOrdID",
    "Account",
    "Symbol",
    "Side",
    "OrdStatus",
    "PendingAction",
    "OrderQty",
    "Price",
    "CumQty",
    "LeavesQty",
    "AvgPx",
    "LastExecType",
    "LastUpdateTs",
)


def order_grid_columns() -> Tuple[str, ...]:
    """Return the orders-grid column order: lead columns first, then the rest.

    Returns:
        Every :data:`ORDER_STATE_COLUMNS` entry exactly once, with
        :data:`ORDER_GRID_LEAD_COLUMNS` moved to the front in that order.
    """
    lead = tuple(c for c in ORDER_GRID_LEAD_COLUMNS if c in ORDER_STATE_SCHEMA)
    rest = tuple(c for c in ORDER_STATE_COLUMNS if c not in lead)
    return lead + rest
