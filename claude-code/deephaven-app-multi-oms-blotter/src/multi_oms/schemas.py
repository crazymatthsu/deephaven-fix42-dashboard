"""Published table schemas -- doc 09 section 4.1.

Exactly the doc 01 section 4/6 schemas with the multi-OMS columns **leading**
(before ``OrderKey``):

============================  =========================================
Stream                        Added columns
============================  =========================================
``oms_order_state_blink``     ``Oms``, ``GlobalKey``, ``ExtOrdID``
``oms_executions_blink``      ``Oms``, ``GlobalKey``
``oms_order_events_blink``    ``Oms``, ``GlobalKey``
``oms_fix_messages_blink``    ``Oms``
``oms_ingest_errors``         ``Oms``
============================  =========================================

The doc 01 column dicts are **copied verbatim** rather than imported from
``dh_app.schemas``: doc 05 section 8's module-ownership rule. ``dh_app`` is another
module's contract, and this one must not break when it re-orders a column -- the
shared truth is doc 01, not the other module's source file. The java parity golden
covers doc 01 itself, so a drift here is caught by the same doc, not by an import.

Each schema is an ordered ``dict`` mapping column name -> Deephaven dtype; the
insertion order *is* the publisher column order and is reused verbatim when
:mod:`multi_oms.pipeline` builds per-cycle batches.
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
    "STREAM_NAMES",
    "NULLABLE_BOOLEAN_COLUMNS",
]


# --------------------------------------------------------------------------------------
# oms_order_state -- doc 01 section 4 + Oms / GlobalKey / ExtOrdID.
#                    One row per chain per hub, published after every applied message.
# --------------------------------------------------------------------------------------
ORDER_STATE_SCHEMA: Dict[str, "dht.DType"] = {
    # ---- doc 09 section 4.1 additions, leading ----
    "Oms": dht.string,
    "GlobalKey": dht.string,
    "ExtOrdID": dht.string,
    # ---- doc 01 section 4, verbatim ----
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
# oms_executions -- doc 01 section 6 + Oms / GlobalKey. Per-hop tape: quantities are
#                   never summed across hubs here (issue #10), only in the recon columns.
# --------------------------------------------------------------------------------------
EXECUTIONS_SCHEMA: Dict[str, "dht.DType"] = {
    "Oms": dht.string,
    "GlobalKey": dht.string,
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
# oms_order_events -- doc 01 section 6 + Oms / GlobalKey. Also the source of every
#                     ClOrdID a chain has ever carried, for id_index (doc 09 section 5.2).
# --------------------------------------------------------------------------------------
ORDER_EVENTS_SCHEMA: Dict[str, "dht.DType"] = {
    "Oms": dht.string,
    "GlobalKey": dht.string,
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
# oms_fix_messages -- doc 01 section 6 + Oms. Tag columns follow the doc 01 section 2
#                     vocabulary table order.
# --------------------------------------------------------------------------------------
FIX_MESSAGES_SCHEMA: Dict[str, "dht.DType"] = {
    "Oms": dht.string,
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
# oms_ingest_errors -- not a fix42cache row type; produced by this module when a
#                      message cannot be processed. Carries Oms so a bad tape is
#                      attributable to the hub that produced it.
# --------------------------------------------------------------------------------------
INGEST_ERRORS_SCHEMA: Dict[str, "dht.DType"] = {
    "Oms": dht.string,
    "RawFix": dht.string,
    "Error": dht.string,
    "IngestTs": dht.Instant,
}


ORDER_STATE_COLUMNS: Tuple[str, ...] = tuple(ORDER_STATE_SCHEMA)
EXECUTIONS_COLUMNS: Tuple[str, ...] = tuple(EXECUTIONS_SCHEMA)
ORDER_EVENTS_COLUMNS: Tuple[str, ...] = tuple(ORDER_EVENTS_SCHEMA)
FIX_MESSAGES_COLUMNS: Tuple[str, ...] = tuple(FIX_MESSAGES_SCHEMA)
INGEST_ERRORS_COLUMNS: Tuple[str, ...] = tuple(INGEST_ERRORS_SCHEMA)

#: All publisher schemas keyed by the doc 09 section 4.1 blink-table name.
ALL_SCHEMAS: Dict[str, Dict[str, "dht.DType"]] = {
    "oms_order_state_blink": ORDER_STATE_SCHEMA,
    "oms_executions_blink": EXECUTIONS_SCHEMA,
    "oms_order_events_blink": ORDER_EVENTS_SCHEMA,
    "oms_fix_messages_blink": FIX_MESSAGES_SCHEMA,
    "oms_ingest_errors": INGEST_ERRORS_SCHEMA,
}

#: The published stream names, in publish order.
STREAM_NAMES: Tuple[str, ...] = tuple(ALL_SCHEMAS)

#: Boolean columns that are genuinely tri-state. Every other boolean column is
#: contractually always populated, so a missing value defaults to ``False``.
NULLABLE_BOOLEAN_COLUMNS = frozenset({"ChecksumOk"})
