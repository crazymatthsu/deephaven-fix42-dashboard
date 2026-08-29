package com.fix42.dashboard.fixcache;

import java.util.List;

/**
 * The frozen, ordered column lists of the published row types.
 *
 * <p>Port of the {@code *_COLUMNS} tuples in {@code fix42cache.model}. These names are frozen by
 * {@code docs/01-fix42-messages-and-state-machine.md} section 4 (OrderState) and section 6
 * (executions / order_events / fix_messages); the Deephaven layer builds its table schemas from
 * them, so they must not drift from the python package's.
 */
public final class Columns {

    /** doc 01 section 4 -- {@code order_state}. */
    public static final List<String> ORDER_STATE = List.of(
            "OrderKey",
            "OrderID",
            "ClOrdID",
            "OrigClOrdID",
            "RootClOrdID",
            "ClOrdIDChain",
            "Account",
            "Symbol",
            "Side",
            "OrdType",
            "TimeInForce",
            "OrderQty",
            "Price",
            "StopPx",
            "OrdStatus",
            "PendingAction",
            "PendingClOrdID",
            "LastExecType",
            "CumQty",
            "LeavesQty",
            "AvgPx",
            "LastShares",
            "LastPx",
            "LastMkt",
            "OrdRejReason",
            "CxlRejReason",
            "DKReason",
            "Text",
            "ExecCount",
            "MsgCount",
            "FirstSeenTs",
            "LastUpdateTs",
            "LastMsgType",
            "Terminal");

    /** doc 01 section 6 -- {@code executions}. */
    public static final List<String> EXECUTION = List.of(
            "OrderKey",
            "OrderID",
            "ClOrdID",
            "ExecID",
            "ExecRefID",
            "ExecTransType",
            "ExecType",
            "OrdStatus",
            "LastShares",
            "LastPx",
            "CumQty",
            "LeavesQty",
            "AvgPx",
            "LastMkt",
            "Text",
            "IsFill",
            "FillStatus",
            "TransactTime",
            "IngestTs");

    /** doc 01 section 6 -- {@code order_events}. */
    public static final List<String> ORDER_EVENT = List.of(
            "OrderKey",
            "ClOrdID",
            "OrigClOrdID",
            "OrderID",
            "EventType",
            "MsgType",
            "OrdStatus",
            "OrderQty",
            "Price",
            "Detail",
            "TransactTime",
            "IngestTs");

    /** doc 01 section 6 -- {@code fix_messages}. */
    public static final List<String> MESSAGE = List.of(
            "OrderKey",
            "MsgType",
            "ClOrdID",
            "OrigClOrdID",
            "OrderID",
            "ExecID",
            "ExecRefID",
            "ExecTransType",
            "ExecType",
            "OrdStatus",
            "Account",
            "Symbol",
            "Side",
            "OrderQty",
            "OrdType",
            "Price",
            "TimeInForce",
            "CumQty",
            "LeavesQty",
            "AvgPx",
            "LastShares",
            "LastPx",
            "LastMkt",
            "OrdRejReason",
            "CxlRejReason",
            "CxlRejResponseTo",
            "DKReason",
            "Text",
            "TransactTime",
            "HandlInst",
            "RawFix",
            "ChecksumOk",
            "SeqNum",
            "SendingTime",
            "IngestTs");

    private Columns() {}
}
