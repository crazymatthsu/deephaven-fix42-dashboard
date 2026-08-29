package com.fix42.dashboard.amps.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Built-in FIX 4.2 code -> name tables, for {@code fields[].decode}.
 *
 * <p>A raw FIX enumerated value is a single character chosen for the wire, not for a reader:
 * {@code 54=1} is a buy and {@code 39=E} is a pending replace. Naming a table here publishes
 * the name instead of the code, so the Deephaven column reads {@code BUY} rather than
 * {@code 1}. A code with no entry passes through unchanged, so an unrecognised value stays
 * visible rather than turning into a null.
 *
 * <p>These are the <strong>full</strong> FIX 4.2 tables. They are deliberately not shared with
 * {@code fixcache.FixEnums} in {@code deephaven-app-java}, which narrows the same tags to the
 * subset the FIX 4.2 dashboard's state machine handles (doc 01) -- a connector bridging an
 * arbitrary AMPS topic has no such licence to drop values.
 *
 * <p>Anything not covered here, or any venue that deviates from the spec, is what
 * {@code fields[].values} is for: an inline map, applied over whichever table is named.
 */
public enum FixValueDecode {

    /** Tag 54 Side. */
    SIDE(map(
            "1", "BUY",
            "2", "SELL",
            "3", "BUY_MINUS",
            "4", "SELL_PLUS",
            "5", "SELL_SHORT",
            "6", "SELL_SHORT_EXEMPT",
            "7", "UNDISCLOSED",
            "8", "CROSS",
            "9", "CROSS_SHORT")),

    /** Tag 39 OrdStatus. */
    ORD_STATUS(map(
            "0", "NEW",
            "1", "PARTIALLY_FILLED",
            "2", "FILLED",
            "3", "DONE_FOR_DAY",
            "4", "CANCELED",
            "5", "REPLACED",
            "6", "PENDING_CANCEL",
            "7", "STOPPED",
            "8", "REJECTED",
            "9", "SUSPENDED",
            "A", "PENDING_NEW",
            "B", "CALCULATED",
            "C", "EXPIRED",
            "D", "ACCEPTED_FOR_BIDDING",
            "E", "PENDING_REPLACE")),

    /** Tag 150 ExecType. */
    EXEC_TYPE(map(
            "0", "NEW",
            "1", "PARTIAL_FILL",
            "2", "FILL",
            "3", "DONE_FOR_DAY",
            "4", "CANCELED",
            "5", "REPLACED",
            "6", "PENDING_CANCEL",
            "7", "STOPPED",
            "8", "REJECTED",
            "9", "SUSPENDED",
            "A", "PENDING_NEW",
            "B", "CALCULATED",
            "C", "EXPIRED",
            "D", "RESTATED",
            "E", "PENDING_REPLACE")),

    /** Tag 20 ExecTransType. */
    EXEC_TRANS_TYPE(map(
            "0", "NEW",
            "1", "CANCEL",
            "2", "CORRECT",
            "3", "STATUS")),

    /** Tag 40 OrdType. */
    ORD_TYPE(map(
            "1", "MARKET",
            "2", "LIMIT",
            "3", "STOP",
            "4", "STOP_LIMIT",
            "5", "MARKET_ON_CLOSE",
            "6", "WITH_OR_WITHOUT",
            "7", "LIMIT_OR_BETTER",
            "8", "LIMIT_WITH_OR_WITHOUT",
            "9", "ON_BASIS",
            "A", "ON_CLOSE",
            "B", "LIMIT_ON_CLOSE",
            "C", "FOREX_MARKET",
            "D", "PREVIOUSLY_QUOTED",
            "E", "PREVIOUSLY_INDICATED",
            "F", "FOREX_LIMIT",
            "G", "FOREX_SWAP",
            "H", "FOREX_PREVIOUSLY_QUOTED",
            "I", "FUNARI",
            "P", "PEGGED")),

    /** Tag 59 TimeInForce. */
    TIME_IN_FORCE(map(
            "0", "DAY",
            "1", "GTC",
            "2", "AT_THE_OPENING",
            "3", "IOC",
            "4", "FOK",
            "5", "GTX",
            "6", "GTD")),

    /** Tag 21 HandlInst. */
    HANDL_INST(map(
            "1", "AUTOMATED_PRIVATE",
            "2", "AUTOMATED_PUBLIC",
            "3", "MANUAL")),

    /** Tag 63 SettlmntTyp. */
    SETTLMNT_TYP(map(
            "0", "REGULAR",
            "1", "CASH",
            "2", "NEXT_DAY",
            "3", "T_PLUS_2",
            "4", "T_PLUS_3",
            "5", "T_PLUS_4",
            "6", "FUTURE",
            "7", "WHEN_ISSUED",
            "8", "SELLERS_OPTION",
            "9", "T_PLUS_5")),

    /** Tag 77 OpenClose / PositionEffect. */
    OPEN_CLOSE(map(
            "O", "OPEN",
            "C", "CLOSE",
            "R", "ROLLED",
            "F", "FIFO")),

    /** Tag 103 OrdRejReason. */
    ORD_REJ_REASON(map(
            "0", "BROKER_OPTION",
            "1", "UNKNOWN_SYMBOL",
            "2", "EXCHANGE_CLOSED",
            "3", "ORDER_EXCEEDS_LIMIT",
            "4", "TOO_LATE_TO_ENTER",
            "5", "UNKNOWN_ORDER",
            "6", "DUPLICATE_ORDER",
            "7", "DUPLICATE_OF_A_VERBALLY_COMMUNICATED_ORDER",
            "8", "STALE_ORDER")),

    /** Tag 102 CxlRejReason. */
    CXL_REJ_REASON(map(
            "0", "TOO_LATE_TO_CANCEL",
            "1", "UNKNOWN_ORDER",
            "2", "BROKER_OPTION",
            "3", "ALREADY_PENDING_CANCEL_OR_REPLACE")),

    /** Tag 434 CxlRejResponseTo. */
    CXL_REJ_RESPONSE_TO(map(
            "1", "ORDER_CANCEL_REQUEST",
            "2", "ORDER_CANCEL_REPLACE_REQUEST")),

    /** Tag 35 MsgType. */
    MSG_TYPE(map(
            "0", "HEARTBEAT",
            "1", "TEST_REQUEST",
            "2", "RESEND_REQUEST",
            "3", "REJECT",
            "4", "SEQUENCE_RESET",
            "5", "LOGOUT",
            "6", "IOI",
            "7", "ADVERTISEMENT",
            "8", "EXECUTION_REPORT",
            "9", "ORDER_CANCEL_REJECT",
            "A", "LOGON",
            "B", "NEWS",
            "C", "EMAIL",
            "D", "NEW_ORDER_SINGLE",
            "E", "NEW_ORDER_LIST",
            "F", "ORDER_CANCEL_REQUEST",
            "G", "ORDER_CANCEL_REPLACE_REQUEST",
            "H", "ORDER_STATUS_REQUEST",
            "J", "ALLOCATION",
            "K", "LIST_CANCEL_REQUEST",
            "L", "LIST_EXECUTE",
            "M", "LIST_STATUS_REQUEST",
            "N", "LIST_STATUS",
            "P", "ALLOCATION_ACK",
            "Q", "DONT_KNOW_TRADE",
            "R", "QUOTE_REQUEST",
            "S", "QUOTE",
            "T", "SETTLEMENT_INSTRUCTIONS",
            "V", "MARKET_DATA_REQUEST",
            "W", "MARKET_DATA_SNAPSHOT_FULL_REFRESH",
            "X", "MARKET_DATA_INCREMENTAL_REFRESH",
            "Y", "MARKET_DATA_REQUEST_REJECT",
            "Z", "QUOTE_CANCEL",
            "a", "QUOTE_STATUS_REQUEST",
            "b", "QUOTE_ACKNOWLEDGEMENT",
            "c", "SECURITY_DEFINITION_REQUEST",
            "d", "SECURITY_DEFINITION",
            "e", "SECURITY_STATUS_REQUEST",
            "f", "SECURITY_STATUS",
            "g", "TRADING_SESSION_STATUS_REQUEST",
            "h", "TRADING_SESSION_STATUS",
            "i", "MASS_QUOTE",
            "j", "BUSINESS_MESSAGE_REJECT",
            "k", "BID_REQUEST",
            "l", "BID_RESPONSE",
            "m", "LIST_STRIKE_PRICE"));

    private final Map<String, String> table;

    FixValueDecode(Map<String, String> table) {
        this.table = table;
    }

    /** The code -> name table, unmodifiable and in FIX's own order. */
    public Map<String, String> table() {
        return table;
    }

    private static Map<String, String> map(String... codesAndNames) {
        Map<String, String> table = new LinkedHashMap<>();
        for (int i = 0; i < codesAndNames.length; i += 2) {
            table.put(codesAndNames[i], codesAndNames[i + 1]);
        }
        // Not Map.copyOf: that discards insertion order, and table() promises FIX's own.
        return Collections.unmodifiableMap(table);
    }
}
