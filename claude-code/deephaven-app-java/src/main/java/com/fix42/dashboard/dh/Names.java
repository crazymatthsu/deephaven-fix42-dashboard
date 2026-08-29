package com.fix42.dashboard.dh;

import java.util.List;

/**
 * The frozen global table names the dashboard, the query API and the integration test bind to.
 *
 * <p>doc 03 sections 2.3 (publisher blink tables) and 2.4 (derived nodes). The java app exports
 * exactly these names so it is a drop-in replacement for the python one -- including for
 * {@code integration-test/test_e2e.py}, whose {@code REQUIRED_TABLES} list is
 * {@link #DERIVED_TABLES}.
 */
public final class Names {

    // --- publisher blink tables (doc 03 section 2.3) ---
    public static final String ORDER_STATE_BLINK = "order_state_blink";
    public static final String EXECUTIONS_BLINK = "executions_blink";
    public static final String ORDER_EVENTS_BLINK = "order_events_blink";
    public static final String FIX_MESSAGES_BLINK = "fix_messages_blink";
    public static final String INGEST_ERRORS = "ingest_errors";

    // --- derived nodes (doc 03 section 2.4) ---
    public static final String ORDER_STATE_LATEST = "order_state_latest";
    public static final String EXECUTIONS = "executions";
    public static final String EXECUTIONS_LATEST = "executions_latest";
    public static final String ORDER_EVENTS = "order_events";
    public static final String FIX_MESSAGES = "fix_messages";
    public static final String CLORDID_INDEX = "clordid_index";
    public static final String EXECID_INDEX = "execid_index";
    public static final String STATUS_SUMMARY = "status_summary";
    public static final String SYMBOL_SUMMARY = "symbol_summary";
    public static final String OPEN_ORDERS = "open_orders";
    public static final String ACCOUNT_LIST = "account_list";

    // --- the source ---
    public static final String FIX_RAW = "fix_raw";

    /** Every derived global, in dependency order (doc 03 section 2.4). */
    public static final List<String> DERIVED_TABLES = List.of(
            ORDER_STATE_LATEST,
            EXECUTIONS,
            EXECUTIONS_LATEST,
            ORDER_EVENTS,
            FIX_MESSAGES,
            CLORDID_INDEX,
            EXECID_INDEX,
            STATUS_SUMMARY,
            SYMBOL_SUMMARY,
            OPEN_ORDERS,
            ACCOUNT_LIST);

    private Names() {}
}
