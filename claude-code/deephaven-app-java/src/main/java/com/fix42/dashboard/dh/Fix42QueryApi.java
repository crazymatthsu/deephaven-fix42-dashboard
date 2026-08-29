package com.fix42.dashboard.dh;

import io.deephaven.engine.table.Table;
import io.deephaven.util.SafeCloseable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Query API over the derived DAG -- doc 03 section 2.5 / doc 05 section 4, ported from
 * {@code dh_app.query_api}.
 *
 * <p>Every method returns a <b>live</b> table (still a DAG node, so callers may subscribe to it);
 * snapshotting is the caller's choice. Aliases resolve through the index tables with
 * {@code whereIn} so the resolution itself stays live and incremental rather than being frozen at
 * call time.
 *
 * <p>All identifiers are sanitized before interpolation into query strings (doc 04 section 9.7):
 * ids are generator-controlled alphanumerics, but the filters are strings compiled to Java, so
 * quoting characters are stripped regardless.
 */
public final class Fix42QueryApi {

    /** Characters that could break out of a backtick-quoted query-string literal. */
    private static final String FORBIDDEN = "`\"'\\\n\r\t";

    private final Table orderStateLatest;
    private final Table executions;
    private final Table orderEvents;
    private final Table clOrdIdIndex;
    private final Table execIdIndex;

    /**
     * Binds the API to a set of derived tables.
     *
     * @param tables the map returned by {@link Fix42Dag#buildDerived(Map)}
     */
    public Fix42QueryApi(Map<String, Table> tables) {
        this.orderStateLatest = tables.get(Names.ORDER_STATE_LATEST);
        this.executions = tables.get(Names.EXECUTIONS);
        this.orderEvents = tables.get(Names.ORDER_EVENTS);
        this.clOrdIdIndex = tables.get(Names.CLORDID_INDEX);
        this.execIdIndex = tables.get(Names.EXECID_INDEX);
    }

    /**
     * Strips quoting/escape characters from a user-supplied identifier.
     *
     * @param value any identifier ({@code OrderKey}, {@code ClOrdID}, {@code Account}, ...)
     * @return the value with backticks, quotes, backslashes and control whitespace removed, safe to
     *     interpolate into a backtick-quoted literal
     */
    public static String sanitizeId(Object value) {
        if (value == null) {
            return "";
        }
        String text = value instanceof String s ? s : String.valueOf(value);
        StringBuilder cleaned = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (FORBIDDEN.indexOf(c) < 0 && c >= ' ') {
                cleaned.append(c);
            }
        }
        return cleaned.toString().strip();
    }

    /**
     * The live cache row(s) for a venue {@code OrderID} (tag 37).
     *
     * @param orderId the venue order id
     * @return a filtered live view of {@code order_state_latest} (empty if unknown)
     */
    public Table getByOrderId(String orderId) {
        return orderStateLatest.where("OrderID == `" + sanitizeId(orderId) + "`");
    }

    /**
     * The live cache row for any {@code ClOrdID} in an amend chain.
     *
     * <p>Resolution goes through {@code clordid_index} so superseded client ids (the {@code C1} of a
     * {@code C1 -> C2 -> C3} chain) still find their order.
     *
     * @param clOrdId any client order id ever seen for the chain (tag 11)
     * @return a filtered live view of {@code order_state_latest} (empty if unknown)
     */
    public Table getByClOrdId(String clOrdId) {
        Table matches = clOrdIdIndex.where("ClOrdID == `" + sanitizeId(clOrdId) + "`");
        return whereInLocked(matches);
    }

    /**
     * The live cache row owning an {@code ExecID} (tag 17).
     *
     * @param execId the execution id
     * @return a filtered live view of {@code order_state_latest} (empty if unknown)
     */
    public Table getByExecId(String execId) {
        Table matches = execIdIndex.where("ExecID == `" + sanitizeId(execId) + "`");
        return whereInLocked(matches);
    }

    /**
     * All live cache rows for an account (tag 1).
     *
     * @param account the account identifier
     * @return a filtered live view of {@code order_state_latest}
     */
    public Table findByAccount(String account) {
        return orderStateLatest.where("Account == `" + sanitizeId(account) + "`");
    }

    /**
     * All live cache rows for a symbol (tag 55).
     *
     * @param symbol the instrument symbol
     * @return a filtered live view of {@code order_state_latest}
     */
    public Table findBySymbol(String symbol) {
        return orderStateLatest.where("Symbol == `" + sanitizeId(symbol) + "`");
    }

    /**
     * {@code order_state_latest.whereIn(matches, "OrderKey")} under the update-graph shared lock.
     *
     * <p>Unlike {@code where}/{@code lastBy}/{@code view}, {@code whereIn} snapshots its right-hand
     * side and so must not run concurrently with a cycle. These methods are called from user code
     * -- an IDE console, a client, the dashboard -- on arbitrary threads, so the lock is taken here
     * rather than assumed.
     */
    private Table whereInLocked(Table matches) {
        try (SafeCloseable ignored = orderStateLatest.getUpdateGraph().sharedLock().lockCloseable()) {
            return orderStateLatest.whereIn(matches, "OrderKey");
        }
    }

    /**
     * The three linked views for one order chain.
     *
     * @param orderKey the chain's stable {@code OrderKey} (doc 01 section 3)
     * @return {@code {"state": ..., "executions": ..., "events": ...}} -- all live tables filtered to
     *     that chain, executions and events newest first
     */
    public Map<String, Table> orderDetail(String orderKey) {
        String predicate = "OrderKey == `" + sanitizeId(orderKey) + "`";
        Map<String, Table> detail = new LinkedHashMap<>();
        detail.put("state", orderStateLatest.where(predicate));
        detail.put("executions", executions.where(predicate).sortDescending("IngestTs"));
        detail.put("events", orderEvents.where(predicate).sortDescending("IngestTs"));
        return detail;
    }
}
