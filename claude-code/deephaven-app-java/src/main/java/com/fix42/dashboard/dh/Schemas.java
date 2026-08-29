package com.fix42.dashboard.dh;

import com.fix42.dashboard.fixcache.Columns;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Single source of truth for the published table schemas -- port of {@code dh_app.schemas}.
 *
 * <p>Column names are <b>frozen</b> by {@code docs/01-fix42-messages-and-state-machine.md} (section 4
 * for {@code order_state}, section 6 for {@code executions} / {@code order_events} /
 * {@code fix_messages}). They must match, name for name and in order, the keys returned by
 * {@code fixcache}'s {@code toRow()} methods; {@link #ORDER_STATE} and friends are therefore built
 * <em>from</em> {@link Columns}, and {@code SchemasTest} asserts the two never drift.
 *
 * <p>Each definition's column order <em>is</em> the publisher column order, and is reused verbatim
 * when {@link BlinkStream} builds per-cycle batches.
 */
public final class Schemas {

    // --------------------------------------------------------------------------------------
    // order_state -- doc 01 section 4, one row per chain, published after every message applied
    //                to that chain.
    // --------------------------------------------------------------------------------------
    public static final TableDefinition ORDER_STATE = definition(
            Map.ofEntries(
                    Map.entry("OrderQty", Kind.DOUBLE),
                    Map.entry("Price", Kind.DOUBLE),
                    Map.entry("StopPx", Kind.DOUBLE),
                    Map.entry("CumQty", Kind.DOUBLE),
                    Map.entry("LeavesQty", Kind.DOUBLE),
                    Map.entry("AvgPx", Kind.DOUBLE),
                    Map.entry("LastShares", Kind.DOUBLE),
                    Map.entry("LastPx", Kind.DOUBLE),
                    Map.entry("ExecCount", Kind.LONG),
                    Map.entry("MsgCount", Kind.LONG),
                    Map.entry("FirstSeenTs", Kind.INSTANT),
                    Map.entry("LastUpdateTs", Kind.INSTANT),
                    Map.entry("Terminal", Kind.BOOLEAN)),
            Columns.ORDER_STATE);

    // --------------------------------------------------------------------------------------
    // executions -- doc 01 section 6, one row per 35=8 (and per 35=Q); bust/correct/DK also
    //               re-emit the referenced execution's row with its new FillStatus.
    // --------------------------------------------------------------------------------------
    public static final TableDefinition EXECUTIONS = definition(
            Map.ofEntries(
                    Map.entry("LastShares", Kind.DOUBLE),
                    Map.entry("LastPx", Kind.DOUBLE),
                    Map.entry("CumQty", Kind.DOUBLE),
                    Map.entry("LeavesQty", Kind.DOUBLE),
                    Map.entry("AvgPx", Kind.DOUBLE),
                    Map.entry("IsFill", Kind.BOOLEAN),
                    Map.entry("TransactTime", Kind.INSTANT),
                    Map.entry("IngestTs", Kind.INSTANT)),
            Columns.EXECUTION);

    // --------------------------------------------------------------------------------------
    // order_events -- doc 01 section 6, one row per lifecycle event (order-history panel).
    // --------------------------------------------------------------------------------------
    public static final TableDefinition ORDER_EVENTS = definition(
            Map.of(
                    "OrderQty", Kind.DOUBLE,
                    "Price", Kind.DOUBLE,
                    "TransactTime", Kind.INSTANT,
                    "IngestTs", Kind.INSTANT),
            Columns.ORDER_EVENT);

    // --------------------------------------------------------------------------------------
    // fix_messages -- doc 01 section 6: OrderKey, MsgType, all section 2 tags as typed columns,
    //                 RawFix, ChecksumOk, SeqNum, SendingTime, IngestTs.
    // --------------------------------------------------------------------------------------
    public static final TableDefinition FIX_MESSAGES = definition(
            Map.ofEntries(
                    Map.entry("OrderQty", Kind.DOUBLE),
                    Map.entry("Price", Kind.DOUBLE),
                    Map.entry("CumQty", Kind.DOUBLE),
                    Map.entry("LeavesQty", Kind.DOUBLE),
                    Map.entry("AvgPx", Kind.DOUBLE),
                    Map.entry("LastShares", Kind.DOUBLE),
                    Map.entry("LastPx", Kind.DOUBLE),
                    Map.entry("TransactTime", Kind.INSTANT),
                    Map.entry("ChecksumOk", Kind.BOOLEAN),
                    Map.entry("SeqNum", Kind.LONG),
                    Map.entry("SendingTime", Kind.INSTANT),
                    Map.entry("IngestTs", Kind.INSTANT)),
            Columns.MESSAGE);

    // --------------------------------------------------------------------------------------
    // ingest_errors -- not a fixcache row type; produced by this layer when a message cannot be
    //                  processed (doc 03 section 2.2 failure policy).
    // --------------------------------------------------------------------------------------
    public static final List<String> INGEST_ERROR_COLUMNS = List.of("RawFix", "Error", "IngestTs");

    public static final TableDefinition INGEST_ERRORS =
            definition(Map.of("IngestTs", Kind.INSTANT), INGEST_ERROR_COLUMNS);

    /** Every publisher schema keyed by its doc 03 section 2.3 blink-table name, in build order. */
    public static final Map<String, TableDefinition> ALL;

    static {
        Map<String, TableDefinition> all = new LinkedHashMap<>();
        all.put(Names.ORDER_STATE_BLINK, ORDER_STATE);
        all.put(Names.EXECUTIONS_BLINK, EXECUTIONS);
        all.put(Names.ORDER_EVENTS_BLINK, ORDER_EVENTS);
        all.put(Names.FIX_MESSAGES_BLINK, FIX_MESSAGES);
        all.put(Names.INGEST_ERRORS, INGEST_ERRORS);
        ALL = Collections.unmodifiableMap(all);
    }

    /**
     * Boolean columns that are genuinely tri-state.
     *
     * <p>Every other boolean column is contractually always populated, so a missing value becomes
     * {@code false} -- which is what keeps {@code order_state_latest.where("!Terminal")} (doc 03
     * section 2.4) null-safe.
     */
    public static final Set<String> NULLABLE_BOOLEAN_COLUMNS = Set.of("ChecksumOk");

    /** Preferred left-hand columns of the master orders grid (doc 03 section 2.6). */
    public static final List<String> ORDER_GRID_LEAD_COLUMNS = List.of(
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
            "LastUpdateTs");

    private Schemas() {}

    /**
     * The orders-grid column order: lead columns first, then the rest.
     *
     * @return every {@link Columns#ORDER_STATE} entry exactly once, with
     *     {@link #ORDER_GRID_LEAD_COLUMNS} moved to the front in that order
     */
    public static List<String> orderGridColumns() {
        List<String> lead = ORDER_GRID_LEAD_COLUMNS.stream()
                .filter(c -> ORDER_STATE.getColumnNames().contains(c))
                .toList();
        List<String> ordered = new ArrayList<>(lead);
        Columns.ORDER_STATE.stream().filter(c -> !lead.contains(c)).forEach(ordered::add);
        return List.copyOf(ordered);
    }

    /** The column types this layer can build. */
    enum Kind {
        STRING,
        DOUBLE,
        LONG,
        BOOLEAN,
        INSTANT
    }

    /** The declared type of one column, defaulting to {@link Kind#STRING}. */
    static Kind kindOf(TableDefinition definition, String column) {
        Class<?> type = definition.getColumn(column).getDataType();
        if (type == double.class || type == Double.class) {
            return Kind.DOUBLE;
        }
        if (type == long.class || type == Long.class) {
            return Kind.LONG;
        }
        if (type == Boolean.class) {
            return Kind.BOOLEAN;
        }
        if (type == java.time.Instant.class) {
            return Kind.INSTANT;
        }
        return Kind.STRING;
    }

    /**
     * Builds a definition over {@code columns} in order, giving each the kind named in
     * {@code kinds} and defaulting the rest to string.
     *
     * <p>Listing only the non-string columns keeps these declarations short and makes the typed
     * columns -- the ones that can actually go wrong -- the ones you read.
     */
    private static TableDefinition definition(Map<String, Kind> kinds, List<String> columns) {
        List<ColumnDefinition<?>> definitions = new ArrayList<>(columns.size());
        for (String column : columns) {
            definitions.add(switch (kinds.getOrDefault(column, Kind.STRING)) {
                case DOUBLE -> ColumnDefinition.ofDouble(column);
                case LONG -> ColumnDefinition.ofLong(column);
                case BOOLEAN -> ColumnDefinition.ofBoolean(column);
                case INSTANT -> ColumnDefinition.ofTime(column);
                case STRING -> ColumnDefinition.ofString(column);
            });
        }
        return TableDefinition.of(definitions);
    }
}
