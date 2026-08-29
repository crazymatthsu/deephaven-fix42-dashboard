package com.fix42.dashboard.dh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fix42.dashboard.fixcache.Columns;
import io.deephaven.engine.table.TableDefinition;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The published schemas against the frozen column contracts of doc 01.
 *
 * <p>The first four tests are the drift guard that matters: the Deephaven schemas and the
 * {@code fixcache} row models are separate declarations of the same contract, and a column added to
 * one but not the other would publish a silently-null column rather than failing.
 */
class SchemasTest {

    @Test
    @DisplayName("every schema's columns are exactly the fixcache row model's, in order")
    void schemasMatchTheRowModels() {
        assertEquals(Columns.ORDER_STATE, Schemas.ORDER_STATE.getColumnNames());
        assertEquals(Columns.EXECUTION, Schemas.EXECUTIONS.getColumnNames());
        assertEquals(Columns.ORDER_EVENT, Schemas.ORDER_EVENTS.getColumnNames());
        assertEquals(Columns.MESSAGE, Schemas.FIX_MESSAGES.getColumnNames());
        assertEquals(Schemas.INGEST_ERROR_COLUMNS, Schemas.INGEST_ERRORS.getColumnNames());
    }

    @Test
    void allSchemasCoversTheFivePublishedStreams() {
        assertEquals(
                List.of(
                        Names.ORDER_STATE_BLINK,
                        Names.EXECUTIONS_BLINK,
                        Names.ORDER_EVENTS_BLINK,
                        Names.FIX_MESSAGES_BLINK,
                        Names.INGEST_ERRORS),
                List.copyOf(Schemas.ALL.keySet()));
        assertSame(Schemas.ORDER_STATE, Schemas.ALL.get(Names.ORDER_STATE_BLINK));
    }

    @Test
    @DisplayName("order_state types: doc 01 section 4")
    void orderStateColumnTypes() {
        assertType(Schemas.ORDER_STATE, "OrderKey", String.class);
        assertType(Schemas.ORDER_STATE, "OrderQty", double.class);
        assertType(Schemas.ORDER_STATE, "Price", double.class);
        assertType(Schemas.ORDER_STATE, "CumQty", double.class);
        assertType(Schemas.ORDER_STATE, "ExecCount", long.class);
        assertType(Schemas.ORDER_STATE, "MsgCount", long.class);
        assertType(Schemas.ORDER_STATE, "FirstSeenTs", Instant.class);
        assertType(Schemas.ORDER_STATE, "LastUpdateTs", Instant.class);
        assertType(Schemas.ORDER_STATE, "Terminal", Boolean.class);
        // Enum-ish columns are readable strings, never codes (doc 00 section 5).
        assertType(Schemas.ORDER_STATE, "Side", String.class);
        assertType(Schemas.ORDER_STATE, "OrdStatus", String.class);
    }

    @Test
    @DisplayName("fix_messages types: the audit table's nullable numerics and tri-state boolean")
    void fixMessagesColumnTypes() {
        assertType(Schemas.FIX_MESSAGES, "OrderQty", double.class);
        assertType(Schemas.FIX_MESSAGES, "SeqNum", long.class);
        assertType(Schemas.FIX_MESSAGES, "ChecksumOk", Boolean.class);
        assertType(Schemas.FIX_MESSAGES, "TransactTime", Instant.class);
        assertType(Schemas.FIX_MESSAGES, "SendingTime", Instant.class);
        assertType(Schemas.FIX_MESSAGES, "RawFix", String.class);
    }

    @Test
    void executionsAndEventsColumnTypes() {
        assertType(Schemas.EXECUTIONS, "IsFill", Boolean.class);
        assertType(Schemas.EXECUTIONS, "LastPx", double.class);
        assertType(Schemas.EXECUTIONS, "IngestTs", Instant.class);
        assertType(Schemas.EXECUTIONS, "FillStatus", String.class);
        assertType(Schemas.ORDER_EVENTS, "OrderQty", double.class);
        assertType(Schemas.ORDER_EVENTS, "Detail", String.class);
        assertType(Schemas.INGEST_ERRORS, "IngestTs", Instant.class);
        assertType(Schemas.INGEST_ERRORS, "Error", String.class);
    }

    @Test
    @DisplayName("only ChecksumOk is tri-state; every other boolean defaults to false")
    void onlyChecksumOkIsNullable() {
        assertEquals(Set.of("ChecksumOk"), Schemas.NULLABLE_BOOLEAN_COLUMNS);
        // Terminal and IsFill are contractually populated, which is what keeps
        // order_state_latest.where("!Terminal") null-safe.
        assertFalse(Schemas.NULLABLE_BOOLEAN_COLUMNS.contains("Terminal"));
        assertFalse(Schemas.NULLABLE_BOOLEAN_COLUMNS.contains("IsFill"));
    }

    @Test
    void orderGridColumnsIsAPermutationWithTheLeadColumnsFirst() {
        List<String> grid = Schemas.orderGridColumns();
        assertEquals(new HashSet<>(Columns.ORDER_STATE), new HashSet<>(grid), "same set of columns");
        assertEquals(Columns.ORDER_STATE.size(), grid.size(), "no duplicates");
        assertEquals(Schemas.ORDER_GRID_LEAD_COLUMNS, grid.subList(0, Schemas.ORDER_GRID_LEAD_COLUMNS.size()));
        for (String lead : Schemas.ORDER_GRID_LEAD_COLUMNS) {
            assertTrue(Columns.ORDER_STATE.contains(lead), lead + " is not an order_state column");
        }
    }

    @Test
    void namesListsExactlyTheGlobalsTheIntegrationTestRequires() {
        // integration-test/test_e2e.py REQUIRED_TABLES, verbatim.
        assertEquals(
                List.of(
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
                        "account_list"),
                Names.DERIVED_TABLES);
    }

    private static void assertType(TableDefinition definition, String column, Class<?> expected) {
        Map<String, ?> byName = definition.getColumnNameMap();
        assertTrue(byName.containsKey(column), column + " missing from " + definition.getColumnNames());
        assertEquals(expected, definition.getColumn(column).getDataType(), column);
    }
}
