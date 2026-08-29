package com.fix42.dashboard.amps.deephaven;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.mapping.TableSchema;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TableBootstrapScriptTest {

    @Test
    @DisplayName("a keyed table is created with key_cols")
    void createsAKeyedInputTableForSowTopics() {
        String script = TableBootstrapScript.createIfMissing(
                TableSchema.of(TestConnectors.fixOrders()), "orders-fix");

        assertThat(script)
                .contains("from deephaven import input_table")
                .contains("from deephaven import dtypes as dht")
                .contains("amps_orders = input_table(col_defs=_amps_defs_amps_orders, "
                        + "key_cols=[\"ClOrdID\"])");
    }

    @Test
    @DisplayName("a journal topic's table is created append-only, without key_cols")
    void createsAnAppendOnlyInputTableForJournalTopics() {
        String script = TableBootstrapScript.createIfMissing(
                TableSchema.of(TestConnectors.jsonTrades()), "trades-json");

        assertThat(script).contains("amps_trades = input_table(col_defs=_amps_defs_amps_trades)");
        assertThat(script).doesNotContain("key_cols");
    }

    @Test
    void declaresEveryColumnWithItsDeephavenDtype() {
        String script = TableBootstrapScript.createIfMissing(
                TableSchema.of(TestConnectors.fixOrders()), "orders-fix");

        assertThat(script)
                .contains("\"ClOrdID\": dht.string,")
                .contains("\"OrderQty\": dht.double,")
                .contains("\"TransactTime\": dht.Instant,");
    }

    @Test
    @DisplayName("creation is skipped when the global already exists")
    void guardsCreationBehindANameLookup() {
        String script = TableBootstrapScript.createIfMissing(
                TableSchema.of(TestConnectors.fixOrders()), "orders-fix");

        assertThat(script)
                .contains("try:")
                .contains("_amps_existing_amps_orders = amps_orders")
                .contains("except NameError:")
                .contains("_amps_existing_amps_orders = None")
                .contains("if _amps_existing_amps_orders is None:");
    }

    @Test
    @DisplayName("an existing table with different columns is an error, not a silent mismatch")
    void checksTheColumnsOfAnExistingTable() {
        String script = TableBootstrapScript.createIfMissing(
                TableSchema.of(TestConnectors.fixOrders()), "orders-fix");

        assertThat(script)
                .contains("_amps_have_amps_orders = list(_amps_existing_amps_orders"
                        + ".definition.keys())")
                .contains("_amps_want_amps_orders = list(_amps_defs_amps_orders.keys())")
                .contains("if _amps_have_amps_orders != _amps_want_amps_orders:")
                .contains("raise RuntimeError(");
    }

    @Test
    @DisplayName("...and so are its column types and its keys")
    void checksTheTypesAndKeysOfAnExistingTable() {
        String script = TableBootstrapScript.createIfMissing(
                TableSchema.of(TestConnectors.fixOrders()), "orders-fix");

        assertThat(script)
                .contains("_amps_existing_amps_orders.definition[n].data_type.j_name "
                        + "!= _amps_defs_amps_orders[n].j_name")
                .contains("disagrees with the connector on the type of column(s)")
                .contains("_amps_keys_amps_orders = list(getattr(_amps_existing_amps_orders, "
                        + "\"key_names\", []))")
                .contains("if _amps_keys_amps_orders != [\"ClOrdID\"]:")
                .contains("is keyed on %s but the connector is configured for %s");
    }

    @Test
    @DisplayName("a blink table is created through a table publisher, not input_table")
    void createsABlinkTableThroughAPublisher() {
        String script = TableBootstrapScript.createIfMissing(
                TableSchema.of(TestConnectors.jsonTicks()), "ticks-json");

        assertThat(script)
                .contains("from deephaven.stream.table_publisher import table_publisher")
                .contains("_amps_blink_amps_ticks, _amps_pub_amps_ticks = table_publisher("
                        + "\"amps-connectors:amps_ticks\", _amps_defs_amps_ticks)")
                .contains("amps_ticks = _amps_blink_amps_ticks")
                .doesNotContain("input_table")
                .doesNotContain("ring_table");
    }

    @Test
    @DisplayName("a ring table publishes the ring, and keeps the blink table reachable")
    void createsARingTableOverTheBlinkTable() {
        String script = TableBootstrapScript.createIfMissing(
                TableSchema.of(TestConnectors.jsonTicksRing(5_000)), "ticks-json-ring");

        assertThat(script)
                .contains("from deephaven import ring_table")
                .contains("_amps_blink_amps_ticks_ring, _amps_pub_amps_ticks_ring = "
                        + "table_publisher(")
                .contains("amps_ticks_ring = ring_table(_amps_blink_amps_ticks_ring, 5000)")
                .contains("created ring table amps_ticks_ring (capacity 5000)");
    }

    @Test
    @DisplayName("a blink table we do not hold the publisher for cannot be adopted")
    void refusesToAdoptAForeignBlinkTable() {
        String script = TableBootstrapScript.createIfMissing(
                TableSchema.of(TestConnectors.jsonTicks()), "ticks-json");

        assertThat(script)
                .contains("elif \"_amps_pub_amps_ticks\" not in globals():")
                .contains("already exists but was not created by this connector");
    }

    @Test
    @DisplayName("one batch per publish, so overlapping flushes cannot clobber each other")
    void namesEachPublishedBatchUniquely() {
        assertThat(TableBootstrapScript.batchVariable("amps_ticks", 7))
                .isEqualTo("_amps_batch_amps_ticks_7")
                .isNotEqualTo(TableBootstrapScript.batchVariable("amps_ticks", 8));

        assertThat(TableBootstrapScript.publishBatch("amps_ticks", "_amps_batch_amps_ticks_7"))
                .isEqualTo("try:\n"
                        + "    _amps_pub_amps_ticks.add(_amps_batch_amps_ticks_7)\n"
                        + "finally:\n"
                        + "    del _amps_batch_amps_ticks_7\n");
    }

    @Test
    @DisplayName("column order in the generated defs matches the publish order")
    void columnOrderMatchesTheSchema() {
        TableSchema schema = TableSchema.of(TestConnectors.fixOrders());
        String script = TableBootstrapScript.createIfMissing(schema, "orders-fix");

        int previous = -1;
        for (var column : schema.columns()) {
            int position = script.indexOf("\"" + column.name() + "\": ");
            assertThat(position).as("%s is declared", column.name()).isGreaterThan(previous);
            previous = position;
        }
    }

    @Test
    void probeRaisesForEveryMissingTable() {
        String script = TableBootstrapScript.probe(List.of("amps_orders", "amps_trades"));

        assertThat(script)
                .contains("[\"amps_orders\", \"amps_trades\"]")
                .contains("if n not in globals()")
                .contains("raise RuntimeError(");
    }

    @Test
    @DisplayName("text interpolated into python is escaped")
    void escapesQuotesInGeneratedStrings() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        String script = TableBootstrapScript.createIfMissing(
                TableSchema.of(connector), "he said \"hi\"");
        assertThat(script).contains("he said \\\"hi\\\"");
    }
}
