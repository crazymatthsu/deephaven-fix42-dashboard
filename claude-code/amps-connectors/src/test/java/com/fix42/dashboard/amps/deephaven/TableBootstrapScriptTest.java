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
                .contains("_amps_have = list(_amps_existing_amps_orders.definition.keys())")
                .contains("_amps_want = list(_amps_defs_amps_orders.keys())")
                .contains("if _amps_have != _amps_want:")
                .contains("raise RuntimeError(");
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
