package com.fix42.dashboard.amps.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.config.ColumnType;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TableSchemaTest {

    @Test
    void columnsFollowConfigurationOrder() {
        TableSchema schema = TableSchema.of(TestConnectors.fixOrders());
        assertThat(schema.columns()).extracting(ColumnSpec::name)
                .containsExactly("ClOrdID", "Symbol", "OrderQty", "Price", "TransactTime");
        assertThat(schema.tableName()).isEqualTo("amps_orders");
        assertThat(schema.size()).isEqualTo(5);
    }

    @Test
    @DisplayName("synthetic columns are appended after the mapped fields")
    void appendsSyntheticColumnsLast() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getDeephaven().setSowKeyColumn("SowKey");
        connector.getDeephaven().setIngestTimestampColumn("IngestTs");
        TableSchema schema = TableSchema.of(connector);

        assertThat(schema.columns()).extracting(ColumnSpec::name)
                .containsExactly("ClOrdID", "Symbol", "OrderQty", "Price", "TransactTime",
                        "SowKey", "IngestTs");
        assertThat(schema.columns().get(5).origin()).isEqualTo(ColumnSpec.Origin.SOW_KEY);
        assertThat(schema.columns().get(5).type()).isEqualTo(ColumnType.STRING);
        assertThat(schema.columns().get(6).origin()).isEqualTo(ColumnSpec.Origin.INGEST_TIMESTAMP);
        assertThat(schema.columns().get(6).type()).isEqualTo(ColumnType.INSTANT);
    }

    @Test
    void keyedWhenKeyColumnsAreConfigured() {
        assertThat(TableSchema.of(TestConnectors.fixOrders()).keyed()).isTrue();
        assertThat(TableSchema.of(TestConnectors.jsonTrades()).keyed()).isFalse();
    }

    @Test
    void buildsACompositeRowKey() {
        TableSchema schema = TableSchema.of(TestConnectors.nvfixPositions());
        Object[] values = {"ACC-1", "AAPL", 100.0d, 185.5d};
        assertThat(schema.rowKey(values)).contains("ACC-1").contains("AAPL");
    }

    @Test
    void anUnkeyedSchemaHasNoRowKey() {
        TableSchema schema = TableSchema.of(TestConnectors.jsonTrades());
        assertThat(schema.rowKey(new Object[] {"T-1", "AAPL", 100L, "XNAS"})).isNull();
    }

    @Test
    @DisplayName("distinct key values must not collide through the key separator")
    void compositeKeysDoNotCollide() {
        TableSchema schema = TableSchema.of(TestConnectors.nvfixPositions());
        String first = schema.rowKey(new Object[] {"A", "B-C", 1.0d, 1.0d});
        String second = schema.rowKey(new Object[] {"A-B", "C", 1.0d, 1.0d});
        assertThat(first).isNotEqualTo(second);
    }

    @Test
    @DisplayName("a row missing a key value has no key, rather than one that reads 'null'")
    void aNullKeyValueYieldsNoRowKey() {
        TableSchema schema = TableSchema.of(TestConnectors.nvfixPositions());
        assertThat(schema.rowKey(new Object[] {null, "AAPL", 1.0d, 1.0d})).isNull();
        assertThat(schema.rowKey(new Object[] {"ACC-1", null, 1.0d, 1.0d})).isNull();
        assertThat(schema.rowKey(new Object[] {"ACC-1", "AAPL", 1.0d, 1.0d})).isNotNull();
    }

    @Test
    void indexOfRejectsUnknownColumns() {
        TableSchema schema = TableSchema.of(TestConnectors.fixOrders());
        assertThat(schema.indexOf("Price")).isEqualTo(3);
        assertThatThrownBy(() -> schema.indexOf("Nope"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("has no column 'Nope'");
    }
}
