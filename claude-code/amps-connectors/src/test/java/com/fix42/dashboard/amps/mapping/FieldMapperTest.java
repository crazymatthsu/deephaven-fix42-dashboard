package com.fix42.dashboard.amps.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.source.AmpsRecord;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FieldMapperTest {

    private static final Instant INGEST = Instant.parse("2024-01-15T14:30:00Z");

    @Test
    void mapsConfiguredFieldsOntoTheirColumns() {
        TableSchema schema = TableSchema.of(TestConnectors.fixOrders());
        MappedRow row = new FieldMapper(schema).map(
                AmpsRecord.of("payload", "SOW-1"),
                Map.of("11", "C-1", "55", "AAPL", "38", "100", "44", "185.5",
                        "60", "20240115-14:30:00"),
                INGEST);

        assertThat(row.values()).containsExactly(
                "C-1", "AAPL", 100.0d, 185.5d, Instant.parse("2024-01-15T14:30:00Z"));
        assertThat(row.action()).isEqualTo(MappedRow.Action.UPSERT);
        assertThat(row.rowKey()).isEqualTo("C-1");
    }

    @Test
    @DisplayName("a field with no mapping is never published")
    void dropsUnmappedFields() {
        TableSchema schema = TableSchema.of(TestConnectors.fixOrders());
        MappedRow row = new FieldMapper(schema).map(
                AmpsRecord.of("payload"),
                Map.of("11", "C-1", "9999", "ignored", "58", "also ignored"),
                INGEST);

        assertThat(schema.columns()).extracting(ColumnSpec::name).doesNotContain("58", "9999");
        assertThat(row.values()).hasSize(5);
        assertThat(row.values()[0]).isEqualTo("C-1");
    }

    @Test
    @DisplayName("a mapped field the payload omits is null and not present")
    void marksAbsentFieldsAsNotPresent() {
        TableSchema schema = TableSchema.of(TestConnectors.fixOrders());
        MappedRow row = new FieldMapper(schema).map(
                AmpsRecord.of("payload"), Map.of("11", "C-1"), INGEST);

        assertThat(row.present()[0]).isTrue();
        assertThat(row.present()[1]).isFalse();
        assertThat(row.values()[1]).isNull();
    }

    @Test
    @DisplayName("a mapped field the payload sends empty is null but present -- an explicit clear")
    void marksBlankFieldsAsPresent() {
        TableSchema schema = TableSchema.of(TestConnectors.fixOrders());
        MappedRow row = new FieldMapper(schema).map(
                AmpsRecord.of("payload"), Map.of("11", "C-1", "55", ""), INGEST);

        assertThat(row.present()[1]).isTrue();
        assertThat(row.values()[1]).isNull();
    }

    @Test
    void populatesSyntheticColumns() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getDeephaven().setSowKeyColumn("SowKey");
        connector.getDeephaven().setIngestTimestampColumn("IngestTs");
        TableSchema schema = TableSchema.of(connector);

        MappedRow row = new FieldMapper(schema).map(
                AmpsRecord.of("payload", "SOW-42"), Map.of("11", "C-1"), INGEST);

        assertThat(row.values()[schema.indexOf("SowKey")]).isEqualTo("SOW-42");
        assertThat(row.values()[schema.indexOf("IngestTs")]).isEqualTo(INGEST);
    }

    @Test
    void carriesTheDeleteActionThrough() {
        TableSchema schema = TableSchema.of(TestConnectors.fixOrders());
        MappedRow row = new FieldMapper(schema).map(
                AmpsRecord.delete("", "SOW-1"), Map.of("11", "C-1"), INGEST);
        assertThat(row.action()).isEqualTo(MappedRow.Action.DELETE);
    }

    @Test
    void reportsWhichColumnFailedToCoerce() {
        TableSchema schema = TableSchema.of(TestConnectors.fixOrders());
        assertThatThrownBy(() -> new FieldMapper(schema).map(
                        AmpsRecord.of("payload"), Map.of("11", "C-1", "38", "many"), INGEST))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column OrderQty (tag 38)");
    }
}
