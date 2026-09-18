package com.fix42.dashboard.amps.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.config.ColumnType;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.DeephavenTableType;
import com.fix42.dashboard.amps.config.ExplodeProperties;
import com.fix42.dashboard.amps.config.SourceFormat;
import com.fix42.dashboard.amps.decode.JsonRecordDecoder;
import com.fix42.dashboard.amps.decode.RecordDecoder;
import com.fix42.dashboard.amps.decode.RecordDecoderFactory;
import com.fix42.dashboard.amps.source.AmpsRecord;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** {@code explode}: one row per member of an object-valued field, doc 07 section 5.4. */
class RecordExploderTest {

    private static final Instant INGEST = Instant.parse("2024-01-15T14:30:00Z");

    private final ConnectorProperties connector = TestConnectors.jsonPortfolios();
    private final TableSchema schema = TableSchema.of(connector);
    private final FieldMapper mapper = new FieldMapper(schema);
    private final RecordExploder exploder =
            new RecordExploder(connector.getExplode(), schema, mapper);
    private final RecordDecoder decoder = new JsonRecordDecoder(new ObjectMapper());

    private List<MappedRow> explode(AmpsRecord record) {
        Map<String, String> fields = record.data() == null && record.parts() == null
                ? Map.of()
                : decoder.decode(record);
        return exploder.explode(record, fields, INGEST);
    }

    private Object valueOf(MappedRow row, String column) {
        return row.values()[schema.indexOf(column)];
    }

    @Test
    @DisplayName("one record becomes one row per member")
    void explodesOneRowPerMember() {
        List<MappedRow> rows = explode(AmpsRecord.of(
                "{\"key\":\"portfolio-1\",\"value\":{"
                        + "\"AAPL\":{\"qty\":250,\"px\":187.5},"
                        + "\"MSFT\":{\"qty\":100}}}",
                "S-1"));

        assertThat(rows).hasSize(2);
        MappedRow aapl = rows.get(0);
        assertThat(valueOf(aapl, "OuterKey")).isEqualTo("portfolio-1");
        assertThat(valueOf(aapl, "Symbol")).isEqualTo("AAPL");
        assertThat(valueOf(aapl, "Qty")).isEqualTo(250.0d);
        assertThat((String) valueOf(aapl, "Position")).contains("\"qty\":250");
        assertThat(aapl.rowKey()).isNotNull();

        assertThat(valueOf(rows.get(1), "Symbol")).isEqualTo("MSFT");
        assertThat(valueOf(rows.get(1), "Qty")).isEqualTo(100.0d);
    }

    @Test
    @DisplayName("a scalar member value is addressable as '.'")
    void scalarMembersThroughDot() {
        List<MappedRow> rows = explode(AmpsRecord.of(
                "{\"key\":\"limits\",\"value\":{\"maxQty\":50000}}", "S-2"));

        assertThat(rows).hasSize(1);
        assertThat(valueOf(rows.get(0), "Symbol")).isEqualTo("maxQty");
        assertThat(valueOf(rows.get(0), "Position")).isEqualTo("50000");
        // No qty inside a bare scalar: the column is null, not an error.
        assertThat(valueOf(rows.get(0), "Qty")).isNull();
    }

    @Test
    @DisplayName("member names containing dots survive intact -- they are data, not paths")
    void memberNamesWithDotsSurvive() {
        List<MappedRow> rows = explode(AmpsRecord.of(
                "{\"key\":\"portfolio-2\",\"value\":{\"BRK.B\":{\"qty\":10}}}", "S-3"));

        assertThat(rows).hasSize(1);
        assertThat(valueOf(rows.get(0), "Symbol")).isEqualTo("BRK.B");
        assertThat(valueOf(rows.get(0), "Qty")).isEqualTo(10.0d);
    }

    @Test
    @DisplayName("a member missing from the next publish of its record is deleted")
    void vanishedMemberIsDeleted() {
        explode(AmpsRecord.of(
                "{\"key\":\"p1\",\"value\":{\"AAPL\":{\"qty\":1},\"MSFT\":{\"qty\":2}}}", "S-1"));
        List<MappedRow> rows = explode(AmpsRecord.of(
                "{\"key\":\"p1\",\"value\":{\"AAPL\":{\"qty\":9}}}", "S-1"));

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).action()).isEqualTo(MappedRow.Action.UPSERT);
        assertThat(valueOf(rows.get(0), "Symbol")).isEqualTo("AAPL");
        MappedRow delete = rows.get(1);
        assertThat(delete.action()).isEqualTo(MappedRow.Action.DELETE);
        assertThat(valueOf(delete, "Symbol")).isEqualTo("MSFT");
        assertThat(valueOf(delete, "OuterKey")).isEqualTo("p1");
        assertThat(delete.rowKey()).isNotNull();
    }

    @Test
    @DisplayName("an explicit null value is a clear: every member row is deleted")
    void explicitClearDeletesEveryMember() {
        explode(AmpsRecord.of(
                "{\"key\":\"p1\",\"value\":{\"AAPL\":{\"qty\":1},\"MSFT\":{\"qty\":2}}}", "S-1"));
        List<MappedRow> rows = explode(AmpsRecord.of("{\"key\":\"p1\",\"value\":null}", "S-1"));

        assertThat(rows).hasSize(2);
        assertThat(rows).allMatch(row -> row.action() == MappedRow.Action.DELETE);
        assertThat(rows).extracting(row -> valueOf(row, "Symbol"))
                .containsExactlyInAnyOrder("AAPL", "MSFT");
    }

    @Test
    @DisplayName("a record leaving the SOW deletes all of its tracked member rows")
    void recordDeleteDeletesTrackedMembers() {
        explode(AmpsRecord.of(
                "{\"key\":\"p1\",\"value\":{\"AAPL\":{\"qty\":1},\"MSFT\":{\"qty\":2}}}", "S-1"));
        List<MappedRow> rows = explode(AmpsRecord.delete("{\"key\":\"p1\"}", "S-1"));

        assertThat(rows).hasSize(2);
        assertThat(rows).allMatch(row -> row.action() == MappedRow.Action.DELETE);
        assertThat(exploder.trackedRecords()).isZero();
    }

    @Test
    @DisplayName("an untracked delete falls back to the members its own payload names")
    void untrackedDeleteFallsBackToThePayload() {
        List<MappedRow> rows = explode(AmpsRecord.delete(
                "{\"key\":\"p9\",\"value\":{\"NVDA\":{\"qty\":5}}}", "S-9"));

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).action()).isEqualTo(MappedRow.Action.DELETE);
        assertThat(valueOf(rows.get(0), "Symbol")).isEqualTo("NVDA");
    }

    @Test
    @DisplayName("a payload without the exploded field changes nothing")
    void absentTagPublishesNothing() {
        explode(AmpsRecord.of("{\"key\":\"p1\",\"value\":{\"AAPL\":{\"qty\":1}}}", "S-1"));
        List<MappedRow> rows = explode(AmpsRecord.of("{\"key\":\"p1\"}", "S-1"));

        assertThat(rows).isEmpty();
        assertThat(exploder.trackedRecords()).isEqualTo(1);
    }

    @Test
    @DisplayName("a non-object exploded value is refused")
    void refusesANonObjectValue() {
        assertThatThrownBy(() -> explode(AmpsRecord.of("{\"key\":\"p1\",\"value\":42}", "S-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a JSON object");
    }

    @Test
    @DisplayName("nothing is tracked for a target that cannot delete")
    void unkeyedTargetsNeverTrack() {
        ConnectorProperties untracked = TestConnectors.jsonPortfolios();
        untracked.getDeephaven().setTableType(DeephavenTableType.APPEND_ONLY);
        untracked.getDeephaven().setKeyColumns(List.of());
        TableSchema unkeyed = TableSchema.of(untracked);
        RecordExploder appendOnly = new RecordExploder(
                untracked.getExplode(), unkeyed, new FieldMapper(unkeyed));

        Map<String, String> first = decoder.decode(
                "{\"key\":\"p1\",\"value\":{\"AAPL\":{\"qty\":1},\"MSFT\":{\"qty\":2}}}");
        Map<String, String> second = decoder.decode(
                "{\"key\":\"p1\",\"value\":{\"AAPL\":{\"qty\":9}}}");
        appendOnly.explode(AmpsRecord.of("payload", "S-1"), first, INGEST);
        List<MappedRow> rows = appendOnly.explode(AmpsRecord.of("payload", "S-1"), second, INGEST);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).action()).isEqualTo(MappedRow.Action.UPSERT);
        assertThat(appendOnly.trackedRecords()).isZero();
    }

    @Test
    @DisplayName("without a SOW key, republications are matched by the record-level key columns")
    void journalRecordsAreTrackedByTheirKeyColumns() {
        ConnectorProperties journal = TestConnectors.jsonPortfolios();
        journal.getSource().setSow(false);
        journal.getDeephaven().setTableType(DeephavenTableType.KEYED);
        TableSchema keyed = TableSchema.of(journal);
        FieldMapper journalMapper = new FieldMapper(keyed);
        RecordExploder tracked = new RecordExploder(journal.getExplode(), keyed, journalMapper);

        tracked.explode(AmpsRecord.of("p"), decoder.decode(
                "{\"key\":\"p1\",\"value\":{\"AAPL\":{\"qty\":1},\"MSFT\":{\"qty\":2}}}"), INGEST);
        List<MappedRow> rows = tracked.explode(AmpsRecord.of("p"), decoder.decode(
                "{\"key\":\"p1\",\"value\":{\"MSFT\":{\"qty\":3}}}"), INGEST);

        assertThat(rows).hasSize(2);
        assertThat(rows.get(1).action()).isEqualTo(MappedRow.Action.DELETE);
        assertThat(rows.get(1).values()[keyed.indexOf("Symbol")]).isEqualTo("AAPL");
    }

    @Test
    @DisplayName("explode works behind a composite part's tag")
    void explodesACompositePartField() {
        ConnectorProperties composite = TestConnectors.jsonPortfolios();
        composite.setFormat(SourceFormat.COMPOSITE);
        composite.setCompositeParts(List.of(SourceFormat.JSON));
        composite.getSource().setMessageType("composite-json");
        composite.setFields(List.of(
                TestConnectors.field("0.key", "OuterKey", ColumnType.STRING)));
        ExplodeProperties explode = composite.getExplode();
        explode.setTag("0.value");

        TableSchema compositeSchema = TableSchema.of(composite);
        FieldMapper compositeMapper = new FieldMapper(compositeSchema);
        RecordExploder compositeExploder =
                new RecordExploder(explode, compositeSchema, compositeMapper);
        RecordDecoder compositeDecoder =
                new RecordDecoderFactory(new ObjectMapper()).create(composite);

        AmpsRecord record = AmpsRecord.composite(
                List.of("{\"key\":\"p1\",\"value\":{\"AAPL\":{\"qty\":7}}}"),
                "S-1", AmpsRecord.Action.UPSERT);
        List<MappedRow> rows =
                compositeExploder.explode(record, compositeDecoder.decode(record), INGEST);

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).values()[compositeSchema.indexOf("OuterKey")]).isEqualTo("p1");
        assertThat(rows.get(0).values()[compositeSchema.indexOf("Symbol")]).isEqualTo("AAPL");
        assertThat(rows.get(0).values()[compositeSchema.indexOf("Qty")]).isEqualTo(7.0d);
    }
}
