package com.fix42.dashboard.connectors.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.connectors.TestConnectors;
import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.ConnectorValidator;
import com.fix42.dashboard.connectors.decode.RecordDecoderFactory;
import com.fix42.dashboard.connectors.deephaven.DeephavenGateway;
import com.fix42.dashboard.connectors.deephaven.DeephavenUnavailableException;
import com.fix42.dashboard.connectors.deephaven.RecordingDeephavenGateway;
import com.fix42.dashboard.connectors.mapping.TableSchema;
import com.fix42.dashboard.connectors.source.SourceRecord;
import com.fix42.dashboard.connectors.source.FakeRecordSource;
import com.fix42.dashboard.connectors.transform.RecordTransform;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.unit.DataSize;

/** The per-message pipeline: decode -> map -> (merge) -> batch -> publish. */
class ConnectorTest {

    private static final Instant INGEST = Instant.parse("2024-01-15T14:30:00Z");

    private final RecordingDeephavenGateway gateway = new RecordingDeephavenGateway();
    private final RecordDecoderFactory decoders = new RecordDecoderFactory(new ObjectMapper());
    private final FakeRecordSource subscriber = new FakeRecordSource();

    /** Where a connector's spool spills, so a test can look for (the absence of) segment files. */
    @TempDir
    Path spoolDir;

    private Connector connector(ConnectorProperties properties) {
        return connector(properties, List.of());
    }

    private Connector connector(
            ConnectorProperties properties, List<RecordTransform> transforms) {
        return connector(properties, transforms, gateway);
    }

    private Connector connector(
            ConnectorProperties properties, List<RecordTransform> transforms, DeephavenGateway target) {
        properties.getDeephaven().setFlushInterval(Duration.ofHours(1));
        properties.getSpool().setDirectory(spoolDir.toString());
        return new Connector(
                properties,
                decoders.create(properties),
                target,
                unused -> subscriber,
                unused -> transforms,
                Clock.fixed(INGEST, ZoneOffset.UTC));
    }

    /** A transform that writes {@code value} at {@code tag}, whatever was there before. */
    private static RecordTransform put(String tag, String value) {
        return (record, fields) -> {
            Map<String, String> copy = new LinkedHashMap<>(fields);
            copy.put(tag, value);
            return copy;
        };
    }

    @Test
    @DisplayName("a FIX record lands as one row of the configured columns")
    void publishesAMappedFixRecord() throws Exception {
        Connector connector = connector(TestConnectors.fixOrders());
        connector.start();
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited(
                "11", "C-1", "55", "AAPL", "38", "100", "44", "185.5",
                "60", "20240115-14:30:00"), "SOW-1"));
        connector.stop();

        assertThat(gateway.createdTables()).containsExactly("amps_orders");
        assertThat(gateway.rowsFor("amps_orders", "add")).singleElement()
                .satisfies(row -> assertThat(row).containsExactly(
                        "C-1", "AAPL", 100.0d, 185.5d, Instant.parse("2024-01-15T14:30:00Z")));
    }

    @Test
    @DisplayName("tags with no mapping never reach Deephaven")
    void ignoresUnmappedTags() throws Exception {
        Connector connector = connector(TestConnectors.fixOrders());
        connector.start();
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited(
                "11", "C-1", "58", "chatty text", "9999", "vendor extension"), "SOW-1"));
        connector.stop();

        assertThat(gateway.rowsFor("amps_orders", "add")).singleElement()
                .satisfies(row -> {
                    assertThat(row).hasSize(5);
                    assertThat(row).doesNotContain("chatty text", "vendor extension");
                });
    }

    @Test
    @DisplayName("in DELTA publish mode a partial update keeps the columns it omits")
    void mergesDeltaUpdates() throws Exception {
        Connector connector = connector(TestConnectors.nvfixPositions());
        connector.start();
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited(
                "Account", "ACC-1", "Symbol", "AAPL", "Quantity", "100", "AvgCost", "185.5"), "SOW-1"));
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited(
                "Account", "ACC-1", "Symbol", "AAPL", "Quantity", "150"), "SOW-1"));
        connector.stop();

        assertThat(gateway.rowsFor("amps_positions", "add")).hasSize(2);
        Object[] second = gateway.rowsFor("amps_positions", "add").get(1);
        assertThat(second).containsExactly("ACC-1", "AAPL", 150.0d, 185.5d);
    }

    @Test
    @DisplayName("an out-of-focus message removes the row from the keyed table")
    void publishesDeletesForRemovedRecords() throws Exception {
        Connector connector = connector(TestConnectors.fixOrders());
        connector.start();
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited("11", "C-1", "55", "AAPL"), "SOW-1"));
        subscriber.deliver(SourceRecord.delete(TestConnectors.delimited("11", "C-1"), "SOW-1"));
        connector.stop();

        assertThat(gateway.publishes()).extracting(RecordingDeephavenGateway.Publish::action)
                .containsExactly("add", "delete");
    }

    @Test
    @DisplayName("a journal topic appends every record rather than keying them")
    void appendsJournalRecords() throws Exception {
        Connector connector = connector(TestConnectors.jsonTrades());
        connector.start();
        subscriber.deliver(SourceRecord.of("{\"tradeId\":\"T-1\",\"symbol\":\"AAPL\",\"quantity\":10}"));
        subscriber.deliver(SourceRecord.of("{\"tradeId\":\"T-2\",\"symbol\":\"MSFT\",\"quantity\":20}"));
        connector.stop();

        assertThat(connector.schema().keyed()).isFalse();
        assertThat(gateway.rowsFor("amps_trades", "add")).hasSize(2);
    }

    @Test
    @DisplayName("a malformed record is counted and skipped, not fatal")
    void rejectsBadRecordsWithoutKillingTheSubscription() throws Exception {
        Connector connector = connector(TestConnectors.fixOrders());
        connector.start();
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited("11", "C-1", "38", "not-a-number"), "SOW-1"));
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited("11", "C-2", "38", "50"), "SOW-2"));
        connector.stop();

        assertThat(connector.receivedRecords()).isEqualTo(2);
        assertThat(connector.rejectedRecords()).isEqualTo(1);
        assertThat(gateway.rowsFor("amps_orders", "add")).hasSize(1);
    }

    @Test
    @DisplayName("a table can be keyed on the AMPS SOW key rather than a mapped field")
    void keysOnTheSourceKey() throws Exception {
        // The shape a SOW topic with a KeyGenerator forces: the key is assigned by AMPS and is
        // not reconstructible from the record body, so the SOW key header is the only handle.
        ConnectorProperties properties = TestConnectors.fixOrders();
        properties.getDeephaven().setSourceKeyColumn("SourceKey");
        properties.getDeephaven().setKeyColumns(List.of("SourceKey"));
        assertThat(ConnectorValidator.validate(properties)).isEmpty();

        Connector connector = connector(properties);
        connector.start();
        subscriber.deliver(SourceRecord.of(
                TestConnectors.delimited("11", "C-1", "55", "AAPL"), "1234567890123456789"));
        subscriber.deliver(SourceRecord.of(
                TestConnectors.delimited("11", "C-1", "55", "MSFT"), "1234567890123456789"));
        connector.stop();

        int sourceKeyIndex = connector.schema().indexOf("SourceKey");
        assertThat(gateway.rowsFor("amps_orders", "add"))
                .hasSize(2)
                .allSatisfy(row -> assertThat(row[sourceKeyIndex]).isEqualTo("1234567890123456789"));
    }

    @Test
    @DisplayName("an OOF delete resolves its key from the SOW key even with an empty body")
    void deletesBySourceKeyWithNoBody() throws Exception {
        ConnectorProperties properties = TestConnectors.fixOrders();
        properties.getDeephaven().setSourceKeyColumn("SourceKey");
        properties.getDeephaven().setKeyColumns(List.of("SourceKey"));

        Connector connector = connector(properties);
        connector.start();
        subscriber.deliver(SourceRecord.of(
                TestConnectors.delimited("11", "C-1", "55", "AAPL"), "987654321"));
        subscriber.deliver(SourceRecord.delete("", "987654321"));
        connector.stop();

        int sourceKeyIndex = connector.schema().indexOf("SourceKey");
        assertThat(gateway.publishes()).extracting(RecordingDeephavenGateway.Publish::action)
                .containsExactly("add", "delete");
        assertThat(gateway.rowsFor("amps_orders", "delete")).singleElement()
                .satisfies(row -> assertThat(row[sourceKeyIndex]).isEqualTo("987654321"));
    }

    @Test
    @DisplayName("records with no SOW key are rejected, not collapsed onto one row")
    void rejectsRecordsWithNoKeyValue() throws Exception {
        ConnectorProperties properties = TestConnectors.fixOrders();
        properties.getDeephaven().setSourceKeyColumn("SourceKey");
        properties.getDeephaven().setKeyColumns(List.of("SourceKey"));

        Connector connector = connector(properties);
        connector.start();
        // Two distinct records, neither carrying a SOW key.
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited("11", "C-1", "55", "AAPL")));
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited("11", "C-2", "55", "MSFT")));
        connector.stop();

        assertThat(gateway.publishes()).as("nothing keyless is published").isEmpty();
        assertThat(connector.rejectedRecords()).isEqualTo(2);
    }

    // ---- transforms ------------------------------------------------------------------

    @Test
    @DisplayName("a tag a transform derives is mapped like one the payload carried")
    void publishesATagDerivedByATransform() throws Exception {
        // The point of transforming in the tag namespace: the derived value goes through the
        // ordinary allowlist and coercion, so the mapping does not have to know it is derived.
        ConnectorProperties properties = TestConnectors.fixOrders();
        Connector connector = connector(properties, List.of(
                (record, fields) -> {
                    Map<String, String> copy = new LinkedHashMap<>(fields);
                    copy.put("44", String.valueOf(
                            Double.parseDouble(copy.getOrDefault("38", "0")) * 2));
                    return copy;
                }));
        connector.start();
        subscriber.deliver(SourceRecord.of(
                TestConnectors.delimited("11", "C-1", "55", "AAPL", "38", "100"), "SOW-1"));
        connector.stop();

        assertThat(gateway.rowsFor("amps_orders", "add")).singleElement()
                .satisfies(row -> assertThat(row[3]).isEqualTo(200.0d));
    }

    @Test
    @DisplayName("transforms are folded in the configured order, so the last write wins")
    void appliesTransformsInOrder() throws Exception {
        Connector connector = connector(TestConnectors.fixOrders(),
                List.of(put("55", "FIRST"), put("55", "SECOND")));
        connector.start();
        subscriber.deliver(SourceRecord.of(
                TestConnectors.delimited("11", "C-1", "55", "AAPL"), "SOW-1"));
        connector.stop();

        assertThat(gateway.rowsFor("amps_orders", "add")).singleElement()
                .satisfies(row -> assertThat(row[1]).isEqualTo("SECOND"));
    }

    @Test
    @DisplayName("returning null drops the record, counted apart from a rejection")
    void dropsRecordsATransformRefuses() throws Exception {
        Connector connector = connector(TestConnectors.fixOrders(), List.of(
                (record, fields) -> "AAPL".equals(fields.get("55")) ? null : fields));
        connector.start();
        subscriber.deliver(SourceRecord.of(
                TestConnectors.delimited("11", "C-1", "55", "AAPL"), "SOW-1"));
        subscriber.deliver(SourceRecord.of(
                TestConnectors.delimited("11", "C-2", "55", "MSFT"), "SOW-2"));
        connector.stop();

        assertThat(gateway.rowsFor("amps_orders", "add")).hasSize(1);
        assertThat(connector.receivedRecords()).isEqualTo(2);
        assertThat(connector.droppedRecords()).isEqualTo(1);
        assertThat(connector.rejectedRecords()).as("a decision, not a failure").isZero();
    }

    @Test
    @DisplayName("deletes go through the transforms too, so a derived key can be removed by")
    void appliesTransformsToDeletes() throws Exception {
        ConnectorProperties properties = TestConnectors.fixOrders();
        // Key on a column no payload carries: only the transform can populate it, and a
        // delete that skipped the transforms would have no key to remove.
        Connector connector = connector(properties, List.of(put("11", "DERIVED")));
        connector.start();
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited("55", "AAPL"), "SOW-1"));
        subscriber.deliver(SourceRecord.delete("", "SOW-1"));
        connector.stop();

        assertThat(gateway.publishes()).extracting(RecordingDeephavenGateway.Publish::action)
                .containsExactly("add", "delete");
        assertThat(gateway.rowsFor("amps_orders", "delete")).singleElement()
                .satisfies(row -> assertThat(row[0]).isEqualTo("DERIVED"));
    }

    @Test
    void startCreatesTheTableAndStopIsIdempotent() throws Exception {
        Connector connector = connector(TestConnectors.fixOrders());
        connector.start();
        assertThat(connector.isStarted()).isTrue();
        assertThat(gateway.createdTables()).containsExactly("amps_orders");

        connector.stop();
        connector.stop();
        assertThat(connector.isStarted()).isFalse();
        assertThat(subscriber.closeCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a restart replays from scratch, so merged state from the last life is dropped")
    void restartClearsDeltaState() throws Exception {
        Connector connector = connector(TestConnectors.nvfixPositions());
        connector.start();
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited(
                "Account", "ACC-1", "Symbol", "AAPL", "Quantity", "100", "AvgCost", "185.5"), "SOW-1"));
        connector.stop();

        connector.start();
        subscriber.deliver(SourceRecord.of(TestConnectors.delimited(
                "Account", "ACC-1", "Symbol", "AAPL", "Quantity", "150"), "SOW-1"));
        connector.stop();

        Object[] afterRestart = gateway.rowsFor("amps_positions", "add").get(1);
        assertThat(afterRestart[3]).as("AvgCost is not carried across a restart").isNull();
    }


    // ---- the spool and replay batching (s8.1) --------------------------------------------

    private static SourceRecord order(int index) {
        return SourceRecord.of(TestConnectors.delimited("11", "C-" + index, "55", "AAPL"), "SOW-" + index);
    }

    private long segmentFiles() throws Exception {
        try (Stream<Path> files = Files.walk(spoolDir)) {
            return files.filter(file -> file.getFileName().toString().endsWith(".seg")).count();
        }
    }

    @Test
    @DisplayName("a snapshot replay goes out in one large batch, then live traffic in live-sized ones")
    void publishesASnapshotReplayInLargeBatches() throws Exception {
        ConnectorProperties properties = TestConnectors.fixOrders();
        properties.getDeephaven().setMaxBatchRows(2);
        properties.getDeephaven().setReplayBatchRows(100);
        Connector connector = connector(properties);
        connector.start();

        subscriber.snapshotBegin();
        for (int i = 0; i < 5; i++) {
            subscriber.deliver(order(i));
        }
        subscriber.snapshotEnd();
        for (int i = 5; i < 8; i++) {
            subscriber.deliver(order(i));
        }
        connector.stop();

        assertThat(gateway.publishes()).extracting(publish -> publish.rows().size())
                .containsExactly(5, 2, 1);
        assertThat(gateway.rowsFor("amps_orders", "add")).extracting(row -> row[0])
                .containsExactly("C-0", "C-1", "C-2", "C-3", "C-4", "C-5", "C-6", "C-7");
        assertThat(connector.isReplaying()).isFalse();
        assertThat(connector.receivedRecords()).isEqualTo(8);
    }

    @Test
    @DisplayName("a backlog beyond memory-limit spills to segment files, gone by the time stop returns")
    void spillsToDiskAndPublishesInOrder() throws Exception {
        ConnectorProperties properties = TestConnectors.fixOrders();
        properties.getSpool().setMemoryLimit(DataSize.ofBytes(0));
        properties.getSpool().setSegmentSize(DataSize.ofKilobytes(2));
        Connector connector = connector(properties);
        connector.start();
        for (int i = 0; i < 300; i++) {
            subscriber.deliver(order(i));
        }
        connector.stop();

        assertThat(connector.receivedRecords()).isEqualTo(300);
        assertThat(connector.spilledRecords()).isEqualTo(300);
        assertThat(connector.discardedOnStop()).isZero();
        List<Object> keys = gateway.rowsFor("amps_orders", "add").stream().map(row -> row[0]).toList();
        assertThat(keys).hasSize(300).startsWith("C-0", "C-1", "C-2").endsWith("C-298", "C-299");
        assertThat(segmentFiles()).isZero();
    }

    @Test
    @DisplayName("stop() drains for stop-timeout, then discards the backlog and deletes the files")
    void stopDiscardsTheBacklogAfterTheTimeout() throws Exception {
        RecordingDeephavenGateway slow = new RecordingDeephavenGateway() {
            @Override
            public void addRows(TableSchema schema, List<Object[]> rows) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DeephavenUnavailableException("interrupted");
                }
                super.addRows(schema, rows);
            }
        };
        ConnectorProperties properties = TestConnectors.fixOrders();
        properties.getDeephaven().setMaxBatchRows(1);
        properties.getDeephaven().setReplayBatchRows(1);
        properties.getSpool().setMemoryLimit(DataSize.ofBytes(0));
        properties.getSpool().setStopTimeout(Duration.ofMillis(300));
        Connector connector = connector(properties, List.of(), slow);
        connector.start();
        for (int i = 0; i < 50; i++) {
            subscriber.deliver(order(i));
        }

        long started = System.nanoTime();
        connector.stop();
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        assertThat(took).isLessThan(Duration.ofSeconds(5));
        assertThat(connector.discardedOnStop()).isPositive();
        assertThat(connector.publishedRows() + connector.discardedOnStop()).isLessThanOrEqualTo(50);
        assertThat(segmentFiles()).isZero();
    }

    @Test
    @DisplayName("with the spool disabled, the pipeline runs on the source's thread")
    void runsThePipelineInlineWithoutASpool() throws Exception {
        ConnectorProperties properties = TestConnectors.fixOrders();
        properties.getSpool().setEnabled(false);
        Connector connector = connector(properties);
        connector.start();

        subscriber.deliver(SourceRecord.of(TestConnectors.delimited("11", "C-1", "38", "not-a-number"), "SOW-1"));
        assertThat(connector.rejectedRecords()).as("rejected before deliver returned").isEqualTo(1);

        subscriber.snapshotBegin();
        subscriber.deliver(order(2));
        assertThat(connector.isReplaying()).isTrue();
        subscriber.snapshotEnd();
        assertThat(gateway.rowsFor("amps_orders", "add")).as("ending the replay flushed inline").hasSize(1);
        assertThat(connector.spoolBacklog()).isZero();
        connector.stop();
    }
}
