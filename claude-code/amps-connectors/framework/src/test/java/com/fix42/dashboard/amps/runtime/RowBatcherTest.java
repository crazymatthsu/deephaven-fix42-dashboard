package com.fix42.dashboard.amps.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.deephaven.DeephavenUnavailableException;
import com.fix42.dashboard.amps.deephaven.RecordingDeephavenGateway;
import com.fix42.dashboard.amps.mapping.MappedRow;
import com.fix42.dashboard.amps.mapping.TableSchema;
import java.time.Duration;
import java.util.List;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RowBatcherTest {

    private final TableSchema schema = TableSchema.of(TestConnectors.fixOrders());
    private final RecordingDeephavenGateway gateway = new RecordingDeephavenGateway();

    private RowBatcher batcher(int maxBatchRows, Duration flushInterval) {
        return new RowBatcher("test", schema, gateway, maxBatchRows, flushInterval);
    }

    private static MappedRow row(String key, MappedRow.Action action) {
        Object[] values = {key, "AAPL", 100.0d, 185.5d, null};
        return new MappedRow(values, new boolean[values.length], action, key);
    }

    @Test
    void nothingIsPublishedUntilTheBatchIsFlushed() {
        try (RowBatcher batcher = batcher(10, Duration.ofHours(1))) {
            batcher.submit(row("C-1", MappedRow.Action.UPSERT));
            assertThat(gateway.publishes()).isEmpty();

            batcher.flush();
            assertThat(gateway.rowsFor("amps_orders", "add")).hasSize(1);
        }
    }

    @Test
    @DisplayName("a full batch publishes without waiting for the flush tick")
    void publishesWhenTheBatchIsFull() {
        try (RowBatcher batcher = batcher(3, Duration.ofHours(1))) {
            batcher.submit(row("C-1", MappedRow.Action.UPSERT));
            batcher.submit(row("C-2", MappedRow.Action.UPSERT));
            assertThat(gateway.publishes()).isEmpty();

            batcher.submit(row("C-3", MappedRow.Action.UPSERT));
            assertThat(gateway.publishes()).hasSize(1);
            assertThat(gateway.rowsFor("amps_orders", "add")).hasSize(3);
        }
    }

    @Test
    void theTimerFlushesAPartialBatch() {
        try (RowBatcher batcher = batcher(1000, Duration.ofMillis(20))) {
            batcher.start();
            batcher.submit(row("C-1", MappedRow.Action.UPSERT));

            Awaitility.await().atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(gateway.rowsFor("amps_orders", "add")).hasSize(1));
        }
    }

    @Test
    @DisplayName("upserts and deletes keep their relative order across a batch")
    void preservesOrderAcrossActionRuns() {
        try (RowBatcher batcher = batcher(100, Duration.ofHours(1))) {
            batcher.submit(row("C-1", MappedRow.Action.UPSERT));
            batcher.submit(row("C-2", MappedRow.Action.UPSERT));
            batcher.submit(row("C-1", MappedRow.Action.DELETE));
            batcher.submit(row("C-3", MappedRow.Action.UPSERT));
            batcher.flush();

            assertThat(gateway.publishes()).extracting(RecordingDeephavenGateway.Publish::action)
                    .containsExactly("add", "delete", "add");
            assertThat(gateway.publishes().get(0).rows()).hasSize(2);
            assertThat(gateway.publishes().get(1).rows()).hasSize(1);
            assertThat(gateway.publishes().get(2).rows()).hasSize(1);
        }
    }

    @Test
    @DisplayName("a failed timer flush is counted, not thrown -- the AMPS thread keeps running")
    void swallowsAndCountsFailedTimerFlushes() {
        gateway.failPublishWith(new DeephavenUnavailableException("server gone"));
        try (RowBatcher batcher = batcher(1, Duration.ofHours(1))) {
            batcher.submit(row("C-1", MappedRow.Action.UPSERT));

            assertThat(batcher.failedFlushes()).isEqualTo(1);
            assertThat(gateway.publishes()).isEmpty();
        }
    }

    @Test
    void discardDropsBufferedRows() {
        try (RowBatcher batcher = batcher(100, Duration.ofHours(1))) {
            batcher.submit(row("C-1", MappedRow.Action.UPSERT));
            batcher.discard();
            batcher.flush();
            assertThat(gateway.publishes()).isEmpty();
        }
    }

    @Test
    void countsPublishedRows() {
        try (RowBatcher batcher = batcher(100, Duration.ofHours(1))) {
            List.of("C-1", "C-2", "C-3").forEach(key -> batcher.submit(row(key, MappedRow.Action.UPSERT)));
            batcher.flush();
            assertThat(batcher.publishedRows()).isEqualTo(3);
        }
    }
}
