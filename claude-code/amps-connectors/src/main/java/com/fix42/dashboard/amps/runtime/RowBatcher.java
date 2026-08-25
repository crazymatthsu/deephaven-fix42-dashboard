package com.fix42.dashboard.amps.runtime;

import com.fix42.dashboard.amps.deephaven.DeephavenGateway;
import com.fix42.dashboard.amps.mapping.MappedRow;
import com.fix42.dashboard.amps.mapping.TableSchema;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffers mapped rows and publishes them to Deephaven in batches.
 *
 * <p>A Flight round trip per AMPS message would be pointless overhead, so rows accumulate until
 * either {@code max-batch-rows} is reached (flushed by the submitting thread, which also
 * back-pressures a fast AMPS feed) or {@code flush-interval} elapses.
 *
 * <p>Order is preserved across the upsert/delete boundary: the buffer is published as
 * consecutive runs of a single action, never regrouped. Publishing all upserts and then all
 * deletes would resurrect a record that was deleted and re-added within one batch.
 */
public final class RowBatcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RowBatcher.class);

    private final String connectorName;
    private final TableSchema schema;
    private final DeephavenGateway gateway;
    private final int maxBatchRows;
    private final Duration flushInterval;

    private final Object bufferLock = new Object();
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();

    private List<MappedRow> buffer = new ArrayList<>();
    private ScheduledExecutorService scheduler;

    public RowBatcher(
            String connectorName,
            TableSchema schema,
            DeephavenGateway gateway,
            int maxBatchRows,
            Duration flushInterval) {
        this.connectorName = connectorName;
        this.schema = schema;
        this.gateway = gateway;
        this.maxBatchRows = maxBatchRows;
        this.flushInterval = flushInterval;
    }

    /** Start the periodic flush. */
    public void start() {
        synchronized (bufferLock) {
            if (scheduler != null) {
                return;
            }
            scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "amps-flush-" + connectorName);
                thread.setDaemon(true);
                return thread;
            });
            long periodMillis = Math.max(1L, flushInterval.toMillis());
            scheduler.scheduleWithFixedDelay(
                    this::flushQuietly, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Buffer one row, flushing immediately when the batch is full.
     *
     * @param row the mapped row
     */
    public void submit(MappedRow row) {
        boolean full;
        synchronized (bufferLock) {
            buffer.add(row);
            full = buffer.size() >= maxBatchRows;
        }
        if (full) {
            flushQuietly();
        }
    }

    /**
     * Publish everything buffered.
     *
     * @throws com.fix42.dashboard.amps.deephaven.DeephavenUnavailableException if publishing fails
     */
    public void flush() {
        List<MappedRow> pending;
        synchronized (bufferLock) {
            if (buffer.isEmpty()) {
                return;
            }
            pending = buffer;
            buffer = new ArrayList<>();
        }
        publishRuns(pending);
        published.addAndGet(pending.size());
    }

    private void publishRuns(List<MappedRow> pending) {
        int start = 0;
        while (start < pending.size()) {
            MappedRow.Action action = pending.get(start).action();
            int end = start + 1;
            while (end < pending.size() && pending.get(end).action() == action) {
                end++;
            }
            List<Object[]> values = new ArrayList<>(end - start);
            for (int i = start; i < end; i++) {
                values.add(pending.get(i).values());
            }
            if (action == MappedRow.Action.DELETE) {
                gateway.deleteRows(schema, values);
            } else {
                gateway.addRows(schema, values);
            }
            start = end;
        }
    }

    private void flushQuietly() {
        try {
            flush();
        } catch (RuntimeException e) {
            // The rows are gone, but nothing is lost for long: losing Deephaven is exactly what
            // triggers a reconnect and a full AMPS replay, which republishes current state.
            long lost = dropped.incrementAndGet();
            log.warn("[{}] flush to {} failed ({} failures so far): {}",
                    connectorName, schema.tableName(), lost, e.getMessage());
        }
    }

    /** Discard everything buffered without publishing -- used when a connector restarts. */
    public void discard() {
        synchronized (bufferLock) {
            buffer = new ArrayList<>();
        }
    }

    /** Rows published since construction. */
    public long publishedRows() {
        return published.get();
    }

    /** Failed flush attempts since construction. */
    public long failedFlushes() {
        return dropped.get();
    }

    @Override
    public void close() {
        ScheduledExecutorService current;
        synchronized (bufferLock) {
            current = scheduler;
            scheduler = null;
        }
        if (current != null) {
            current.shutdownNow();
        }
    }
}
