package com.fix42.dashboard.connectors.runtime;

import com.fix42.dashboard.connectors.deephaven.DeephavenGateway;
import com.fix42.dashboard.connectors.mapping.MappedRow;
import com.fix42.dashboard.connectors.mapping.TableSchema;
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
 * <p>A Flight round trip per message would be pointless overhead, so rows accumulate until
 * either the batch is full (flushed by the submitting thread) or {@code flush-interval}
 * elapses. How full is full depends on what is flowing: {@code max-batch-rows} for live
 * traffic, {@code replay-batch-rows} between {@link #beginReplay()} and {@link #endReplay()},
 * while a source replays its state and every round trip saved shortens the replay. During a
 * replay the timer flushes only a batch that has stopped growing since its last tick, so a
 * streaming replay goes out in full batches and a stalled one is not left unpublished.
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
    private final int replayBatchRows;
    private final Duration flushInterval;

    private final Object bufferLock = new Object();
    private final AtomicLong published = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong submitted = new AtomicLong();
    private volatile boolean replaying;
    /** Scheduler thread only: what {@link #submitted} read at the previous tick. */
    private long submittedAtLastTick;

    private List<MappedRow> buffer = new ArrayList<>();
    private ScheduledExecutorService scheduler;

    public RowBatcher(
            String connectorName,
            TableSchema schema,
            DeephavenGateway gateway,
            int maxBatchRows,
            int replayBatchRows,
            Duration flushInterval) {
        this.connectorName = connectorName;
        this.schema = schema;
        this.gateway = gateway;
        this.maxBatchRows = maxBatchRows;
        this.replayBatchRows = replayBatchRows;
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
                    this::tick, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
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
            submitted.incrementAndGet();
            full = buffer.size() >= (replaying ? replayBatchRows : maxBatchRows);
        }
        if (full) {
            flushQuietly();
        }
    }

    /**
     * A snapshot replay begins: batch by {@code replay-batch-rows} until {@link #endReplay()},
     * and let the timer leave a growing batch alone. Idempotent, so a replay restarted by a
     * reconnect mid-way simply continues.
     */
    public void beginReplay() {
        replaying = true;
    }

    /** The replay is over: publish what it left buffered and return to the live batch size. */
    public void endReplay() {
        if (!replaying) {
            return;
        }
        replaying = false;
        flushQuietly();
    }

    /** Whether rows are currently batched by {@code replay-batch-rows}. */
    public boolean isReplaying() {
        return replaying;
    }

    /**
     * Publish everything buffered.
     *
     * @throws com.fix42.dashboard.connectors.deephaven.DeephavenUnavailableException if publishing fails
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

    /** The timer: a partial batch is published unless a replay is still filling it. */
    private void tick() {
        long seen = submitted.get();
        boolean grew = seen != submittedAtLastTick;
        submittedAtLastTick = seen;
        if (replaying && grew) {
            return;
        }
        flushQuietly();
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
