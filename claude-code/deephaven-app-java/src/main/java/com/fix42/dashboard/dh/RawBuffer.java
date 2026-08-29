package com.fix42.dashboard.dh;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded hand-off from the AMPS client thread to the update-graph thread -- port of
 * {@code dh_app.amps_ingest.RawBuffer}.
 *
 * <p>On overflow {@link #offer} <b>blocks</b> rather than dropping. A dropped FIX message would
 * silently corrupt the fold -- the state machine would be missing a link in an amend chain with
 * nothing able to detect it -- while blocking the AMPS reader thread is ordinary TCP backpressure,
 * which the server is built to absorb and which the HA client recovers from by resuming at its last
 * bookmark.
 */
public final class RawBuffer {

    /** One buffered message: the payload, its transaction-log position, and the ingest clock. */
    public record Row(String rawFix, String bookmark, Instant ingestTs) {}

    private final int max;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition drained = lock.newCondition();
    private List<Row> rows = new ArrayList<>();
    private boolean closed;
    private long offered;
    private long dropped;
    private long waits;

    /** Creates an empty buffer holding at most {@code maxPending} rows. */
    public RawBuffer(int maxPending) {
        this.max = Math.max(1, maxPending);
    }

    /**
     * Appends {@code row}, waiting while the buffer is full.
     *
     * @param row the row to buffer
     * @param timeoutMillis milliseconds to wait for space; {@code 0} or less waits indefinitely
     * @return true if buffered; false if the buffer was closed or the wait timed out (both counted
     *     in {@link #dropped()})
     */
    public boolean offer(Row row, long timeoutMillis) {
        lock.lock();
        try {
            if (closed) {
                dropped++;
                return false;
            }
            if (rows.size() >= max) {
                waits++;
                if (timeoutMillis > 0) {
                    long remaining = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
                    while (!closed && rows.size() >= max && remaining > 0) {
                        remaining = drained.awaitNanos(remaining);
                    }
                } else {
                    while (!closed && rows.size() >= max) {
                        drained.await();
                    }
                }
                if (closed || rows.size() >= max) {
                    dropped++;
                    return false;
                }
            }
            rows.add(row);
            offered++;
            return true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            dropped++;
            return false;
        } finally {
            lock.unlock();
        }
    }

    /** Appends {@code row}, waiting indefinitely for space. */
    public boolean offer(Row row) {
        return offer(row, 0);
    }

    /** Takes every buffered row, freeing space for blocked producers. */
    public List<Row> drain() {
        lock.lock();
        try {
            if (rows.isEmpty()) {
                return List.of();
            }
            List<Row> taken = rows;
            rows = new ArrayList<>();
            drained.signalAll();
            return taken;
        } finally {
            lock.unlock();
        }
    }

    /** Refuses further rows and releases anything blocked in {@link #offer}. */
    public void close() {
        lock.lock();
        try {
            closed = true;
            drained.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Rows buffered but not yet published. */
    public int pending() {
        lock.lock();
        try {
            return rows.size();
        } finally {
            lock.unlock();
        }
    }

    /** True once {@link #close()} has been called. */
    public boolean closed() {
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }

    /** Rows accepted since construction. */
    public long offered() {
        lock.lock();
        try {
            return offered;
        } finally {
            lock.unlock();
        }
    }

    /** Rows refused because the buffer was closed or a bounded wait expired. */
    public long dropped() {
        lock.lock();
        try {
            return dropped;
        } finally {
            lock.unlock();
        }
    }

    /** How often a producer had to wait for space -- the backpressure signal. */
    public long waits() {
        lock.lock();
        try {
            return waits;
        } finally {
            lock.unlock();
        }
    }
}
