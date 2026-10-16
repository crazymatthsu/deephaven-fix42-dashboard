package com.fix42.dashboard.connectors.source;

/** Callback a {@link RecordSource} invokes for every message it receives. */
@FunctionalInterface
public interface RecordHandler {

    /**
     * Handle one record. Must not throw: a source treats a thrown exception as a bug and
     * logs it, but the subscription carries on.
     *
     * @param record the received record
     */
    void onRecord(SourceRecord record);

    /**
     * The source is about to replay a complete snapshot of its state -- an AMPS SOW replay,
     * which the server brackets with {@code group_begin} and {@code group_end}. Everything up
     * to {@link #onSnapshotEnd()} is current state arriving as fast as the server can send it
     * rather than a live update, so the connector publishes it in larger batches. Optional: a
     * source with no such boundary never calls it, and nothing downstream depends on it.
     */
    default void onSnapshotBegin() {
    }

    /** The snapshot opened by {@link #onSnapshotBegin()} is complete. */
    default void onSnapshotEnd() {
    }
}
