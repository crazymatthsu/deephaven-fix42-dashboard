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
}
