package com.fix42.dashboard.amps.source;

/** Callback an {@link AmpsSubscriber} invokes for every message it receives. */
@FunctionalInterface
public interface RecordHandler {

    /**
     * Handle one record. Must not throw: a subscriber treats a thrown exception as a bug and
     * logs it, but the subscription carries on.
     *
     * @param record the received record
     */
    void onRecord(AmpsRecord record);
}
