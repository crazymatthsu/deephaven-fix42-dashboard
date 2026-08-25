package com.fix42.dashboard.amps.source;

/**
 * A live subscription to one AMPS topic.
 *
 * <p>The seam that keeps the rest of the connector testable without an AMPS server:
 * {@link AmpsClientSubscriber} talks to a real server, {@link SimulatedAmpsSubscriber}
 * generates records in process.
 *
 * <p>Implementations own their reconnect behaviour. {@link #start} either establishes the
 * subscription or throws; once started, a dropped connection is the implementation's problem
 * to retry, not the caller's.
 */
public interface AmpsSubscriber extends AutoCloseable {

    /**
     * Connect and subscribe, delivering every record to {@code handler}.
     *
     * @param handler the record callback
     * @throws Exception if the connection or the subscription could not be established
     */
    void start(RecordHandler handler) throws Exception;

    /** Whether the subscription is currently connected. */
    boolean isConnected();

    /** Unsubscribe and disconnect. Idempotent. */
    @Override
    void close();
}
