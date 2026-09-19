package com.fix42.dashboard.connectors.source;

/**
 * A live subscription to one upstream feed.
 *
 * <p>The seam that keeps the rest of the connector testable without a broker: a source module
 * (for example {@code :dh-connectors:source-amps}) implements one of these per driver and
 * contributes it through a {@link SourceFactory}, while {@link SimulatedSource} generates
 * records in process.
 *
 * <p>Implementations own their reconnect behaviour. {@link #start} either establishes the
 * subscription or throws; once started, a dropped connection is the implementation's problem
 * to retry, not the caller's.
 */
public interface RecordSource extends AutoCloseable {

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
