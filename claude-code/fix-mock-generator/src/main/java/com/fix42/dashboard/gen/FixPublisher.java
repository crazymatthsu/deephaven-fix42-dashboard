package com.fix42.dashboard.gen;

/**
 * Sink for serialized FIX 4.2 messages: Kafka ({@link KafkaFixPublisher}) or AMPS
 * ({@link AmpsFixPublisher}).
 *
 * <p>The generator picks one implementation at startup ({@code --amps-uri} selects AMPS, the
 * default is Kafka -- {@code docs/10-deephaven-remote-uri.md} §10) and drives it identically
 * afterwards: one {@link #publish(String, String, String)} per generated message, a
 * {@link #flush()} at the end of each batch, {@link #close()} on the way out.
 *
 * <p>{@code chainKey} is the routing key that keeps one order chain in order (the venue
 * {@code 37 OrderID} in single-tape mode, the hub order's {@code D} ClOrdID in multi-OMS mode).
 * Kafka uses it as the record key; AMPS topics carry no key, so the AMPS implementation ignores
 * it -- per-topic total order is the broker's own guarantee there.
 *
 * <p>{@link #close()} narrows {@link AutoCloseable#close()} to no checked exception, so
 * try-with-resources over a {@code FixPublisher} needs no extra catch.
 */
public interface FixPublisher extends AutoCloseable {

    /** Sends one raw SOH-delimited FIX message to {@code topic}. */
    void publish(String topic, String chainKey, String rawFix);

    /** Blocks until everything published so far has reached the broker. */
    void flush();

    /** Messages accepted by {@link #publish(String, String, String)} since construction. */
    long publishedCount();

    /** Flushes and releases the underlying client. */
    @Override
    void close();
}
