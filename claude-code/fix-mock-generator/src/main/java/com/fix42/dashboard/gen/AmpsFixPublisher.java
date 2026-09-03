package com.fix42.dashboard.gen;

import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.exception.AMPSException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.BiConsumer;

/**
 * Publishes serialized FIX 4.2 messages to 60East AMPS ({@code --amps-uri}, doc 10 §10).
 *
 * <p>The message payload is byte-identical to the Kafka one -- the raw SOH-delimited FIX string,
 * framing included. The <b>chain key is not sent</b>: an AMPS topic carries no per-message key, so
 * {@link #publish(String, String, String)} drops it. Nothing is lost, because ordering does not
 * depend on it here: AMPS gives a single total order per topic, while Kafka needs the key to keep
 * one chain inside one partition ({@code docs/00-overview.md} §5).
 *
 * <p>The client is an {@link HAClient} with a memory-backed publish store, named
 * {@code fix-mock-generator-<pid>} so two generators can publish at once -- AMPS drops the older
 * connection when a second one logs on under the same client name.
 *
 * <p>Checked AMPS exceptions never leave this class: a failure to connect is an
 * {@link IllegalStateException}, a failure to publish or flush an {@link UncheckedIOException},
 * both naming the URI.
 *
 * <p>{@link #close()} closes the client without flushing; the caller flushes each batch
 * ({@link GeneratorMain} does) exactly as it does for Kafka.
 */
public final class AmpsFixPublisher implements FixPublisher {

    private final String uri;
    private final BiConsumer<String, String> sink;
    private final Runnable flusher;
    private final Runnable closer;
    private long published;

    /**
     * Connects to {@code uri}, e.g. {@code tcp://localhost:29007/amps/fix} -- the transport, the
     * protocol and the message type are all part of the URI, so nothing else is configured here.
     *
     * <p>An unreachable broker is <b>retried, not reported</b>: {@code HAClient}'s default
     * reconnect strategy has no retry budget, so {@code connectAndLogon()} keeps trying (200 ms
     * backing off to 20 s) until AMPS answers. That is deliberate for a stack that starts the
     * broker and the publisher together -- and it means a mistyped URI waits instead of failing.
     * The {@link IllegalStateException} below is for the failures that <em>are</em> terminal,
     * such as a rejected logon.
     *
     * @throws IllegalStateException if the publish store cannot be created or the logon fails
     */
    public AmpsFixPublisher(String uri) {
        this.uri = uri;
        HAClient client;
        try {
            client = HAClient.createMemoryBacked(
                    "fix-mock-generator-" + ProcessHandle.current().pid());
            client.setServerChooser(new DefaultServerChooser().add(uri));
            client.connectAndLogon();
        } catch (AMPSException e) {
            throw new IllegalStateException("cannot connect to AMPS at " + uri + ": " + e, e);
        }
        this.sink = (topic, rawFix) -> {
            try {
                client.publish(topic, rawFix);
            } catch (AMPSException e) {
                throw failure(uri, "publish to topic '" + topic + "'", e);
            }
        };
        this.flusher = () -> {
            try {
                client.publishFlush();
            } catch (AMPSException e) {
                throw failure(uri, "publish flush", e);
            }
        };
        this.closer = client::close;
    }

    /**
     * Test seam: the same counting and topic routing over an arbitrary sink, so the unit tests
     * never open a socket. {@code sink} receives {@code (topic, rawFix)}.
     */
    AmpsFixPublisher(BiConsumer<String, String> sink, Runnable flusher) {
        this.uri = "in-process sink";
        this.sink = sink;
        this.flusher = flusher;
        this.closer = () -> {};
    }

    private static UncheckedIOException failure(String uri, String what, AMPSException cause) {
        return new UncheckedIOException(new IOException("AMPS " + what + " failed at " + uri, cause));
    }

    /** Sends the raw FIX string to {@code topic}; {@code chainKey} is not part of an AMPS message. */
    @Override
    public void publish(String topic, String chainKey, String rawFix) {
        sink.accept(topic, rawFix);
        published++;
    }

    @Override
    public void flush() {
        flusher.run();
    }

    @Override
    public long publishedCount() {
        return published;
    }

    /** The AMPS URI this publisher is connected to. */
    public String uri() {
        return uri;
    }

    @Override
    public void close() {
        closer.run();
    }
}
