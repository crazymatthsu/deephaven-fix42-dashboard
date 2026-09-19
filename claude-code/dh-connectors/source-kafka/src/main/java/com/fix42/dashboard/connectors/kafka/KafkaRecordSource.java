package com.fix42.dashboard.connectors.kafka;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.KafkaSourceProperties;
import com.fix42.dashboard.connectors.source.RecordHandler;
import com.fix42.dashboard.connectors.source.RecordSource;
import com.fix42.dashboard.connectors.source.SourceRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RecordSource} backed by the Apache Kafka consumer.
 *
 * <p>Reads its cluster and topic settings from {@code source.kafka}; the wire format stays on
 * the connector, because it describes the payload rather than the transport. Values and keys
 * arrive as strings and go straight to the connector's decoder, exactly as an AMPS payload
 * does.
 *
 * <h2>No consumer group, no committed offsets</h2>
 *
 * <p>This source deliberately does not {@code subscribe()}. It {@link Consumer#assign assigns}
 * every partition of the topic itself, seeks, and never commits -- {@code enable.auto.commit}
 * is off and no {@code group.id} is set.
 *
 * <p>That is not a shortcut, it follows from what the connector is for: <strong>the Deephaven
 * table is the state</strong>, not a consumer group's offsets. {@code ConnectorManager}
 * restarts every connector when the Deephaven server bounces, and each restart rebuilds the
 * target table from nothing; a resume from a committed offset would then leave the table
 * holding whatever the topic happened to publish since, which is not the state of the world.
 * So the connector owns its position: rehydration builds a fresh source that re-seeks, and a
 * compacted topic replayed from the beginning is the Kafka spelling of an AMPS SOW replay.
 *
 * <p>Where it seeks follows from the same idea:
 *
 * <table border="1">
 *   <caption>Seek selection</caption>
 *   <tr><th>configuration</th><th>seek</th><th>why</th></tr>
 *   <tr><td>{@code compacted: true}</td><td>beginning</td>
 *       <td>the compacted log <em>is</em> the state; replaying it rebuilds the table</td></tr>
 *   <tr><td>{@code from: EARLIEST}</td><td>beginning</td>
 *       <td>the retained log is wanted in full, as a journal topic's {@code epoch}
 *           bookmark is</td></tr>
 *   <tr><td>{@code from: LATEST}</td><td>end</td>
 *       <td>only what happens from now on, as AMPS' {@code now} bookmark is</td></tr>
 * </table>
 *
 * <p>A reconnect goes through the same assign-and-seek, so it replays whatever the
 * configuration says to replay -- the same contract the AMPS client's automatic resubscribe
 * gives, and the reason the downstream pipeline is written to tolerate a record it has already
 * seen (a keyed table upserts it, an append-only table is a log of what arrived).
 *
 * <p>{@link #start} returns as soon as the poll thread is running, rather than connecting on
 * the caller's thread the way {@code AmpsRecordSource} does. For a transport whose normal
 * state includes "the broker or the topic is not up yet", a failed first connect is the same
 * event as a dropped one and both belong in the same backoff -- so {@link #isConnected()},
 * not a thrown {@code start}, is what reports the connection.
 */
public class KafkaRecordSource implements RecordSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaRecordSource.class);

    /** How long {@link #close()} waits for the poll thread before giving up on it. */
    private static final long CLOSE_JOIN_MILLIS = 5_000;

    private final ConnectorProperties connector;
    private final KafkaSourceProperties source;
    private final Supplier<Consumer<String, String>> consumerFactory;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);

    /** Monitor the reconnect backoff waits on, so {@link #close()} cuts it short. */
    private final Object backoff = new Object();

    private volatile Consumer<String, String> consumer;
    private volatile Thread thread;

    public KafkaRecordSource(ConnectorProperties connector) {
        this(connector, null);
    }

    /**
     * Package-private seam: hands the poll loop a consumer of the test's choosing (a
     * {@code MockConsumer}) instead of dialling a broker. Each call must return a <em>new</em>
     * consumer -- the loop closes its consumer before it reconnects.
     *
     * @param connector the connector configuration
     * @param consumerFactory builds the consumer, or {@code null} for a real
     *     {@link KafkaConsumer} built from {@link #consumerConfig()}
     */
    KafkaRecordSource(
            ConnectorProperties connector, Supplier<Consumer<String, String>> consumerFactory) {
        this.connector = connector;
        this.source = connector.getSource().getKafka();
        this.consumerFactory = consumerFactory != null
                ? consumerFactory
                : () -> new KafkaConsumer<>(consumerConfig());
    }

    @Override
    public void start(RecordHandler handler) {
        log.info("[{}] starting Kafka source: {} (topic '{}', {}, from {})",
                connector.getName(), source.getBootstrapServers(), source.getTopic(),
                source.isCompacted() ? "compacted" : "streaming", source.getFrom());
        Thread runner = new Thread(() -> run(handler), connector.getName() + "-kafka");
        runner.setDaemon(true);
        this.thread = runner;
        runner.start();
    }

    /**
     * The consumer configuration this source dials with.
     *
     * <p>Package-private because it is the honest place to assert what the connector sends to
     * the broker; the {@code properties} passthrough is applied <em>last</em>, so an operator
     * can override anything here -- including the deserializers -- without a code change.
     *
     * @return the consumer properties, in the order they are applied
     */
    Map<String, Object> consumerConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, source.getBootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // Nothing commits, so nothing may commit in the background either: an auto-commit
        // would quietly create the group state this source is built not to depend on.
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // Deliberately no group.id: assign() needs none, and its absence is what stops a
        // second instance of this connector from being handed half the partitions.
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, connector.getName());
        config.putAll(source.getProperties());
        return config;
    }

    /**
     * Connect, assign, seek, poll -- and on any failure short of {@link #close()}, do it all
     * again after the backoff. One iteration of the outer loop is one consumer's lifetime.
     */
    private void run(RecordHandler handler) {
        while (!closed.get()) {
            Consumer<String, String> client = null;
            try {
                client = consumerFactory.get();
                this.consumer = client;
                assignAndSeek(client);
                connected.set(true);
                consume(client, handler);
            } catch (WakeupException e) {
                // The only thing that wakes the poll is close().
                if (!closed.get()) {
                    log.warn("[{}] Kafka poll woken without a close", connector.getName());
                }
            } catch (Exception e) {
                log.error("[{}] Kafka source failed on topic '{}'",
                        connector.getName(), source.getTopic(), e);
            } finally {
                connected.set(false);
                this.consumer = null;
                closeQuietly(client);
            }
            if (!closed.get()) {
                log.info("[{}] reconnecting to Kafka in {} (the replay starts over)",
                        connector.getName(), source.getReconnectDelay());
                if (!sleep(source.getReconnectDelay())) {
                    break;
                }
            }
        }
        connected.set(false);
        log.info("[{}] Kafka source stopped", connector.getName());
    }

    /**
     * Take every partition of the topic and position the consumer on it.
     *
     * <p>Package-private so the position a configuration resolves to is assertable against a
     * mock consumer without a poll loop in the way.
     *
     * @param client the consumer to position
     */
    void assignAndSeek(Consumer<String, String> client) {
        List<PartitionInfo> partitions = client.partitionsFor(source.getTopic());
        if (partitions == null || partitions.isEmpty()) {
            // A topic that does not exist yet answers empty rather than failing, and an
            // assignment of nothing polls forever in silence. Treat it as a connection
            // failure so the backoff retries it: broker and topic often arrive together.
            throw new IllegalStateException("topic '" + source.getTopic()
                    + "' has no partitions on " + source.getBootstrapServers()
                    + " -- it does not exist yet, or the client cannot see it");
        }
        List<TopicPartition> assignment = new ArrayList<>(partitions.size());
        for (PartitionInfo partition : partitions) {
            assignment.add(new TopicPartition(partition.topic(), partition.partition()));
        }
        client.assign(assignment);
        if (replayFromBeginning()) {
            client.seekToBeginning(assignment);
        } else {
            client.seekToEnd(assignment);
        }
        log.info("[{}] assigned {} partition(s) of '{}', seeked to the {}",
                connector.getName(), assignment.size(), source.getTopic(),
                replayFromBeginning() ? "beginning" : "end");
    }

    /**
     * Whether this connector's table is rebuilt from the log on every start.
     *
     * @return {@code true} to seek to the beginning
     */
    boolean replayFromBeginning() {
        // stateful() is compacted(), spelled transport-independently: a compacted topic is
        // state, and state has to be replayed whatever `from` says.
        return connector.getSource().stateful()
                || source.getFrom() == KafkaSourceProperties.From.EARLIEST;
    }

    private void consume(Consumer<String, String> client, RecordHandler handler) {
        Duration pollTimeout = source.getPollTimeout();
        while (!closed.get()) {
            ConsumerRecords<String, String> records = client.poll(pollTimeout);
            for (ConsumerRecord<String, String> record : records) {
                dispatch(record, handler);
            }
        }
    }

    private void dispatch(ConsumerRecord<String, String> record, RecordHandler handler) {
        try {
            if (record.value() == null) {
                // A tombstone: key, no value. The empty payload keeps the DELETE path's
                // decode quiet -- a removal is addressed by its key columns, and on a
                // compacted topic the message key is the only thing that can carry them,
                // which is why that combination requires deephaven.source-key-column.
                handler.onRecord(SourceRecord.delete("", record.key()));
                return;
            }
            handler.onRecord(SourceRecord.of(record.value(), record.key()));
        } catch (RuntimeException e) {
            log.error("[{}] failed to handle Kafka record at {}-{}@{}", connector.getName(),
                    record.topic(), record.partition(), record.offset(), e);
        }
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public void close() {
        closed.set(true);
        connected.set(false);
        Consumer<String, String> client = this.consumer;
        if (client != null) {
            try {
                // The one consumer method that is safe to call from another thread.
                client.wakeup();
            } catch (RuntimeException e) {
                log.debug("[{}] Kafka wakeup on close failed", connector.getName(), e);
            }
        }
        synchronized (backoff) {
            backoff.notifyAll();
        }
        Thread runner = this.thread;
        this.thread = null;
        if (runner == null || runner == Thread.currentThread()) {
            return;
        }
        try {
            runner.join(CLOSE_JOIN_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        if (runner.isAlive()) {
            log.warn("[{}] Kafka poll thread did not stop within {}ms",
                    connector.getName(), CLOSE_JOIN_MILLIS);
        }
    }

    /** The consumer belongs to its poll thread, so only that thread ever closes it. */
    private void closeQuietly(Consumer<String, String> client) {
        if (client == null) {
            return;
        }
        try {
            client.close();
        } catch (RuntimeException e) {
            log.debug("[{}] Kafka consumer close failed", connector.getName(), e);
        }
    }

    /**
     * Wait out the reconnect backoff, returning early when {@link #close()} rings the monitor.
     *
     * @param delay the configured backoff
     * @return {@code false} if the source should stop instead of reconnecting
     */
    private boolean sleep(Duration delay) {
        synchronized (backoff) {
            if (closed.get()) {
                return false;
            }
            try {
                backoff.wait(Math.max(1L, delay.toMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return !closed.get();
    }
}
