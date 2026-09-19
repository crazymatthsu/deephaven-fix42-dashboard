package com.fix42.dashboard.connectors.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.KafkaSourceProperties;
import com.fix42.dashboard.connectors.config.SourceFormat;
import com.fix42.dashboard.connectors.config.SourceProperties;
import com.fix42.dashboard.connectors.source.SourceRecord;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The Kafka source against a {@link MockConsumer} -- no broker, no testcontainers.
 *
 * <p>What is worth asserting here is everything the connector decides for itself: where it
 * positions the consumer, what a tombstone turns into, and that the passthrough properties
 * really are applied last. The consumer's own behaviour is Kafka's to test.
 */
class KafkaRecordSourceTest {

    private static final String TOPIC = "trades.events";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final Node NODE = new Node(0, "localhost", 9092);

    /** The offset a mock topic's retained log starts and ends at, so a seek is observable. */
    private static final long BEGINNING = 12L;
    private static final long END = 40L;

    // ---- fixtures ------------------------------------------------------------------

    private static ConnectorProperties connector(boolean compacted, KafkaSourceProperties.From from) {
        KafkaSourceProperties kafka = new KafkaSourceProperties();
        kafka.setBootstrapServers("broker-1:9092");
        kafka.setTopic(TOPIC);
        kafka.setCompacted(compacted);
        kafka.setFrom(from);
        kafka.setPollTimeout(Duration.ofMillis(20));
        kafka.setReconnectDelay(Duration.ofMillis(50));

        SourceProperties source = new SourceProperties();
        source.setKafka(kafka);

        ConnectorProperties connector = new ConnectorProperties();
        connector.setName("trades-kafka");
        connector.setFormat(SourceFormat.JSON);
        connector.setSource(source);
        return connector;
    }

    /**
     * A {@link MockConsumer} that idles instead of spinning.
     *
     * <p>{@code MockConsumer.poll} ignores its timeout and returns immediately, so the poll
     * loop under test would busy-wait -- and because every other {@code MockConsumer} method
     * is {@code synchronized}, a busy-wait can starve the {@code wakeup()} that
     * {@code close()} depends on. Sleeping OUTSIDE the monitor (before delegating) restores
     * the blocking poll the real consumer provides.
     */
    private static final class IdlingMockConsumer extends MockConsumer<String, String> {

        IdlingMockConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public ConsumerRecords<String, String> poll(Duration timeout) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return super.poll(timeout);
        }
    }

    private static MockConsumer<String, String> mockConsumer() {
        MockConsumer<String, String> consumer = new IdlingMockConsumer();
        consumer.updatePartitions(TOPIC, List.of(
                new PartitionInfo(TOPIC, 0, NODE, new Node[] {NODE}, new Node[] {NODE})));
        consumer.updateBeginningOffsets(Map.of(PARTITION, BEGINNING));
        consumer.updateEndOffsets(Map.of(PARTITION, END));
        return consumer;
    }

    private static ConsumerRecord<String, String> record(long offset, String key, String value) {
        return new ConsumerRecord<>(TOPIC, 0, offset, key, value);
    }

    /** Start the source against {@code consumer} and wait until it has assigned and seeked. */
    private static KafkaRecordSource started(
            ConnectorProperties connector,
            MockConsumer<String, String> consumer,
            List<SourceRecord> received) {
        KafkaRecordSource source = new KafkaRecordSource(connector, () -> consumer);
        source.start(received::add);
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(source::isConnected);
        return source;
    }

    // ---- records -------------------------------------------------------------------

    @Test
    @DisplayName("an upsert reaches the handler with the message key and the payload")
    void deliversRecordsWithTheirKeyAndValue() {
        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        MockConsumer<String, String> consumer = mockConsumer();

        try (KafkaRecordSource source = started(connector(false, KafkaSourceProperties.From.EARLIEST),
                consumer, received)) {
            consumer.schedulePollTask(() -> {
                consumer.addRecord(record(BEGINNING, "T-1", "{\"tradeId\":\"T-1\"}"));
                consumer.addRecord(record(BEGINNING + 1, "T-2", "{\"tradeId\":\"T-2\"}"));
            });

            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> received.size() == 2);
            assertThat(received).extracting(SourceRecord::key).containsExactly("T-1", "T-2");
            assertThat(received).extracting(SourceRecord::data)
                    .containsExactly("{\"tradeId\":\"T-1\"}", "{\"tradeId\":\"T-2\"}");
            assertThat(received).extracting(SourceRecord::action)
                    .containsOnly(SourceRecord.Action.UPSERT);
        }
    }

    @Test
    @DisplayName("a tombstone arrives as a DELETE carrying its key and an empty payload")
    void aNullValueBecomesADelete() {
        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        MockConsumer<String, String> consumer = mockConsumer();

        try (KafkaRecordSource source = started(connector(true, KafkaSourceProperties.From.EARLIEST),
                consumer, received)) {
            consumer.schedulePollTask(() -> {
                consumer.addRecord(record(BEGINNING, "ACC-1", "{\"account\":\"ACC-1\"}"));
                consumer.addRecord(record(BEGINNING + 1, "ACC-1", null));
            });

            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> received.size() == 2);
            SourceRecord tombstone = received.get(1);
            assertThat(tombstone.action()).isEqualTo(SourceRecord.Action.DELETE);
            assertThat(tombstone.key()).isEqualTo("ACC-1");
            // Empty rather than null: the DELETE path still decodes the payload, and an
            // empty document is the quiet answer.
            assertThat(tombstone.data()).isEmpty();
        }
    }

    // ---- where it starts reading ---------------------------------------------------

    @Test
    @DisplayName("a compacted topic replays from the beginning -- the Kafka SOW replay")
    void compactedTopicsSeekToTheBeginning() {
        MockConsumer<String, String> consumer = mockConsumer();
        // LATEST, and still a replay: state has to be rebuilt whatever `from` asks for.
        ConnectorProperties connector = connector(true, KafkaSourceProperties.From.LATEST);

        try (KafkaRecordSource source =
                     started(connector, consumer, new CopyOnWriteArrayList<>())) {
            assertThat(source.replayFromBeginning()).isTrue();
            assertThat(consumer.position(PARTITION)).isEqualTo(BEGINNING);
        }
    }

    @Test
    @DisplayName("an uncompacted topic from EARLIEST replays the retained log")
    void fromEarliestSeeksToTheBeginning() {
        MockConsumer<String, String> consumer = mockConsumer();
        ConnectorProperties connector = connector(false, KafkaSourceProperties.From.EARLIEST);

        try (KafkaRecordSource source =
                     started(connector, consumer, new CopyOnWriteArrayList<>())) {
            assertThat(consumer.position(PARTITION)).isEqualTo(BEGINNING);
        }
    }

    @Test
    @DisplayName("an uncompacted topic from LATEST ignores everything already published")
    void fromLatestSeeksToTheEnd() {
        MockConsumer<String, String> consumer = mockConsumer();
        ConnectorProperties connector = connector(false, KafkaSourceProperties.From.LATEST);

        try (KafkaRecordSource source =
                     started(connector, consumer, new CopyOnWriteArrayList<>())) {
            assertThat(source.replayFromBeginning()).isFalse();
            assertThat(consumer.position(PARTITION)).isEqualTo(END);
        }
    }

    @Test
    @DisplayName("every partition of the topic is assigned -- no subscribe, no consumer group")
    void assignsEveryPartitionItself() {
        MockConsumer<String, String> consumer = new IdlingMockConsumer();
        TopicPartition second = new TopicPartition(TOPIC, 1);
        consumer.updatePartitions(TOPIC, List.of(
                new PartitionInfo(TOPIC, 0, NODE, new Node[] {NODE}, new Node[] {NODE}),
                new PartitionInfo(TOPIC, 1, NODE, new Node[] {NODE}, new Node[] {NODE})));
        consumer.updateBeginningOffsets(Map.of(PARTITION, BEGINNING, second, BEGINNING));
        consumer.updateEndOffsets(Map.of(PARTITION, END, second, END));

        try (KafkaRecordSource source = started(connector(false, KafkaSourceProperties.From.EARLIEST),
                consumer, new CopyOnWriteArrayList<>())) {
            assertThat(consumer.assignment()).containsExactlyInAnyOrder(PARTITION, second);
            assertThat(consumer.subscription()).isEmpty();
        }
    }

    // ---- configuration --------------------------------------------------------------

    @Test
    @DisplayName("the consumer is configured without a group and without auto-commit")
    void consumerConfigOwnsItsOwnOffsets() {
        Map<String, Object> config =
                new KafkaRecordSource(connector(false, KafkaSourceProperties.From.EARLIEST), null)
                        .consumerConfig();

        assertThat(config).containsEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker-1:9092")
                .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .containsEntry(ConsumerConfig.CLIENT_ID_CONFIG, "trades-kafka")
                .containsEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName())
                .containsEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName())
                .doesNotContainKey(ConsumerConfig.GROUP_ID_CONFIG);
    }

    @Test
    @DisplayName("passthrough properties are applied last, so an operator can override anything")
    void passthroughPropertiesWin() {
        ConnectorProperties connector = connector(false, KafkaSourceProperties.From.EARLIEST);
        connector.getSource().getKafka().setProperties(Map.of(
                "security.protocol", "SSL",
                ConsumerConfig.CLIENT_ID_CONFIG, "chosen-by-the-operator"));

        Map<String, Object> config = new KafkaRecordSource(connector, null).consumerConfig();

        assertThat(config).containsEntry("security.protocol", "SSL")
                .containsEntry(ConsumerConfig.CLIENT_ID_CONFIG, "chosen-by-the-operator");
    }

    // ---- lifecycle -------------------------------------------------------------------

    @Test
    @DisplayName("close() wakes the poll and stops the thread")
    void closeStopsThePollThread() {
        MockConsumer<String, String> consumer = mockConsumer();
        KafkaRecordSource source = started(connector(false, KafkaSourceProperties.From.EARLIEST),
                consumer, new CopyOnWriteArrayList<>());

        source.close();

        assertThat(source.isConnected()).isFalse();
        assertThat(consumer.closed()).as("the poll thread closes its own consumer").isTrue();
        assertThat(Thread.getAllStackTraces().keySet())
                .as("no connector thread is left behind")
                .noneMatch(thread -> thread.getName().equals("trades-kafka-kafka"));
        // Idempotent, the way ConnectorManager calls it.
        source.close();
    }
}
