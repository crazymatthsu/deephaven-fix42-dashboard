package com.fix42.dashboard.gen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** {@link KafkaFixPublisher} keying and lifecycle, driven through kafka-clients' {@code MockProducer}. */
class KafkaFixPublisherTest {

    private static final String TOPIC = "fix42.messages";

    private static MockProducer<String, String> mockProducer() {
        return new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    }

    @Test
    @DisplayName("every record is keyed by its chain's venue OrderID")
    void keysAreChainKeys() {
        MockProducer<String, String> producer = mockProducer();
        ScenarioEngine engine = new ScenarioEngine(42L, Instant.parse("2025-08-14T12:00:00Z"));
        ScenarioEngine.GeneratedBatch batch = engine.generate(12, ScenarioCatalog.ALL);

        try (KafkaFixPublisher publisher = new KafkaFixPublisher(producer, TOPIC)) {
            for (ScenarioEngine.EmittedMessage emitted : batch.messages()) {
                publisher.publish(emitted.chainKey(), FixSerializer.serialize(emitted.message()));
            }
            assertEquals(batch.messages().size(), publisher.publishedCount());
        }

        List<ProducerRecord<String, String>> records = producer.history();
        assertEquals(batch.messages().size(), records.size());
        for (int i = 0; i < records.size(); i++) {
            ScenarioEngine.EmittedMessage emitted = batch.messages().get(i);
            ProducerRecord<String, String> record = records.get(i);
            assertEquals(TOPIC, record.topic());
            assertEquals(emitted.chainKey(), record.key(), "key must be the chain key");
            assertEquals(FixSerializer.serialize(emitted.message()), record.value());
        }
    }

    @Test
    @DisplayName("all records of one chain share a key, and keys match the generated chains")
    void keysPartitionByChain() {
        MockProducer<String, String> producer = mockProducer();
        ScenarioEngine.GeneratedBatch batch =
                new ScenarioEngine(7L, Instant.parse("2025-08-14T12:00:00Z")).generate(10, ScenarioCatalog.ALL);

        try (KafkaFixPublisher publisher = new KafkaFixPublisher(producer, TOPIC)) {
            batch.messages().forEach(m -> publisher.publish(m.chainKey(), FixSerializer.serialize(m.message())));
        }

        Set<String> expectedKeys = batch.chains().stream()
                .map(OrderScenario::chainKey)
                .collect(Collectors.toSet());
        Set<String> actualKeys = producer.history().stream()
                .map(ProducerRecord::key)
                .collect(Collectors.toSet());
        assertEquals(expectedKeys, actualKeys);

        for (OrderScenario chain : batch.chains()) {
            long count = producer.history().stream().filter(r -> chain.chainKey().equals(r.key())).count();
            assertEquals(chain.messageCount(), count, chain.chainKey());
        }
    }

    @Test
    @DisplayName("record values are raw SOH-delimited FIX with valid framing")
    void valuesAreRawFix() {
        MockProducer<String, String> producer = mockProducer();
        ScenarioEngine.GeneratedBatch batch =
                new ScenarioEngine(3L, Instant.parse("2025-08-14T12:00:00Z")).generate(6, ScenarioCatalog.ALL);

        try (KafkaFixPublisher publisher = new KafkaFixPublisher(producer, TOPIC)) {
            batch.messages().forEach(m -> publisher.publish(m.chainKey(), FixSerializer.serialize(m.message())));
        }

        for (ProducerRecord<String, String> record : producer.history()) {
            assertTrue(record.value().startsWith("8=FIX.4.2" + FixTags.SOH));
            assertTrue(record.value().indexOf(FixTags.PIPE) < 0, "values must not be pipe-rendered");
            assertTrue(TestFix.framingValid(record.value()));
        }
    }

    @Test
    @DisplayName("close() flushes and closes the underlying producer")
    void closeFlushesAndCloses() {
        MockProducer<String, String> producer = mockProducer();
        KafkaFixPublisher publisher = new KafkaFixPublisher(producer, TOPIC);
        publisher.publish("ORD-0001", "8=FIX.4.2" + FixTags.SOH);
        publisher.flush();
        assertEquals(TOPIC, publisher.topic());
        assertEquals(1, publisher.publishedCount());
        publisher.close();
        assertTrue(producer.closed());
    }
}
