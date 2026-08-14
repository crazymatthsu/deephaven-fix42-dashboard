package com.fix42.dashboard.gen;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Publishes serialized FIX 4.2 messages to Kafka.
 *
 * <p>The record key is the chain's venue {@code 37 OrderID}, so every message of one order lands in
 * one partition and per-order ordering survives the broker ({@code docs/00-overview.md} §5). The
 * record value is the raw SOH-delimited FIX string.
 */
public final class KafkaFixPublisher implements AutoCloseable {

    private final Producer<String, String> producer;
    private final String topic;
    private long published;

    /** Builds an idempotent {@link KafkaProducer} against {@code bootstrapServers}. */
    public KafkaFixPublisher(String bootstrapServers, String topic) {
        this(new KafkaProducer<>(producerConfig(bootstrapServers)), topic);
    }

    /** Wraps an existing producer; the test suite passes a {@code MockProducer} here. */
    public KafkaFixPublisher(Producer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    private static Properties producerConfig(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "fix-mock-generator");
        return props;
    }

    /** Sends one message keyed by its chain key. */
    public void publish(String chainKey, String rawFix) {
        producer.send(new ProducerRecord<>(topic, chainKey, rawFix));
        published++;
    }

    public long publishedCount() {
        return published;
    }

    public String topic() {
        return topic;
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
