package com.deephaven.fix42.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Publishes the scripted FIX 4.2 tape to Kafka. Intended for the local
 * Redpanda + Deephaven demo.
 *
 * <p>Usage: {@code FixDemoProducer [bootstrap] [topic] [delayMs]}
 */
public final class FixDemoProducer {
    public static void main(String[] args) throws Exception {
        String bootstrap = arg(args, 0, env("KAFKA_BOOTSTRAP", "localhost:19092"));
        String topic = arg(args, 1, env("FIX_TOPIC", DemoScenarios.topic()));
        long delayMs = Long.parseLong(arg(args, 2, env("FIX_DELAY_MS", "250")));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "fix42-demo-producer");

        List<String> messages = DemoScenarios.allMessages();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int seq = 0;
            for (String raw : messages) {
                String key = keyOf(raw);
                producer.send(new ProducerRecord<>(topic, key, raw)).get();
                seq++;
                System.out.printf("sent %d/%d key=%s%n", seq, messages.size(), key);
                if (delayMs > 0 && seq < messages.size()) {
                    Thread.sleep(delayMs);
                }
            }
            producer.flush();
        }
        System.out.printf("published %d FIX messages to %s @ %s%n", messages.size(), topic, bootstrap);
    }

    static String keyOf(String raw) {
        for (String pair : raw.split("\\|")) {
            if (pair.startsWith("11=")) {
                return pair.substring(3);
            }
        }
        return "fix";
    }

    private static String arg(String[] args, int i, String fallback) {
        return args.length > i && !args[i].isBlank() ? args[i] : fallback;
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }
}
