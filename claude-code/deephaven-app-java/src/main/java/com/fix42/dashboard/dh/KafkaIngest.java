package com.fix42.dashboard.dh;

import io.deephaven.engine.table.Table;
import io.deephaven.kafka.KafkaTools;
import java.util.Properties;

/**
 * The Kafka half of the {@code fix_raw} blink source (doc 03 section 2.1).
 *
 * <p>Split out of {@link Ingest} for the same reason {@link AmpsRawSource} is: it is the only class
 * that mentions {@code io.deephaven.kafka}, so the JVM loads that package only when the Kafka path
 * is actually taken -- and {@link Ingest}'s configuration logic stays unit-testable without either
 * client on the classpath, exactly as {@code dh_app.ingest} stays testable on a bare python.
 */
final class KafkaIngest {

    private KafkaIngest() {}

    /**
     * Builds the {@code fix_raw} blink table consuming raw FIX 4.2 strings from Kafka.
     *
     * @param bootstrap bootstrap servers
     * @param topic source topic
     * @param groupId Kafka consumer group id
     * @return a blink table with the Kafka bookkeeping columns ({@code KafkaPartition},
     *     {@code KafkaOffset}, {@code KafkaTimestamp}) plus {@code ChainKey} (message key) and
     *     {@code RawFix} (the SOH-delimited FIX 4.2 message), in that order
     */
    static Table buildFixRaw(String bootstrap, String topic, String groupId) {
        Properties config = new Properties();
        config.put("bootstrap.servers", bootstrap);
        config.put("group.id", groupId);
        // No key/value deserializer is set on purpose: simpleSpec(String.class) resolves the string
        // Serde itself. Nor is a caller-supplied Properties merged in -- a stray
        // deephaven.timestamp.column.name="" in it would silently drop KafkaTimestamp.
        return KafkaTools.consumeToTable(
                config,
                topic,
                KafkaTools.ALL_PARTITIONS,
                // Replay from offset 0 => deterministic rebuild of the whole cache.
                KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING,
                KafkaTools.Consume.simpleSpec("ChainKey", String.class),
                KafkaTools.Consume.simpleSpec("RawFix", String.class),
                KafkaTools.TableType.blink());
    }
}
