/**
 * The Kafka driver for {@code dh-connectors}.
 *
 * <p>{@link com.fix42.dashboard.connectors.kafka.KafkaRecordSource} implements
 * {@link com.fix42.dashboard.connectors.source.RecordSource} over the Apache Kafka consumer,
 * {@link com.fix42.dashboard.connectors.kafka.KafkaSourceFactory} claims the connectors that
 * configure {@code source.kafka}, and
 * {@link com.fix42.dashboard.connectors.kafka.KafkaSourceAutoConfiguration} registers it --
 * the shape {@code :dh-connectors:source-amps} has.
 *
 * <p>The configuration it binds to
 * ({@link com.fix42.dashboard.connectors.config.KafkaSourceProperties}) carries the rules that
 * govern it: a compacted topic is state, so it defaults to a keyed table and its null-valued
 * records are tombstones that delete a key. The source assigns partitions itself and never
 * commits an offset -- the Deephaven table is the state, so the connector owns its position.
 */
package com.fix42.dashboard.connectors.kafka;
