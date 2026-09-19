package com.fix42.dashboard.connectors.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The Kafka side of one connector: which cluster and topic, and how the topic is read.
 *
 * <p>Lives under {@code source.kafka}; its presence is what selects the Kafka source
 * ({@link SourceProperties}).
 *
 * <p>{@link #isCompacted()} is Kafka's answer to the question {@code source.amps.sow} answers
 * for AMPS -- whether the topic carries state or a stream of events -- so it is what this
 * transport reports as {@link SourceProperties#stateful()}.
 */
public class KafkaSourceProperties {

    /** Where to resume from when the consumer group has no committed offset. */
    public enum From {
        /** Replay the topic from its oldest retained record; what rehydration needs. */
        EARLIEST,
        /** Start at the end, ignoring everything already published. */
        LATEST
    }

    /** {@code host:port} list of bootstrap brokers. */
    @NotBlank
    private String bootstrapServers;

    /** The Kafka topic to consume. */
    @NotBlank
    private String topic;

    /**
     * Whether the topic is log-compacted, i.e. retains the last record per message key rather
     * than a window of the log. A compacted topic is state: a replay from the beginning is the
     * state of the world, and a null-valued record is a tombstone that removes its key.
     */
    private boolean compacted = false;

    /** Where a consumer with no committed offset starts. */
    @NotNull
    private From from = From.EARLIEST;

    /** How long a poll blocks waiting for records before returning empty. */
    @NotNull
    private Duration pollTimeout = Duration.ofMillis(500);

    /** How long to wait before retrying after a failed or dropped connection. */
    @NotNull
    private Duration reconnectDelay = Duration.ofSeconds(5);

    /**
     * Raw consumer properties, passed through untouched.
     *
     * <p>The escape hatch for everything this class does not name -- security, SSL, fetch
     * sizing -- so a cluster that needs an unusual setting does not need a new field here.
     * Credentials belong in environment placeholders, never in the config tree.
     */
    @NotNull
    private Map<String, String> properties = new LinkedHashMap<>();

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isCompacted() {
        return compacted;
    }

    public void setCompacted(boolean compacted) {
        this.compacted = compacted;
    }

    public From getFrom() {
        return from;
    }

    public void setFrom(From from) {
        this.from = from;
    }

    public Duration getPollTimeout() {
        return pollTimeout;
    }

    public void setPollTimeout(Duration pollTimeout) {
        this.pollTimeout = pollTimeout;
    }

    public Duration getReconnectDelay() {
        return reconnectDelay;
    }

    public void setReconnectDelay(Duration reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties == null ? new LinkedHashMap<>() : properties;
    }
}
