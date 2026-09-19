package com.fix42.dashboard.connectors.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import java.util.ArrayList;
import java.util.List;

/**
 * The upstream side of one connector: which driver runs it, and the settings of whichever
 * transport it speaks.
 *
 * <p>The transport-specific blocks are siblings and exactly one of them is configured
 * ({@link ConnectorValidator} enforces that): the block that is present is what picks the
 * {@code SourceFactory} that builds the connector's source, so naming two would be naming
 * two feeds for one table.
 *
 * <pre>{@code
 * source:
 *   driver: REAL
 *   amps: { host: amps-1, port: 9007, topic: Orders, sow: true }
 * }</pre>
 *
 * <p>{@code driver: SIMULATED} keeps the block -- it still says what the connector is a
 * stand-in for, and the validator's transport rules go on applying -- but nothing dials it.
 */
public class SourceProperties {

    /** Which implementation runs this source. */
    public enum Driver {
        /** The transport client for whichever block is configured. */
        REAL,
        /** An in-process generator; for demos and tests without a broker. */
        SIMULATED
    }

    private Driver driver = Driver.REAL;

    /** Records per second the {@link Driver#SIMULATED} driver emits. */
    @Min(1)
    private int simulatedRate = 5;

    /** Distinct keys the {@link Driver#SIMULATED} driver cycles through. */
    @Min(1)
    private int simulatedKeys = 8;

    /** 60East AMPS settings; non-null selects the AMPS source. */
    @Valid
    private AmpsSourceProperties amps;

    /** Kafka settings; non-null selects the Kafka source. */
    @Valid
    private KafkaSourceProperties kafka;

    /** Raw TCP settings; non-null selects the TCP source. */
    @Valid
    private TcpSourceProperties tcp;

    /**
     * Names of the transport blocks this source configures, in a stable order.
     *
     * <p>Used by the validator and by {@code SourceResolver}'s failure message: "which feed
     * did you mean" is the only useful thing to say when zero or several are present.
     *
     * @return the configured block names, e.g. {@code ["amps"]}
     */
    public List<String> configuredBlocks() {
        List<String> blocks = new ArrayList<>(3);
        if (amps != null) {
            blocks.add("amps");
        }
        if (kafka != null) {
            blocks.add("kafka");
        }
        if (tcp != null) {
            blocks.add("tcp");
        }
        return blocks;
    }

    /**
     * Whether the feed carries state rather than a stream of events.
     *
     * <p>This is the transport-independent form of the question the whole connector pivots on
     * (doc 07 section 3): a stateful feed is replayed in full on connect and keys its records,
     * so it defaults to a Deephaven <em>keyed</em> table and can express removals. An AMPS SOW
     * topic and a compacted Kafka topic are stateful; a journal topic, an uncompacted Kafka
     * topic and a TCP stream are not.
     *
     * @return {@code true} when the configured transport is replaying state
     */
    public boolean stateful() {
        if (amps != null) {
            return amps.isSow();
        }
        if (kafka != null) {
            return kafka.isCompacted();
        }
        return false;
    }

    /**
     * The update mode the subscription is issued in.
     *
     * <p>Only AMPS can send partial records ({@code delta_subscribe}); every other transport
     * delivers whole payloads, so they answer {@code FULL} and the delta rules never fire on
     * them.
     *
     * @return the AMPS subscription mode, or {@link UpdateMode#FULL}
     */
    public UpdateMode subscriptionMode() {
        return amps != null ? amps.getSubscriptionMode() : UpdateMode.FULL;
    }

    /**
     * A short label for this feed, for logs and error messages: the transport and whatever
     * it calls the thing being read.
     *
     * @return e.g. {@code amps:Orders}, {@code kafka:trades}, {@code tcp:feed-1:5001}
     */
    public String describe() {
        if (amps != null) {
            return "amps:" + amps.getTopic();
        }
        if (kafka != null) {
            return "kafka:" + kafka.getTopic();
        }
        if (tcp != null) {
            return "tcp:" + tcp.getHost() + ":" + tcp.getPort();
        }
        return "<no source>";
    }

    /**
     * Field separator for the delimited wire formats (FIX, NVFIX).
     *
     * <p>Only AMPS lets a feed override it, so anything else gets SOH -- the FIX/NVFIX
     * default the decoders assume.
     *
     * @return the separator character
     */
    public char fieldSeparator() {
        return amps != null ? amps.getFieldSeparator() : AmpsSourceProperties.SOH;
    }

    public Driver getDriver() {
        return driver;
    }

    public void setDriver(Driver driver) {
        this.driver = driver;
    }

    public int getSimulatedRate() {
        return simulatedRate;
    }

    public void setSimulatedRate(int simulatedRate) {
        this.simulatedRate = simulatedRate;
    }

    public int getSimulatedKeys() {
        return simulatedKeys;
    }

    public void setSimulatedKeys(int simulatedKeys) {
        this.simulatedKeys = simulatedKeys;
    }

    public AmpsSourceProperties getAmps() {
        return amps;
    }

    public void setAmps(AmpsSourceProperties amps) {
        this.amps = amps;
    }

    public KafkaSourceProperties getKafka() {
        return kafka;
    }

    public void setKafka(KafkaSourceProperties kafka) {
        this.kafka = kafka;
    }

    public TcpSourceProperties getTcp() {
        return tcp;
    }

    public void setTcp(TcpSourceProperties tcp) {
        this.tcp = tcp;
    }
}
