package com.fix42.dashboard.amps.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Locale;

/**
 * The AMPS side of one connector: where to connect, what to subscribe to, and how.
 *
 * <p>{@link #isSow()} is the switch the whole connector pivots on (doc 07 section 3):
 * a SOW topic is replayed with {@code sow_and_subscribe} into a Deephaven <em>keyed</em>
 * table, while a journal topic is replayed from the {@code epoch} bookmark into an
 * <em>append-only</em> table.
 */
public class AmpsSourceProperties {

    /** SOH, the default FIX/NVFIX field separator. */
    public static final char SOH = (char) 0x01;

    /** Which subscriber implementation to use. */
    public enum Driver {
        /** The 60East AMPS java client -- talks to a real AMPS server. */
        AMPS,
        /** An in-process generator; for demos and tests without an AMPS server. */
        SIMULATED
    }

    private Driver driver = Driver.AMPS;

    /** AMPS server host. Ignored when {@link #getUri()} is set explicitly. */
    @NotBlank
    private String host = "localhost";

    /** AMPS server port. Ignored when {@link #getUri()} is set explicitly. */
    @Min(1)
    @Max(65535)
    private int port = 9007;

    /** AMPS transport, the URI scheme: {@code tcp} or {@code tcps}. */
    @NotBlank
    private String transport = "tcp";

    /**
     * AMPS message type in the connection URI. Defaults to the wire {@link SourceFormat}
     * lower-cased ({@code fix}, {@code nvfix}, {@code json}).
     */
    private String messageType;

    /** Full AMPS URI; when set it wins over host/port/transport/message-type. */
    private String uri;

    /** The AMPS topic to subscribe to. */
    @NotBlank
    private String topic;

    /**
     * Whether {@link #getTopic()} is a SOW topic. {@code true} replays the SOW and keys the
     * Deephaven table; {@code false} treats the topic as a journal and replays it from the
     * beginning into an append-only table.
     */
    private boolean sow = true;

    /** {@code DELTA} issues a delta subscription, so AMPS sends only changed fields. */
    @NotNull
    private UpdateMode subscriptionMode = UpdateMode.FULL;

    /** Optional AMPS content filter, e.g. {@code /Symbol = 'AAPL'}. */
    private String filter;

    /** Extra AMPS subscription options appended to the ones the connector derives. */
    private String options;

    /**
     * Bookmark for journal (non-SOW) subscriptions. Defaults to {@code epoch} -- resubscribe
     * from the beginning of the transaction log so a restart rehydrates everything.
     */
    private String bookmark = "epoch";

    /** AMPS client name; defaults to the connector name. */
    private String clientName;

    /** Field separator for FIX/NVFIX payloads. Defaults to SOH. */
    private char fieldSeparator = SOH;

    /** Timeout for connect/logon and for the subscribe command. */
    @NotNull
    private Duration timeout = Duration.ofSeconds(10);

    /** How long to wait before retrying after a failed or dropped AMPS connection. */
    @NotNull
    private Duration reconnectDelay = Duration.ofSeconds(5);

    /** Records per second the {@link Driver#SIMULATED} driver emits. */
    @Min(1)
    private int simulatedRate = 5;

    /** Distinct keys the {@link Driver#SIMULATED} driver cycles through. */
    @Min(1)
    private int simulatedKeys = 8;

    /**
     * Resolve the AMPS connection URI, building it from host/port/transport/message-type
     * unless an explicit {@code uri} was configured.
     *
     * @param format the connector's wire format, used for the default message type
     * @return an AMPS URI such as {@code tcp://localhost:9007/amps/fix}
     */
    public String resolveUri(SourceFormat format) {
        if (uri != null && !uri.isBlank()) {
            return uri.trim();
        }
        String type = (messageType == null || messageType.isBlank())
                ? format.name().toLowerCase(Locale.ROOT)
                : messageType.trim();
        return transport + "://" + host + ":" + port + "/amps/" + type;
    }

    public Driver getDriver() {
        return driver;
    }

    public void setDriver(Driver driver) {
        this.driver = driver;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getTransport() {
        return transport;
    }

    public void setTransport(String transport) {
        this.transport = transport;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isSow() {
        return sow;
    }

    public void setSow(boolean sow) {
        this.sow = sow;
    }

    public UpdateMode getSubscriptionMode() {
        return subscriptionMode;
    }

    public void setSubscriptionMode(UpdateMode subscriptionMode) {
        this.subscriptionMode = subscriptionMode;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public String getBookmark() {
        return bookmark;
    }

    public void setBookmark(String bookmark) {
        this.bookmark = bookmark;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public char getFieldSeparator() {
        return fieldSeparator;
    }

    public void setFieldSeparator(char fieldSeparator) {
        this.fieldSeparator = fieldSeparator;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public Duration getReconnectDelay() {
        return reconnectDelay;
    }

    public void setReconnectDelay(Duration reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
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
}
