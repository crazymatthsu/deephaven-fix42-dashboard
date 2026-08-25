package com.deephaven.fix42.amps.config;

import jakarta.validation.Valid;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Validated
@ConfigurationProperties(prefix = "amps")
public class AmpsProperties {
    /** When false, the process starts but does not connect to AMPS or Deephaven. */
    private boolean autoStart = true;

    private String defaultHost = "localhost";
    private int defaultPort = 9007;
    private Duration reconnectBackoff = Duration.ofSeconds(2);
    private Duration reconnectMaxBackoff = Duration.ofSeconds(30);

    @Valid
    private List<ConnectorProperties> connectors = new ArrayList<>();

    public boolean isAutoStart() {
        return autoStart;
    }

    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    public String getDefaultHost() {
        return defaultHost;
    }

    public void setDefaultHost(String defaultHost) {
        this.defaultHost = defaultHost;
    }

    public int getDefaultPort() {
        return defaultPort;
    }

    public void setDefaultPort(int defaultPort) {
        this.defaultPort = defaultPort;
    }

    public Duration getReconnectBackoff() {
        return reconnectBackoff;
    }

    public void setReconnectBackoff(Duration reconnectBackoff) {
        this.reconnectBackoff = reconnectBackoff;
    }

    public Duration getReconnectMaxBackoff() {
        return reconnectMaxBackoff;
    }

    public void setReconnectMaxBackoff(Duration reconnectMaxBackoff) {
        this.reconnectMaxBackoff = reconnectMaxBackoff;
    }

    public List<ConnectorProperties> getConnectors() {
        return connectors;
    }

    public void setConnectors(List<ConnectorProperties> connectors) {
        this.connectors = connectors == null ? new ArrayList<>() : connectors;
    }
}
