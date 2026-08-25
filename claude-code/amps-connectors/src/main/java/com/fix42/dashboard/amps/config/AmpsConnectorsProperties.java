package com.fix42.dashboard.amps.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Root of the {@code amps:} configuration tree -- the whole application is driven from here.
 *
 * <pre>{@code
 * amps:
 *   deephaven:
 *     host: localhost
 *     port: 10000
 *   connectors:
 *     - name: orders-fix
 *       format: FIX
 *       source: { host: amps, port: 9007, topic: Orders, sow: true }
 *       deephaven: { table: amps_orders, key-columns: [ClOrdID] }
 *       fields:
 *         - { tag: "11", column: ClOrdID, type: STRING }
 * }</pre>
 */
@ConfigurationProperties(prefix = "amps")
@Validated
public class AmpsConnectorsProperties {

    /** Master switch: {@code false} starts the application with no connectors running. */
    private boolean enabled = true;

    @Valid
    @NotNull
    private DeephavenServerProperties deephaven = new DeephavenServerProperties();

    /** The connectors this application runs. One AMPS topic each. */
    @Valid
    @NotNull
    private List<ConnectorProperties> connectors = new ArrayList<>();

    /** The enabled connectors, in configuration order. */
    public List<ConnectorProperties> enabledConnectors() {
        return connectors.stream().filter(ConnectorProperties::isEnabled).toList();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public DeephavenServerProperties getDeephaven() {
        return deephaven;
    }

    public void setDeephaven(DeephavenServerProperties deephaven) {
        this.deephaven = deephaven;
    }

    public List<ConnectorProperties> getConnectors() {
        return connectors;
    }

    public void setConnectors(List<ConnectorProperties> connectors) {
        this.connectors = connectors == null ? new ArrayList<>() : connectors;
    }
}
