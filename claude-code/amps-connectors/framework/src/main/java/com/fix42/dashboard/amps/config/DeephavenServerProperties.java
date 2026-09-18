package com.fix42.dashboard.amps.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

/**
 * How to reach the Deephaven server that every connector publishes into.
 *
 * <p>One server, shared by all connectors: the tables they create are globals in the same
 * python session, so they show up alongside the FIX dashboard's own tables.
 */
public class DeephavenServerProperties {

    /** Deephaven gRPC host. */
    @NotBlank
    private String host = "localhost";

    /** Deephaven gRPC port. */
    @Min(1)
    @Max(65535)
    private int port = 10_000;

    /** Use TLS for the gRPC channel. */
    private boolean tls = false;

    /**
     * Authentication handed to the server, in {@code type value} form. The demo stack runs
     * with {@code AnonymousAuthenticationHandler}, hence the default.
     */
    @NotBlank
    private String authentication = "Anonymous";

    /** Console session type; the demo server runs {@code -Ddeephaven.console.type=python}. */
    @NotBlank
    private String consoleType = "python";

    /** Timeout applied to session establishment and to each script execution. */
    @NotNull
    private Duration timeout = Duration.ofSeconds(30);

    /**
     * How often to probe the server. The probe is what detects a Deephaven restart and
     * triggers table re-creation plus a full AMPS resubscribe (doc 07 section 6).
     */
    @NotNull
    private Duration healthCheckInterval = Duration.ofSeconds(5);

    /** Wait between failed connection attempts. */
    @NotNull
    private Duration reconnectDelay = Duration.ofSeconds(5);

    /** The {@code host:port} target string for the gRPC channel. */
    public String target() {
        return host + ":" + port;
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

    public boolean isTls() {
        return tls;
    }

    public void setTls(boolean tls) {
        this.tls = tls;
    }

    public String getAuthentication() {
        return authentication;
    }

    public void setAuthentication(String authentication) {
        this.authentication = authentication;
    }

    public String getConsoleType() {
        return consoleType;
    }

    public void setConsoleType(String consoleType) {
        this.consoleType = consoleType;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public Duration getHealthCheckInterval() {
        return healthCheckInterval;
    }

    public void setHealthCheckInterval(Duration healthCheckInterval) {
        this.healthCheckInterval = healthCheckInterval;
    }

    public Duration getReconnectDelay() {
        return reconnectDelay;
    }

    public void setReconnectDelay(Duration reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }
}
