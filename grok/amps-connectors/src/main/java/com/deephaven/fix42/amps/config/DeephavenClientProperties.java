package com.deephaven.fix42.amps.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "deephaven")
public class DeephavenClientProperties {
    @NotBlank
    private String host = "localhost";

    private int port = 10000;
    private boolean secure;
    private String psk = "deephaven";
    private Duration waitTimeout = Duration.ofSeconds(120);
    private Duration retryInterval = Duration.ofSeconds(2);
    private Duration publishTimeout = Duration.ofSeconds(10);

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

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public String getPsk() {
        return psk;
    }

    public void setPsk(String psk) {
        this.psk = psk;
    }

    public Duration getWaitTimeout() {
        return waitTimeout;
    }

    public void setWaitTimeout(Duration waitTimeout) {
        this.waitTimeout = waitTimeout;
    }

    public Duration getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(Duration retryInterval) {
        this.retryInterval = retryInterval;
    }

    public Duration getPublishTimeout() {
        return publishTimeout;
    }

    public void setPublishTimeout(Duration publishTimeout) {
        this.publishTimeout = publishTimeout;
    }
}
