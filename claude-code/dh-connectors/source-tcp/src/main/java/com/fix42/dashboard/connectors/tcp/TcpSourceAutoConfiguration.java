package com.fix42.dashboard.connectors.tcp;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Contributes the TCP driver to any application that depends on this module.
 *
 * <p>Unconditional, unlike the AMPS and Kafka drivers: this source has no client library to
 * guard on -- {@code java.net} is always there -- so if the module is on the classpath the
 * factory is available.
 */
@AutoConfiguration
public class TcpSourceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public TcpSourceFactory tcpSourceFactory() {
        return new TcpSourceFactory();
    }
}
