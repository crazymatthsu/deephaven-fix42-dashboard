package com.fix42.dashboard.connectors.jdbc;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Contributes the JDBC driver to any application that depends on this module.
 *
 * <p>Unconditional, like the TCP driver and unlike the AMPS and Kafka ones: the client this
 * source compiles against is {@code java.sql}, which is always there. The <em>database</em>
 * driver is a runtime concern -- {@code DriverManager} resolves it from the classpath when a
 * connector dials, and a missing one is a connection failure with a readable message, not a
 * reason to withhold the factory.
 */
@AutoConfiguration
public class JdbcSourceAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public JdbcSourceFactory jdbcSourceFactory() {
        return new JdbcSourceFactory();
    }
}
