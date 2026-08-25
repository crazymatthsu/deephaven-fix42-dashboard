package com.fix42.dashboard.amps;

import com.fix42.dashboard.amps.config.AmpsConnectorsProperties;
import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * AMPS -> Deephaven connector application.
 *
 * <p>Runs one or more connectors, each subscribing to an AMPS topic and publishing the fields it
 * is configured to map into a Deephaven table. Everything is driven from {@code application.yml}
 * under the {@code amps:} prefix; see {@link AmpsConnectorsProperties} and
 * {@code docs/07-amps-connectors.md}.
 *
 * <p>Headless: no web server, no database. The only long-lived activity is the Deephaven
 * lifecycle poll and the AMPS subscriptions it starts.
 */
@SpringBootApplication
@EnableConfigurationProperties(AmpsConnectorsProperties.class)
public class AmpsConnectorsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AmpsConnectorsApplication.class, args);
    }

    /** Source of ingest timestamps; a bean so tests can pin it. */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
