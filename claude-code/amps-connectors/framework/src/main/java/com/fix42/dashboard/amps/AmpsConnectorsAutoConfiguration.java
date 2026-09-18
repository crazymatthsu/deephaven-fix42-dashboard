package com.fix42.dashboard.amps;

import com.fix42.dashboard.amps.config.AmpsConnectorsProperties;
import com.fix42.dashboard.amps.config.ColumnTypeConverter;
import com.fix42.dashboard.amps.decode.RecordDecoderFactory;
import com.fix42.dashboard.amps.deephaven.FlightDeephavenGateway;
import com.fix42.dashboard.amps.runtime.ConnectorManager;
import com.fix42.dashboard.amps.runtime.DeephavenLifecycleMonitor;
import com.fix42.dashboard.amps.source.AmpsSubscriberFactory;
import java.time.Clock;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * Registers the whole AMPS -> Deephaven pipeline in any Spring Boot application that has this
 * library on its classpath (via {@code META-INF/spring/...AutoConfiguration.imports}).
 *
 * <p>The framework's beans are contributed here explicitly rather than component-scanned, so an
 * application's own {@code @SpringBootApplication} scan stays confined to its own package: the
 * application supplies a main class and {@code amps:} configuration, and nothing else.
 *
 * <p>Everything is driven from that configuration; see {@link AmpsConnectorsProperties} and
 * {@code docs/07-amps-connectors.md}. The only long-lived activity is the Deephaven lifecycle
 * poll and the AMPS subscriptions it starts.
 */
@AutoConfiguration
@EnableConfigurationProperties(AmpsConnectorsProperties.class)
@Import({
        ColumnTypeConverter.class,
        RecordDecoderFactory.class,
        AmpsSubscriberFactory.class,
        FlightDeephavenGateway.class,
        ConnectorManager.class,
        DeephavenLifecycleMonitor.class
})
public class AmpsConnectorsAutoConfiguration {

    /** Source of ingest timestamps; a bean so tests can pin it. */
    @Bean
    @ConditionalOnMissingBean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
