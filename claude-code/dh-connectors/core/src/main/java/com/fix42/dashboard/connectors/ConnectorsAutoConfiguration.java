package com.fix42.dashboard.connectors;

import com.fix42.dashboard.connectors.config.ColumnTypeConverter;
import com.fix42.dashboard.connectors.config.ConnectorsProperties;
import com.fix42.dashboard.connectors.decode.RecordDecoderFactory;
import com.fix42.dashboard.connectors.deephaven.FlightDeephavenGateway;
import com.fix42.dashboard.connectors.runtime.ConnectorManager;
import com.fix42.dashboard.connectors.runtime.DeephavenLifecycleMonitor;
import com.fix42.dashboard.connectors.source.SourceFactory;
import com.fix42.dashboard.connectors.source.SourceResolver;
import com.fix42.dashboard.connectors.transform.RecordTransform;
import com.fix42.dashboard.connectors.transform.TransformRegistry;
import java.time.Clock;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * Registers the whole source -> Deephaven pipeline in any Spring Boot application that has this
 * library on its classpath (via {@code META-INF/spring/...AutoConfiguration.imports}).
 *
 * <p>The framework's beans are contributed here explicitly rather than component-scanned, so an
 * application's own {@code @SpringBootApplication} scan stays confined to its own package: the
 * application supplies a main class and {@code dh-connectors:} configuration, and nothing else.
 *
 * <p>The two extension points are collected rather than enumerated -- every {@link SourceFactory}
 * on the classpath (each source module auto-configures its own) and every
 * {@link RecordTransform} bean the application declares. Both collections are legitimately
 * empty: an application with no source module runs only simulated connectors, and transforms
 * are the exception rather than the rule.
 *
 * <p>Everything else is driven from configuration; see {@link ConnectorsProperties} and
 * {@code docs/07-dh-connectors.md}. The only long-lived activity is the Deephaven lifecycle
 * poll and the subscriptions it starts.
 */
@AutoConfiguration
@EnableConfigurationProperties(ConnectorsProperties.class)
@Import({
        ColumnTypeConverter.class,
        RecordDecoderFactory.class,
        FlightDeephavenGateway.class,
        ConnectorManager.class,
        DeephavenLifecycleMonitor.class
})
public class ConnectorsAutoConfiguration {

    /** Source of ingest timestamps; a bean so tests can pin it. */
    @Bean
    @ConditionalOnMissingBean
    public Clock clock() {
        return Clock.systemUTC();
    }

    /**
     * The driver lookup, over whichever source modules are on the classpath.
     *
     * @param factories every contributed factory; empty when only the simulator is wanted
     */
    @Bean
    @ConditionalOnMissingBean
    public SourceResolver sourceResolver(ObjectProvider<SourceFactory> factories) {
        return new SourceResolver(factories.orderedStream().toList());
    }

    /**
     * The transforms a connector's {@code transforms:} list can name, by bean name.
     *
     * <p>Asked of the context rather than injected as a {@code Map}, so that the usual case
     * -- no transforms anywhere -- is an empty registry instead of an unsatisfied dependency.
     *
     * @param context the application context, scanned for {@link RecordTransform} beans
     */
    @Bean
    @ConditionalOnMissingBean
    public TransformRegistry transformRegistry(ApplicationContext context) {
        return new TransformRegistry(context.getBeansOfType(RecordTransform.class));
    }
}
