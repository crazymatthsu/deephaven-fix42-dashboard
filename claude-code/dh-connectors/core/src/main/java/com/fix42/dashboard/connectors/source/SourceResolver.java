package com.fix42.dashboard.connectors.source;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.SourceProperties;
import java.util.List;

/**
 * Picks the {@link RecordSource} for a connector out of the {@link SourceFactory} beans on the
 * classpath.
 *
 * <p>{@code driver: SIMULATED} short-circuits to {@link SimulatedSource} whatever transport is
 * configured -- the block still says what the connector stands in for, and the validator's
 * rules for it still apply, but nothing is dialled. Otherwise exactly one factory must claim
 * the connector: zero means the source module for that block is not on the classpath, and more
 * than one means two transports were configured for one table. Both are configuration
 * mistakes that would otherwise surface as an empty table, so they throw.
 */
public class SourceResolver {

    private final List<SourceFactory> factories;

    public SourceResolver(List<SourceFactory> factories) {
        this.factories = List.copyOf(factories);
    }

    /**
     * @param connector the connector configuration
     * @return a fresh, unstarted source
     * @throws IllegalStateException if no factory, or more than one, claims the connector
     */
    public RecordSource resolve(ConnectorProperties connector) {
        SourceProperties source = connector.getSource();
        if (source.getDriver() == SourceProperties.Driver.SIMULATED) {
            return new SimulatedSource(connector);
        }
        List<SourceFactory> claimed = factories.stream()
                .filter(factory -> factory.supports(connector))
                .toList();
        if (claimed.size() == 1) {
            return claimed.get(0).create(connector);
        }
        throw new IllegalStateException("connector '" + connector.getName() + "': "
                + (claimed.isEmpty()
                        ? "no source implementation for " + describe(source.configuredBlocks())
                        : claimed.size() + " source implementations claim "
                                + describe(source.configuredBlocks())));
    }

    private static String describe(List<String> blocks) {
        return blocks.isEmpty() ? "an empty source: block" : "source." + String.join("/", blocks);
    }
}
