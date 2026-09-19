package com.fix42.dashboard.connectors.source;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link SourceResolver} that hands out one {@link FakeRecordSource} per connector name,
 * reusing it across restarts so a test can assert how many times a connector resubscribed.
 *
 * <p>Both a resolver and a {@link SourceFactory}: as a resolver it stands in for the whole
 * lookup wherever a test wires a {@code ConnectorManager}, overriding the
 * {@code driver: SIMULATED} short-circuit so the test's own double is what the connector
 * subscribes to; as a factory it can be handed to a real resolver instead, for the tests
 * that are about resolution.
 */
public class FakeSourceFactory extends SourceResolver implements SourceFactory {

    private final Map<String, FakeRecordSource> sources = new LinkedHashMap<>();

    public FakeSourceFactory() {
        super(List.of());
    }

    @Override
    public RecordSource resolve(ConnectorProperties connector) {
        return get(connector.getName());
    }

    @Override
    public boolean supports(ConnectorProperties connector) {
        return true;
    }

    @Override
    public FakeRecordSource create(ConnectorProperties connector) {
        return get(connector.getName());
    }

    /** The source for a connector, creating it if the connector has not started yet. */
    public FakeRecordSource get(String connectorName) {
        return sources.computeIfAbsent(connectorName, name -> new FakeRecordSource());
    }
}
