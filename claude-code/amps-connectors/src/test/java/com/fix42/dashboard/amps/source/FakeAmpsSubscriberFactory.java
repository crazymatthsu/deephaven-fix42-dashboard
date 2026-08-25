package com.fix42.dashboard.amps.source;

import com.fix42.dashboard.amps.config.ConnectorProperties;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Hands out one {@link FakeAmpsSubscriber} per connector name, reusing it across restarts so a
 * test can assert how many times a connector resubscribed.
 */
public class FakeAmpsSubscriberFactory extends AmpsSubscriberFactory {

    private final Map<String, FakeAmpsSubscriber> subscribers = new LinkedHashMap<>();

    @Override
    public AmpsSubscriber create(ConnectorProperties connector) {
        return subscribers.computeIfAbsent(connector.getName(), name -> new FakeAmpsSubscriber());
    }

    /** The subscriber for a connector, creating it if the connector has not started yet. */
    public FakeAmpsSubscriber get(String connectorName) {
        return subscribers.computeIfAbsent(connectorName, name -> new FakeAmpsSubscriber());
    }
}
