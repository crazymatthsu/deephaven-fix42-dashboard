package com.fix42.dashboard.amps.source;

import com.fix42.dashboard.amps.config.ConnectorProperties;
import org.springframework.stereotype.Component;

/** Builds the {@link AmpsSubscriber} a connector's {@code source.driver} selects. */
@Component
public class AmpsSubscriberFactory {

    /**
     * @param connector the connector configuration
     * @return a fresh, unstarted subscriber
     */
    public AmpsSubscriber create(ConnectorProperties connector) {
        return switch (connector.getSource().getDriver()) {
            case AMPS -> new AmpsClientSubscriber(connector);
            case SIMULATED -> new SimulatedAmpsSubscriber(connector);
        };
    }
}
