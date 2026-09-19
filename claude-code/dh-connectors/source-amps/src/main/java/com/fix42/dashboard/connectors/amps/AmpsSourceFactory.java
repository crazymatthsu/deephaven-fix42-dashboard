package com.fix42.dashboard.connectors.amps;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.source.RecordSource;
import com.fix42.dashboard.connectors.source.SourceFactory;

/**
 * Claims the connectors that configure {@code source.amps}.
 *
 * <p>The presence of the block is the whole test: a connector cannot configure two transports
 * ({@code ConnectorValidator} refuses that), so no further disambiguation is needed or wanted.
 */
public class AmpsSourceFactory implements SourceFactory {

    @Override
    public boolean supports(ConnectorProperties connector) {
        return connector.getSource().getAmps() != null;
    }

    @Override
    public RecordSource create(ConnectorProperties connector) {
        return new AmpsRecordSource(connector);
    }
}
