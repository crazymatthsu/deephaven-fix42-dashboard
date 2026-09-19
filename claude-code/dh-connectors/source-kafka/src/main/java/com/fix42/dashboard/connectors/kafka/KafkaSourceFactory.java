package com.fix42.dashboard.connectors.kafka;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.source.RecordSource;
import com.fix42.dashboard.connectors.source.SourceFactory;

/**
 * Claims the connectors that configure {@code source.kafka}.
 *
 * <p>The presence of the block is the whole test: a connector cannot configure two transports
 * ({@code ConnectorValidator} refuses that), so no further disambiguation is needed or wanted.
 */
public class KafkaSourceFactory implements SourceFactory {

    @Override
    public boolean supports(ConnectorProperties connector) {
        return connector.getSource().getKafka() != null;
    }

    @Override
    public RecordSource create(ConnectorProperties connector) {
        return new KafkaRecordSource(connector);
    }
}
