package com.fix42.dashboard.amps.decode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import org.springframework.stereotype.Component;

/** Builds the {@link RecordDecoder} a connector's {@code format} selects. */
@Component
public class RecordDecoderFactory {

    private final ObjectMapper mapper;

    public RecordDecoderFactory(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param connector the connector configuration
     * @return a decoder for its wire format
     */
    public RecordDecoder create(ConnectorProperties connector) {
        return switch (connector.getFormat()) {
            case FIX, NVFIX -> new DelimitedRecordDecoder(connector.getSource().getFieldSeparator());
            case JSON -> new JsonRecordDecoder(mapper);
        };
    }
}
