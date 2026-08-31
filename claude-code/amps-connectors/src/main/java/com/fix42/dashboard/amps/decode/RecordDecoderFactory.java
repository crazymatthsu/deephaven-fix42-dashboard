package com.fix42.dashboard.amps.decode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.SourceFormat;
import java.util.List;
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
     * @throws IllegalArgumentException if {@code composite-parts} nests {@code COMPOSITE}
     *     -- {@code ConnectorValidator} refuses the configuration first in normal startup
     */
    public RecordDecoder create(ConnectorProperties connector) {
        char separator = connector.getSource().getFieldSeparator();
        return switch (connector.getFormat()) {
            case FIX, NVFIX -> new DelimitedRecordDecoder(separator);
            case JSON -> new JsonRecordDecoder(mapper);
            case COMPOSITE -> new CompositeRecordDecoder(partDecoders(connector, separator));
        };
    }

    private List<RecordDecoder> partDecoders(ConnectorProperties connector, char separator) {
        return connector.getCompositeParts().stream()
                .map(part -> switch (part) {
                    case FIX, NVFIX -> (RecordDecoder) new DelimitedRecordDecoder(separator);
                    case JSON -> new JsonRecordDecoder(mapper);
                    case COMPOSITE -> throw new IllegalArgumentException(
                            "composite-parts cannot themselves be COMPOSITE");
                })
                .toList();
    }
}
