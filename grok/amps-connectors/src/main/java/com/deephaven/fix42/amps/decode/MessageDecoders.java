package com.deephaven.fix42.amps.decode;

import com.deephaven.fix42.amps.config.ConnectorProperties;
import com.deephaven.fix42.amps.config.FieldMappingProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class MessageDecoders {
    private MessageDecoders() {}

    public static MessageDecoder create(ConnectorProperties connector, ObjectMapper mapper) {
        List<String> sources = connector.getFields().stream()
                .map(FieldMappingProperties::getSource)
                .toList();
        Set<String> wanted = new LinkedHashSet<>(sources);
        return switch (connector.getDataFormat()) {
            case FIX -> new FixMessageDecoder(wanted);
            case NVFIX -> new NvfixMessageDecoder(wanted);
            case JSON -> new JsonMessageDecoder(mapper, sources);
        };
    }
}
