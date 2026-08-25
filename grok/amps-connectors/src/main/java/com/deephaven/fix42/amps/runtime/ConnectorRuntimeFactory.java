package com.deephaven.fix42.amps.runtime;

import com.deephaven.fix42.amps.config.ConnectorProperties;
import com.deephaven.fix42.amps.decode.MessageDecoder;
import com.deephaven.fix42.amps.decode.MessageDecoders;
import com.deephaven.fix42.amps.map.FieldMapper;
import com.deephaven.fix42.amps.map.RowMerger;
import com.deephaven.fix42.amps.publish.TableSink;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class ConnectorRuntimeFactory {
    private final ObjectMapper objectMapper;

    public ConnectorRuntimeFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public ConnectorRuntime create(ConnectorProperties connector, TableSink sink) {
        MessageDecoder decoder = MessageDecoders.create(connector, objectMapper);
        FieldMapper mapper = new FieldMapper(connector.getFields());
        RowMerger merger = new RowMerger(connector.getFields(), connector.getPublisherMode());
        return new ConnectorRuntime(connector, decoder, mapper, merger, sink);
    }
}
