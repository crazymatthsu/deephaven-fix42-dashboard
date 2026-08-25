package com.deephaven.fix42.amps.runtime;

import com.deephaven.fix42.amps.amps.AmpsInboundMessage;
import com.deephaven.fix42.amps.config.ColumnType;
import com.deephaven.fix42.amps.config.ConnectorProperties;
import com.deephaven.fix42.amps.config.DataFormat;
import com.deephaven.fix42.amps.config.FieldMappingProperties;
import com.deephaven.fix42.amps.config.TopicKind;
import com.deephaven.fix42.amps.config.UpdateMode;
import com.deephaven.fix42.amps.decode.JsonMessageDecoder;
import com.deephaven.fix42.amps.map.FieldMapper;
import com.deephaven.fix42.amps.map.MappedRow;
import com.deephaven.fix42.amps.map.RowMerger;
import com.deephaven.fix42.amps.publish.TableSink;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConnectorRuntimeTest {
    @Test
    void sowDeltaMergesAndIgnoresUnconfiguredFields() {
        ConnectorProperties c = sow(UpdateMode.DELTA);
        RecordingSink sink = new RecordingSink();
        ConnectorRuntime runtime = runtime(c, sink);

        runtime.onMessage(AmpsInboundMessage.of(
                "sow", "k1", "{\"OrderId\":\"O9\",\"Symbol\":\"IBM\",\"Price\":10.5,\"Secret\":\"nope\"}"));
        assertEquals(1, sink.upserts.size());
        assertEquals("IBM", sink.upserts.get(0).get("Symbol"));
        assertNull(sink.upserts.get(0).get("Secret"));

        runtime.onMessage(AmpsInboundMessage.of("publish", "k1", "{\"OrderId\":\"O9\",\"Price\":11.0}"));
        assertEquals(2, sink.upserts.size());
        MappedRow latest = sink.upserts.get(1);
        assertEquals("IBM", latest.get("Symbol"));
        assertEquals(11.0, latest.get("Price"));
    }

    @Test
    void oofDeletesBySowKey() {
        ConnectorProperties c = sow(UpdateMode.FULL);
        RecordingSink sink = new RecordingSink();
        ConnectorRuntime runtime = runtime(c, sink);
        runtime.onMessage(AmpsInboundMessage.of("sow", "k1", "{\"OrderId\":\"O9\",\"Symbol\":\"IBM\"}"));
        runtime.onMessage(AmpsInboundMessage.of("oof", "k1", ""));
        assertEquals(1, sink.deletes.size());
        assertEquals("O9", sink.deletes.get(0).get("OrderId"));
    }

    @Test
    void journalAlwaysAppends() {
        ConnectorProperties c = journal();
        RecordingSink sink = new RecordingSink();
        ConnectorRuntime runtime = runtime(c, sink);
        runtime.onMessage(AmpsInboundMessage.of("publish", "", "{\"OrderId\":\"C1\",\"Symbol\":\"IBM\"}"));
        runtime.onMessage(AmpsInboundMessage.of("publish", "", "{\"OrderId\":\"C1\",\"Symbol\":\"IBM\"}"));
        assertEquals(2, sink.upserts.size());
    }

    private static ConnectorRuntime runtime(ConnectorProperties c, TableSink sink) {
        List<String> sources = c.getFields().stream().map(FieldMappingProperties::getSource).toList();
        return new ConnectorRuntime(
                c,
                new JsonMessageDecoder(new ObjectMapper(), sources),
                new FieldMapper(c.getFields()),
                new RowMerger(c.getFields(), c.getPublisherMode()),
                sink);
    }

    private static ConnectorProperties sow(UpdateMode publisher) {
        ConnectorProperties c = base();
        c.setTopicKind(TopicKind.SOW);
        c.setPublisherMode(publisher);
        c.setKeyColumns(List.of("OrderId"));
        return c;
    }

    private static ConnectorProperties journal() {
        ConnectorProperties c = base();
        c.setTopicKind(TopicKind.JOURNAL);
        c.setPublisherMode(UpdateMode.FULL);
        c.setKeyColumns(List.of());
        return c;
    }

    private static ConnectorProperties base() {
        ConnectorProperties c = new ConnectorProperties();
        c.setName("t");
        c.setTopic("ORDERS");
        c.setTableName("amps_orders");
        c.setDataFormat(DataFormat.JSON);
        c.setSubscriberMode(UpdateMode.DELTA);
        c.setFields(List.of(field("OrderId", "OrderId", ColumnType.STRING),
                field("Symbol", "Symbol", ColumnType.STRING),
                field("Price", "Price", ColumnType.DOUBLE)));
        return c;
    }

    private static FieldMappingProperties field(String source, String column, ColumnType type) {
        FieldMappingProperties f = new FieldMappingProperties();
        f.setSource(source);
        f.setColumn(column);
        f.setType(type);
        return f;
    }

    private static final class RecordingSink implements TableSink {
        final List<MappedRow> upserts = new ArrayList<>();
        final List<MappedRow> deletes = new ArrayList<>();

        @Override
        public void upsert(MappedRow row) {
            upserts.add(row);
        }

        @Override
        public void delete(MappedRow keyRow) {
            deletes.add(keyRow);
        }
    }
}
