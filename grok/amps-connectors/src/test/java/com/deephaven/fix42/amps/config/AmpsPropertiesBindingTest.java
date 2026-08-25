package com.deephaven.fix42.amps.config;

import com.deephaven.fix42.amps.AmpsConnectorsApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@SpringBootTest(classes = AmpsConnectorsApplication.class, properties = "amps.auto-start=false")
class AmpsPropertiesBindingTest {
    @Autowired
    private AmpsProperties amps;

    @Autowired
    private DeephavenClientProperties deephaven;

    @Test
    void bindsYamlConnectors() {
        assertEquals(3, amps.getConnectors().size());
        ConnectorProperties json = amps.getConnectors().get(0);
        assertEquals("orders-json", json.getName());
        assertEquals(DataFormat.JSON, json.getDataFormat());
        assertEquals(TopicKind.SOW, json.getTopicKind());
        assertEquals(UpdateMode.DELTA, json.getSubscriberMode());
        assertEquals(UpdateMode.DELTA, json.getPublisherMode());
        assertEquals("amps_orders", json.getTableName());
        assertFalse(json.isEnabled());
        assertEquals(ColumnType.DOUBLE, json.getFields().get(3).getType());

        ConnectorProperties nvfix = amps.getConnectors().get(1);
        assertEquals(DataFormat.NVFIX, nvfix.getDataFormat());
        assertEquals("Symbol", nvfix.getFields().get(0).getSource());

        ConnectorProperties fix = amps.getConnectors().get(2);
        assertEquals(DataFormat.FIX, fix.getDataFormat());
        assertEquals(TopicKind.JOURNAL, fix.getTopicKind());
        assertEquals("11", fix.getFields().get(0).getSource());
        assertEquals("ClOrdID", fix.getFields().get(0).getColumn());
    }

    @Test
    void bindsDeephaven() {
        assertEquals("localhost", deephaven.getHost());
        assertEquals(10000, deephaven.getPort());
        assertEquals("deephaven", deephaven.getPsk());
        assertFalse(amps.isAutoStart());
    }
}
