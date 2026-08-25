package com.deephaven.fix42.amps.config;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConnectorConfigValidatorTest {
    @Test
    void sowRequiresKeys() {
        AmpsProperties amps = baseAmps();
        ConnectorProperties c = jsonConnector();
        c.setTopicKind(TopicKind.SOW);
        c.setKeyColumns(List.of());
        assertThrows(ConnectorConfigException.class, () -> ConnectorConfigValidator.validateOne(amps, c));
    }

    @Test
    void fixSourceMustBeNumeric() {
        AmpsProperties amps = baseAmps();
        ConnectorProperties c = jsonConnector();
        c.setDataFormat(DataFormat.FIX);
        c.getFields().get(0).setSource("Symbol");
        assertThrows(ConnectorConfigException.class, () -> ConnectorConfigValidator.validateOne(amps, c));
    }

    @Test
    void keyMustBeMapped() {
        AmpsProperties amps = baseAmps();
        ConnectorProperties c = jsonConnector();
        c.setKeyColumns(List.of("Missing"));
        assertThrows(ConnectorConfigException.class, () -> ConnectorConfigValidator.validateOne(amps, c));
    }

    @Test
    void uriBuiltFromHostPortAndFormat() {
        AmpsProperties amps = baseAmps();
        ConnectorProperties c = jsonConnector();
        assertEquals("tcp://amps.local:9007/amps/json", ConnectorConfigValidator.resolvedUri(amps, c));
        c.setUsername("trader");
        c.setPassword("s3cret");
        assertEquals(
                "tcp://trader:s3cret@amps.local:9007/amps/json", ConnectorConfigValidator.resolvedUri(amps, c));
    }

    private static AmpsProperties baseAmps() {
        AmpsProperties p = new AmpsProperties();
        p.setDefaultHost("amps.local");
        p.setDefaultPort(9007);
        return p;
    }

    private static ConnectorProperties jsonConnector() {
        ConnectorProperties c = new ConnectorProperties();
        c.setName("orders");
        c.setTopic("ORDERS");
        c.setTableName("amps_orders");
        c.setTopicKind(TopicKind.SOW);
        c.setDataFormat(DataFormat.JSON);
        FieldMappingProperties f = new FieldMappingProperties();
        f.setSource("OrderId");
        f.setColumn("OrderId");
        f.setType(ColumnType.STRING);
        c.setFields(List.of(f));
        c.setKeyColumns(List.of("OrderId"));
        return c;
    }
}
