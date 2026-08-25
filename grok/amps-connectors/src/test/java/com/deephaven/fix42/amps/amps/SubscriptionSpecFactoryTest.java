package com.deephaven.fix42.amps.amps;

import com.deephaven.fix42.amps.config.ColumnType;
import com.deephaven.fix42.amps.config.ConnectorProperties;
import com.deephaven.fix42.amps.config.DataFormat;
import com.deephaven.fix42.amps.config.FieldMappingProperties;
import com.deephaven.fix42.amps.config.TopicKind;
import com.deephaven.fix42.amps.config.UpdateMode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubscriptionSpecFactoryTest {
    @Test
    void sowDeltaUsesSowAndDeltaSubscribeAndOof() {
        ConnectorProperties c = connector(TopicKind.SOW, UpdateMode.DELTA);
        SubscriptionSpec spec = SubscriptionSpecFactory.create(c, true);
        assertEquals("sow_and_delta_subscribe", spec.command());
        assertTrue(spec.options().contains("oof"));
        assertEquals("", spec.bookmark());
    }

    @Test
    void journalFromBeginningUsesEpoch() {
        ConnectorProperties c = connector(TopicKind.JOURNAL, UpdateMode.FULL);
        SubscriptionSpec spec = SubscriptionSpecFactory.create(c, true);
        assertEquals("subscribe", spec.command());
        assertEquals(SubscriptionSpec.BOOKMARK_EPOCH, spec.bookmark());
    }

    @Test
    void journalWhenTableAlreadyExistsUsesNow() {
        ConnectorProperties c = connector(TopicKind.JOURNAL, UpdateMode.FULL);
        SubscriptionSpec spec = SubscriptionSpecFactory.create(c, false);
        assertEquals(SubscriptionSpec.BOOKMARK_NOW, spec.bookmark());
    }

    private static ConnectorProperties connector(TopicKind kind, UpdateMode mode) {
        ConnectorProperties c = new ConnectorProperties();
        c.setName("c1");
        c.setTopic("ORDERS");
        c.setTableName("t");
        c.setTopicKind(kind);
        c.setDataFormat(DataFormat.JSON);
        c.setSubscriberMode(mode);
        c.setPublisherMode(mode);
        FieldMappingProperties f = new FieldMappingProperties();
        f.setSource("id");
        f.setColumn("Id");
        f.setType(ColumnType.STRING);
        c.setFields(List.of(f));
        c.setKeyColumns(List.of("Id"));
        return c;
    }
}
