package com.deephaven.fix42.amps.amps;

import com.crankuptheamps.client.Message;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AmpsClientAdapterTest {
    @Test
    void mapsAmpsCommandInts() {
        assertEquals(AmpsInboundMessage.Kind.SOW, AmpsClientAdapter.kindOf(Message.Command.SOW));
        assertEquals(AmpsInboundMessage.Kind.PUBLISH, AmpsClientAdapter.kindOf(Message.Command.Publish));
        assertEquals(AmpsInboundMessage.Kind.PUBLISH, AmpsClientAdapter.kindOf(Message.Command.DeltaPublish));
        assertEquals(AmpsInboundMessage.Kind.OOF, AmpsClientAdapter.kindOf(Message.Command.OOF));
        assertEquals(AmpsInboundMessage.Kind.OTHER, AmpsClientAdapter.kindOf(Message.Command.Ack));
        assertEquals(AmpsInboundMessage.Kind.OTHER, AmpsClientAdapter.kindOf(Message.Command.GroupBegin));
    }
}
