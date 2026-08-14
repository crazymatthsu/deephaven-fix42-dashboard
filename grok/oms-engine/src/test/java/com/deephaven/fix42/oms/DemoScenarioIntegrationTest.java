package com.deephaven.fix42.oms;

import com.deephaven.fix42.demo.DemoScenarios;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DemoScenarioIntegrationTest {
    @Test
    void fullDemoTapeConverges() {
        InMemoryOmsCache cache = new InMemoryOmsCache(CacheConfig.defaults());
        for (String raw : DemoScenarios.allMessages()) {
            cache.ingest(raw);
        }
        OrderState child1 = cache.getByClOrdId("C1").orElseThrow();
        assertEquals("B1", child1.getOrderKey());
        assertEquals("C1b", child1.getClOrdId());
        assertEquals("2", child1.getOrdStatus());
        assertEquals(400.0, child1.getCumQty());

        OrderState child2 = cache.getByClOrdId("C3").orElseThrow();
        assertEquals("B2", child2.getOrderKey());
        assertEquals("4", child2.getOrdStatus());
        assertEquals("2", child2.getCxlRejResponseTo());

        OrderState dk = cache.getByExecId("E2").orElseThrow();
        assertTrue(dk.isDkTrade());
        assertEquals(400.0, dk.getCumQty());

        assertEquals(3, cache.findByAccount("PROP").size());
        ChildRollup rollup = cache.rollup("P1");
        assertEquals(2, rollup.getChildCount());
    }
}
