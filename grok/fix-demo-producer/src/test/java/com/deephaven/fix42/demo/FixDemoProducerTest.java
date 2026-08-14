package com.deephaven.fix42.demo;

import com.deephaven.fix42.codec.FixParser;
import com.deephaven.fix42.codec.ParserConfig;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixDemoProducerTest {
    @Test
    void tapeIsLegalFixAndCoversRequiredMsgTypes() {
        FixParser parser = new FixParser(ParserConfig.defaults());
        List<String> tape = DemoScenarios.allMessages();
        assertFalse(tape.isEmpty());
        Set<String> types = new HashSet<>();
        for (String raw : tape) {
            types.add(parser.parse(raw).msgType());
            assertTrue(raw.startsWith("8=FIX.4.2|"));
            assertTrue(raw.contains("10="));
        }
        assertTrue(types.containsAll(Set.of("D", "G", "F", "8", "9", "Q")));
        assertEquals("C1", FixDemoProducer.keyOf(tape.get(1)));
    }
}
