package com.deephaven.fix42.amps.decode;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixAndNvfixDecoderTest {
    @Test
    void fixKeepsOnlyConfiguredTags() {
        FixMessageDecoder decoder = new FixMessageDecoder(Set.of("11", "55"));
        ParsedFields parsed = decoder.decode("8=FIX.4.2|9=20|35=D|11=C1|55=IBM|44=10.5|10=000|");
        assertEquals("C1", parsed.get("11").orElseThrow());
        assertEquals("IBM", parsed.get("55").orElseThrow());
        assertFalse(parsed.isPresent("44"));
        assertFalse(parsed.isPresent("35"));
    }

    @Test
    void nvfixKeepsOnlyConfiguredTags() {
        NvfixMessageDecoder decoder = new NvfixMessageDecoder(Set.of("Symbol", "Price"));
        ParsedFields parsed = decoder.decode("Symbol=IBM|Price=10.5|Bid=10.4|Account=B1|");
        assertEquals("IBM", parsed.get("Symbol").orElseThrow());
        assertEquals("10.5", parsed.get("Price").orElseThrow());
        assertFalse(parsed.isPresent("Bid"));
        assertTrue(parsed.isPresent("Price"));
    }
}
