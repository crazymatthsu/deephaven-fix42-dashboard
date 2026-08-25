package com.deephaven.fix42.amps.decode;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TagValueParserTest {
    @Test
    void parsesPipeDelimitedLastWins() {
        Map<String, String> parsed = TagValueParser.parse("55=IBM|11=C1|55=AAPL|");
        assertEquals("AAPL", parsed.get("55"));
        assertEquals("C1", parsed.get("11"));
    }

    @Test
    void parsesSoh() {
        Map<String, String> parsed = TagValueParser.parse("Symbol=IBM\u0001Price=10.5\u0001");
        assertEquals("IBM", parsed.get("Symbol"));
        assertEquals("10.5", parsed.get("Price"));
    }

    @Test
    void singleFieldWithoutDelimiter() {
        Map<String, String> parsed = TagValueParser.parse("OrderId=O9");
        assertEquals("O9", parsed.get("OrderId"));
    }

    @Test
    void rejectsMalformed() {
        assertThrows(DecodeException.class, () -> TagValueParser.parse("noequals"));
    }
}
