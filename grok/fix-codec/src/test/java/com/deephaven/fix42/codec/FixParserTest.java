package com.deephaven.fix42.codec;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixParserTest {
    private final FixSerializer serializer = new FixSerializer();
    private final FixParser strict = new FixParser(ParserConfig.defaults());
    private final FixParser lenient = new FixParser(ParserConfig.lenient());

    @Test
    void parsesPipeDelimitedAndRoundTrips() {
        FixMessage built = new FixMessage(
                List.of(
                        new FixField(Tags.MSG_TYPE, "D"),
                        new FixField(Tags.CL_ORD_ID, "C1"),
                        new FixField(Tags.SYMBOL, "MSFT"),
                        new FixField(Tags.SIDE, "1"),
                        new FixField(Tags.ORDER_QTY, "100"),
                        new FixField(Tags.ORD_TYPE, "2"),
                        new FixField(Tags.PRICE, "420")),
                "");
        String soh = serializer.serialize(built);
        String pretty = serializer.serializePretty(built);

        FixMessage fromSoh = strict.parse(soh);
        FixMessage fromPipe = strict.parse(pretty);
        assertEquals("D", fromSoh.msgType());
        assertEquals("C1", fromSoh.getOrEmpty(Tags.CL_ORD_ID));
        assertEquals("D", fromPipe.msgType());
        assertEquals("MSFT", fromPipe.getOrEmpty(Tags.SYMBOL));
        assertEquals(soh, serializer.serialize(fromPipe));
    }

    @Test
    void keepsUnknownTags() {
        FixMessage parsed = lenient.parse("8=FIX.4.2|35=D|11=C1|9999=vendor|10=000|");
        assertEquals("vendor", parsed.getOrEmpty(9999));
    }

    @Test
    void rejectsBadChecksumWhenStrict() {
        assertThrows(FixParseException.class, () -> strict.parse("8=FIX.4.2|9=5|35=D|10=000|"));
    }

    @Test
    void rejectsEmpty() {
        assertThrows(FixParseException.class, () -> lenient.parse(""));
    }

    @Test
    void rejectsMissingDelimiter() {
        assertThrows(FixParseException.class, () -> lenient.parse("8=FIX.4.235=D"));
    }

    @Test
    void prettyFormEndsWithPipe() {
        String pretty = serializer.serializePretty(List.of(new FixField(Tags.MSG_TYPE, "8"), new FixField(Tags.ORDER_ID, "O9")));
        assertTrue(pretty.startsWith("8=FIX.4.2|"));
        assertTrue(pretty.contains("35=8|"));
        assertTrue(pretty.endsWith("|"));
    }
}
