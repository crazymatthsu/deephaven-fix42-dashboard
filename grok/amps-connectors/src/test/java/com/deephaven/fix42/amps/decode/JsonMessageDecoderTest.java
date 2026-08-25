package com.deephaven.fix42.amps.decode;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonMessageDecoderTest {
    private final JsonMessageDecoder decoder =
            new JsonMessageDecoder(new ObjectMapper(), List.of("OrderId", "Symbol", "order.id", "/qty"));

    @Test
    void topLevelAndNestedAndPointer() {
        ParsedFields parsed = decoder.decode(
                "{\"OrderId\":\"O9\",\"Symbol\":\"IBM\",\"order\":{\"id\":\"nested\"},\"qty\":100,\"ignored\":true}");
        assertEquals("O9", parsed.get("OrderId").orElseThrow());
        assertEquals("IBM", parsed.get("Symbol").orElseThrow());
        assertEquals("nested", parsed.get("order.id").orElseThrow());
        assertEquals("100", parsed.get("/qty").orElseThrow());
        assertFalse(parsed.isPresent("ignored"));
    }

    @Test
    void missingFieldsAreAbsent() {
        ParsedFields parsed = decoder.decode("{\"Symbol\":\"IBM\"}");
        assertTrue(parsed.isPresent("Symbol"));
        assertFalse(parsed.isPresent("OrderId"));
    }

    @Test
    void invalidJson() {
        assertThrows(DecodeException.class, () -> decoder.decode("{"));
    }
}
