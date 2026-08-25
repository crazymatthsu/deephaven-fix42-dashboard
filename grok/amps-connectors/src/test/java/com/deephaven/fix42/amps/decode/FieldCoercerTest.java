package com.deephaven.fix42.amps.decode;

import com.deephaven.fix42.amps.config.ColumnType;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FieldCoercerTest {
    @Test
    void numericAndBoolean() {
        assertEquals(12, FieldCoercer.coerce("12", ColumnType.INT));
        assertEquals(12L, FieldCoercer.coerce("12", ColumnType.LONG));
        assertEquals(1.5d, FieldCoercer.coerce("1.5", ColumnType.DOUBLE));
        assertEquals(true, FieldCoercer.coerce("Y", ColumnType.BOOLEAN));
        assertEquals(false, FieldCoercer.coerce("0", ColumnType.BOOLEAN));
        assertNull(FieldCoercer.coerce("", ColumnType.INT));
        assertEquals("", FieldCoercer.coerce("", ColumnType.STRING));
    }

    @Test
    void instantIsoAndFix() {
        Instant iso = (Instant) FieldCoercer.coerce("2024-01-02T03:04:05Z", ColumnType.INSTANT);
        assertEquals(Instant.parse("2024-01-02T03:04:05Z"), iso);
        Instant fix = (Instant) FieldCoercer.coerce("20240102-03:04:05.000", ColumnType.INSTANT);
        assertEquals(iso, fix);
    }

    @Test
    void badNumber() {
        assertThrows(DecodeException.class, () -> FieldCoercer.coerce("x", ColumnType.INT));
        assertThrows(DecodeException.class, () -> FieldCoercer.coerce("maybe", ColumnType.BOOLEAN));
    }
}
