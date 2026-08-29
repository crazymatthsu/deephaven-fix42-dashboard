package com.fix42.dashboard.dh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.deephaven.util.QueryConstants;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The row-to-column coercions (port of {@code dh_app.pipeline}'s {@code _as_*} helpers).
 *
 * <p>The null handling here is the contract that keeps the two implementations' tables identical:
 * {@code ""} for absent strings, Deephaven's null sentinels for absent numerics, and {@code false}
 * -- not null -- for every boolean except {@code ChecksumOk}.
 */
class BlinkStreamTest {

    @Test
    @DisplayName("an absent string is \"\", never null (doc 01 section 6)")
    void asStringNeverReturnsNull() {
        assertEquals("", BlinkStream.asString(null));
        assertEquals("C1", BlinkStream.asString("C1"));
        assertEquals("42", BlinkStream.asString(42));
    }

    @Test
    void asDoubleMapsAbsentAndGarbageToTheNullSentinel() {
        assertEquals(1.5, BlinkStream.asDouble(1.5));
        assertEquals(7.0, BlinkStream.asDouble(7L));
        assertEquals(2.5, BlinkStream.asDouble("2.5"));
        assertEquals(QueryConstants.NULL_DOUBLE, BlinkStream.asDouble(null));
        assertEquals(QueryConstants.NULL_DOUBLE, BlinkStream.asDouble("not a number"));
    }

    @Test
    void asLongMapsAbsentAndGarbageToTheNullSentinel() {
        assertEquals(7L, BlinkStream.asLong(7L));
        assertEquals(3L, BlinkStream.asLong(3.9), "truncates like python's int()");
        assertEquals(12L, BlinkStream.asLong("12"));
        assertEquals(QueryConstants.NULL_LONG, BlinkStream.asLong(null));
        assertEquals(QueryConstants.NULL_LONG, BlinkStream.asLong("nope"));
    }

    @Test
    @DisplayName("null stays null only for a tri-state boolean column")
    void asBooleanRespectsNullability() {
        assertNull(BlinkStream.asBoolean(null, true));
        assertEquals(Boolean.FALSE, BlinkStream.asBoolean(null, false));
        assertEquals(Boolean.TRUE, BlinkStream.asBoolean(Boolean.TRUE, false));
        assertEquals(Boolean.FALSE, BlinkStream.asBoolean(Boolean.FALSE, true));
    }

    @Test
    void asInstantPassesInstantsAndNullsEverythingElse() {
        Instant now = Instant.parse("2024-01-15T14:30:00Z");
        assertEquals(now, BlinkStream.asInstant(now));
        assertNull(BlinkStream.asInstant(null));
        assertNull(BlinkStream.asInstant("2024-01-15T14:30:00Z"), "a bad timestamp must not poison a batch");
    }

    @Test
    @DisplayName("the null-double sentinel really is distinguishable from 0.0")
    void nullDoubleIsNotZero() {
        assertTrue(QueryConstants.NULL_DOUBLE != 0.0);
        assertTrue(QueryConstants.NULL_LONG != 0L);
    }
}
