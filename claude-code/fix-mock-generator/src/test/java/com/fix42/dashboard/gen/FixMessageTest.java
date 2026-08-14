package com.fix42.dashboard.gen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Ordered-map semantics and FIX value formatting for {@link FixMessage}. */
class FixMessageTest {

    @Test
    @DisplayName("fields keep insertion order; 8 and 35 lead")
    void insertionOrder() {
        FixMessage msg = FixMessage.create("D").set(11, "C-1").set(55, "AAPL").set(54, "1");
        assertEquals(List.of(8, 35, 11, 55, 54), List.copyOf(msg.fields().keySet()));
        assertEquals("D", msg.msgType());
    }

    @Test
    @DisplayName("re-setting a tag overwrites in place — the header slot stamping trick")
    void overwriteKeepsPosition() {
        FixMessage msg = FixMessage.create("8").set(34, 1).set(52, "placeholder").set(37, "ORD-1");
        msg.set(34, 99).set(52, "20250814-12:00:00.000");
        assertEquals(List.of(8, 35, 34, 52, 37), List.copyOf(msg.fields().keySet()));
        assertEquals("99", msg.get(34));
        assertEquals("20250814-12:00:00.000", msg.get(52));
    }

    @Test
    @DisplayName("has() reports presence without allocating a value")
    void hasReportsPresence() {
        FixMessage msg = FixMessage.create("D").set(11, "C-1");
        assertTrue(msg.has(11));
        assertFalse(msg.has(41));
    }

    @Test
    @DisplayName("prices render fixed-point with at least two decimals and no exponent")
    void priceFormatting() {
        assertEquals("185.50", FixMessage.price(185.5));
        assertEquals("185.00", FixMessage.price(185.0));
        assertEquals("0.00", FixMessage.price(0));
        assertEquals("185.521667", FixMessage.price(185.5216666666));
        assertEquals("0.000001", FixMessage.price(0.0000012));
    }

    @Test
    @DisplayName("whole quantities render without a decimal point")
    void qtyFormatting() {
        assertEquals("1000", FixMessage.qty(1000));
        assertEquals("0", FixMessage.qty(0));
        assertEquals("12.5", FixMessage.qty(12.5));
    }

    @Test
    @DisplayName("non-positive tags and null values are rejected")
    void invalidFieldsRejected() {
        FixMessage msg = FixMessage.create("D");
        assertThrows(IllegalArgumentException.class, () -> msg.set(0, "x"));
        assertThrows(NullPointerException.class, () -> msg.set(11, (String) null));
    }

    @Test
    @DisplayName("fields() is an unmodifiable view")
    void fieldsUnmodifiable() {
        FixMessage msg = FixMessage.create("D").set(11, "C-1");
        assertThrows(UnsupportedOperationException.class, () -> msg.fields().put(55, "AAPL"));
    }
}
