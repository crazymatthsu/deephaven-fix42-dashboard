package com.fix42.dashboard.dh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Identifier sanitization for the query API (doc 04 section 9.7).
 *
 * <p>The filters are strings compiled to Java, so an identifier carrying a backtick could otherwise
 * close the literal and continue the expression. Ids are generator-controlled alphanumerics in this
 * project, which is exactly why the guard has to be tested rather than assumed.
 */
class Fix42QueryApiTest {

    @Test
    void ordinaryIdentifiersPassThroughUnchanged() {
        assertEquals("ORD-0001", Fix42QueryApi.sanitizeId("ORD-0001"));
        assertEquals("C-0001-1", Fix42QueryApi.sanitizeId("C-0001-1"));
        assertEquals("ACC_1.a", Fix42QueryApi.sanitizeId("ACC_1.a"));
    }

    @Test
    void nullAndBlankBecomeEmpty() {
        assertEquals("", Fix42QueryApi.sanitizeId(null));
        assertEquals("", Fix42QueryApi.sanitizeId(""));
        assertEquals("", Fix42QueryApi.sanitizeId("   "));
    }

    @Test
    void surroundingWhitespaceIsTrimmed() {
        assertEquals("ORD-1", Fix42QueryApi.sanitizeId("  ORD-1  "));
    }

    @ParameterizedTest
    @ValueSource(strings = {"`", "\"", "'", "\\", "\n", "\r", "\t"})
    @DisplayName("every character that could break out of a backtick literal is removed")
    void quotingCharactersAreStripped(String hostile) {
        String sanitized = Fix42QueryApi.sanitizeId("ORD" + hostile + "1");
        assertEquals("ORD1", sanitized);
        assertFalse(sanitized.contains(hostile));
    }

    @Test
    @DisplayName("a crafted identifier cannot close the literal and append a predicate")
    void injectionAttemptIsDefused() {
        String attack = "x` || true || `";
        // The trailing space goes with the surrounding strip(), matching sanitize_id in python.
        assertEquals("x || true ||", Fix42QueryApi.sanitizeId(attack));
        assertFalse(Fix42QueryApi.sanitizeId(attack).contains("`"));
    }

    @Test
    void controlCharactersBelowSpaceAreRemoved() {
        assertEquals("ORD1", Fix42QueryApi.sanitizeId("ORD" + (char) 1 + "1"));
        assertEquals("ORD1", Fix42QueryApi.sanitizeId("ORD" + (char) 0 + "1"));
    }

    @Test
    void nonStringsAreRendered() {
        assertEquals("42", Fix42QueryApi.sanitizeId(42));
    }
}
