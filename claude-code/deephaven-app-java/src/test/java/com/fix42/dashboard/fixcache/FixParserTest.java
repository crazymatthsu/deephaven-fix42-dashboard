package com.fix42.dashboard.fixcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Wire-format parsing (doc 01 section 1).
 *
 * <p>Mirrors {@code deephaven-scripts/tests/test_parser.py}.
 */
class FixParserTest {

    private static final String SOH = String.valueOf(FixParser.SOH);

    @Test
    void parseFixSohDelimited() {
        String raw = "8=FIX.4.2" + SOH + "35=D" + SOH + "11=C1" + SOH + "38=1000" + SOH;
        Map<Integer, String> fields = FixParser.parseFix(raw);
        assertEquals("FIX.4.2", fields.get(8));
        assertEquals("D", fields.get(35));
        assertEquals("C1", fields.get(11));
        assertEquals("1000", fields.get(38));
    }

    @Test
    void parseFixPipeDelimited() {
        Map<Integer, String> fields = FixParser.parseFix("35=D|11=C1|38=1000|");
        assertEquals("D", fields.get(35));
        assertEquals("1000", fields.get(38));
    }

    @Test
    void parseFixSplitsOnFirstEqualsOnly() {
        Map<Integer, String> fields = FixParser.parseFix("35=8|58=key=value|");
        assertEquals("key=value", fields.get(58));
    }

    @Test
    void parseFixSkipsEmptySegments() {
        assertEquals(Map.of(35, "D", 11, "C1"), FixParser.parseFix("||35=D||11=C1||"));
    }

    @Test
    void parseFixIgnoresNonNumericAndUnterminatedSegments() {
        Map<Integer, String> fields = FixParser.parseFix("abc=1|35=D|noequals|=5|11=C1");
        assertEquals(Map.of(35, "D", 11, "C1"), fields);
    }

    @Test
    void parseFixEmptyInputReturnsEmptyMap() {
        assertTrue(FixParser.parseFix("").isEmpty());
        assertTrue(FixParser.parseFix(null).isEmpty());
    }

    @Test
    void parseFixDuplicateTagLastWins() {
        assertEquals("C2", FixParser.parseFix("35=D|11=C1|11=C2").get(11));
    }

    @Test
    void parseFixKeepsPipesInsideTextWhenSohDelimited() {
        String raw = "35=8" + SOH + "58=a|b|c" + SOH;
        assertEquals("a|b|c", FixParser.parseFix(raw).get(58));
    }

    @Test
    void parseFixToleratesTrailingNewline() {
        assertEquals("C1", FixParser.parseFix("35=D|11=C1|\r\n").get(11));
    }

    @Test
    void renderPipeReplacesSoh() {
        String raw = "35=D" + SOH + "11=C1" + SOH;
        assertEquals("35=D|11=C1|", FixParser.renderPipe(raw));
    }

    @Test
    void renderFieldsRoundTripsThroughParseFix() {
        Map<Integer, String> fields = new LinkedHashMap<>();
        fields.put(35, "D");
        fields.put(11, "C1");
        fields.put(38, "1000");
        assertEquals("35=D|11=C1|38=1000", FixParser.renderFields(fields));
        assertEquals(fields, FixParser.parseFix(FixParser.renderFields(fields)));
    }

    @Test
    void checksumOkTrueForValidVector() {
        assertEquals(Boolean.TRUE, FixParser.checksumOk(FixTestMessages.newOrder("C1").build()));
    }

    @Test
    void checksumOkMatchesIndependentComputation() {
        String message = FixTestMessages.newOrder("C1").delimiter(FixParser.SOH).build();
        int marker = message.lastIndexOf(SOH + "10=");
        int computed = 0;
        for (byte b : message.substring(0, marker + 1).getBytes(StandardCharsets.UTF_8)) {
            computed += (b & 0xFF);
        }
        assertEquals(String.format("%03d", computed % 256), FixParser.parseFix(message).get(10));
        assertEquals(Boolean.TRUE, FixParser.checksumOk(message));
    }

    @Test
    void checksumOkFalseForCorruptedChecksum() {
        String message = FixTestMessages.newOrder("C1").build();
        String declared = FixParser.parseFix(message).get(10);
        String wrong = String.format("%03d", (Integer.parseInt(declared) + 1) % 256);
        assertEquals(Boolean.FALSE, FixParser.checksumOk(message.replace("10=" + declared, "10=" + wrong)));
    }

    @Test
    void checksumOkFalseWhenBodyTampered() {
        String message = FixTestMessages.newOrder("C1").build().replace("55=IBM", "55=XXX");
        assertEquals(Boolean.FALSE, FixParser.checksumOk(message));
    }

    @Test
    void checksumOkNullWhenNoTag10() {
        assertNull(FixParser.checksumOk("35=D|11=C1|"));
        assertNull(FixParser.checksumOk(""));
        assertNull(FixParser.checksumOk(null));
    }

    @Test
    void checksumOkFalseForNonNumericChecksum() {
        String message = FixTestMessages.newOrder("C1").build();
        String declared = FixParser.parseFix(message).get(10);
        assertEquals(Boolean.FALSE, FixParser.checksumOk(message.replace("10=" + declared, "10=abc")));
    }

    @Test
    void checksumOkAcceptsPipeRenderedMessage() {
        String pipe = FixTestMessages.newOrder("C1").build();
        String soh = FixTestMessages.newOrder("C1").delimiter(FixParser.SOH).build();
        assertEquals(FixParser.checksumOk(soh), FixParser.checksumOk(pipe));
    }

    @Test
    @DisplayName("the checksum marker may sit at position 0")
    void checksumOkHandlesLeadingMarker() {
        assertEquals(Boolean.FALSE, FixParser.checksumOk("10=999"));
    }

    @Test
    @DisplayName("tag 9 BodyLength spans 35= through the SOH before 10= (doc 01 section 1)")
    void builderEmitsValidFraming() {
        String message = FixTestMessages.newOrder("C1").delimiter(FixParser.SOH).build();
        Map<Integer, String> fields = FixParser.parseFix(message);
        assertEquals("FIX.4.2", fields.get(8));
        assertEquals(3, fields.get(10).length());
        assertEquals(Boolean.TRUE, FixParser.checksumOk(message));

        int bodyStart = message.indexOf("35=");
        int bodyEnd = message.lastIndexOf(SOH + "10=") + 1;
        assertEquals(String.valueOf(bodyEnd - bodyStart), fields.get(9));
    }

    @ParameterizedTest
    @ValueSource(chars = {FixParser.SOH, FixParser.PIPE})
    void builderRoundTripsInBothDelimiters(char delimiter) {
        String message = FixTestMessages.newOrder("C1").delimiter(delimiter).build();
        assertEquals("C1", FixParser.parseFix(message).get(11));
        assertEquals(Boolean.TRUE, FixParser.checksumOk(message));
    }

    @Test
    void parseTransactTimeWithMillis() {
        Instant parsed = FixParser.parseTransactTime("20240115-14:30:00.123");
        assertEquals(Instant.parse("2024-01-15T14:30:00.123Z"), parsed);
    }

    @Test
    void parseTransactTimeWithoutMillis() {
        assertEquals(Instant.parse("2024-01-15T14:30:00Z"), FixParser.parseTransactTime("20240115-14:30:00"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "not a time", "20240115", "2024-01-15T14:30:00", "20240230-14:30:00"})
    void parseTransactTimeInvalidReturnsNull(String value) {
        assertNull(FixParser.parseTransactTime(value));
    }

    @Test
    void parseTransactTimeNullReturnsNull() {
        assertNull(FixParser.parseTransactTime(null));
    }

    @Test
    @DisplayName("python's %m/%d accept one or two digits, so 2024011 is 2024-01-01")
    void parseTransactTimeAcceptsSingleDigitMonthAndDay() {
        assertEquals(Instant.parse("2024-01-01T14:30:00Z"), FixParser.parseTransactTime("2024011-14:30:00"));
    }

    @Test
    @DisplayName("java.time would accept proleptic year 0; python's datetime does not")
    void parseTransactTimeRejectsYearZero() {
        assertNull(FixParser.parseTransactTime("00000101-00:00:00"));
    }

    @Test
    void parseTransactTimeRejectsLeftoverText() {
        assertNull(FixParser.parseTransactTime("20240115-14:30:00.1234567"));
        assertFalse(FixParser.parseTransactTime("20240115-14:30:00") == null);
    }
}
