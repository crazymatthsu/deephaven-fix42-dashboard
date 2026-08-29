package com.fix42.dashboard.fixcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    // ------------------------------------------------------------------ tag parsing

    @Test
    @DisplayName("a zero-padded tag reads as its value, the way python's int() does")
    void parseFixReadsZeroPaddedTags() {
        assertEquals("D", FixParser.parseFix("0000000035=D|0000000011=C1").get(35));
        assertEquals("C1", FixParser.parseFix("0000000035=D|0000000011=C1").get(11));
        assertEquals("D", FixParser.parseFix("035=D").get(35));
        assertEquals("x", FixParser.parseFix("0=x").get(0));
        assertEquals("x", FixParser.parseFix("0000=x").get(0));
    }

    @Test
    @DisplayName("non-ASCII decimal digits are tags too, as python's int() accepts them")
    void parseFixReadsUnicodeDecimalDigitTags() {
        // Arabic-Indic 35 and fullwidth 11.
        assertEquals("D", FixParser.parseFix("\u0663\u0665=D").get(35));
        assertEquals("C1", FixParser.parseFix("\uff11\uff11=C1").get(11));
    }

    @Test
    @DisplayName("python's str.strip() removes non-breaking spaces; String.strip() does not")
    void parseFixStripsTheWhitespacePythonStrips() {
        // U+00A0 NBSP, U+0085 NEL, U+2007 FIGURE SPACE, U+202F NARROW NBSP.
        for (String pad : new String[] {"\u00a0", "\u0085", "\u2007", "\u202f", " ", "\t"}) {
            assertEquals("D", FixParser.parseFix(pad + "35" + pad + "=D").get(35), "pad=" + pad);
        }
    }

    @Test
    @DisplayName("DOCUMENTED DEVIATION: a tag python raises on is skipped here, not fatal")
    void tagsPythonRaisesOnAreSkippedRatherThanFatal() {
        // python's parse_fix calls int(tag_text) unguarded, so both of these raise ValueError out
        // of parse_fix and make the WHOLE message unparseable. This parser drops the one field and
        // reads the rest. See FixParser.asTag for why the second case is not worth reproducing.
        Map<Integer, String> beyondInt = FixParser.parseFix("12345678901=zz|35=D|11=C1");
        assertEquals(Map.of(35, "D", 11, "C1"), beyondInt);

        Map<Integer, String> superscript = FixParser.parseFix("\u00b2=x|35=D|11=C1");
        assertEquals(Map.of(35, "D", 11, "C1"), superscript);
    }

    @Test
    @DisplayName("a very long digit run must not blow the stack (it is an Error, not an Exception)")
    void longDigitRunsAreStackSafe() {
        String raw = "35=D|11=LONG|1=A|55=S|54=1|40=2|59=0|38=" + "1".repeat(100_000);
        Result result = new OrderStateMachine(new FakeClock()).process(raw);
        // python overflows the float and reports it the same way; the point is that nothing escapes.
        assertNotNull(result.error());
        assertTrue(result.error().contains("OverflowError"), result.error());
    }

    @Test
    void parseTransactTimeRejectsLeftoverText() {
        assertNull(FixParser.parseTransactTime("20240115-14:30:00.1234567"));
        assertFalse(FixParser.parseTransactTime("20240115-14:30:00") == null);
    }
}
