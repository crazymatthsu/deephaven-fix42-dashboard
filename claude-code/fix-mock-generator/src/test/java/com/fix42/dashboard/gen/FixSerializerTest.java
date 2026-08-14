package com.fix42.dashboard.gen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Wire-format tests for {@link FixSerializer} — doc 01 §1. */
class FixSerializerTest {

    /**
     * The canonical published FIX 4.2 Logon example, whose {@code 9=65} and {@code 10=062} are
     * independently known; used as the hand-computed framing vector.
     */
    private static final String LOGON_VECTOR =
            "8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|108=30|10=062|";

    /** Hand-computed NewOrderSingle vector in this project's header/body layout. */
    private static final String NOS_VECTOR =
            "8=FIX.4.2|9=149|35=D|49=CLIENT|56=MOCKVENUE|34=1|52=20250814-12:00:00.000|11=C-0001-1|"
                    + "1=ACC-1|21=1|55=AAPL|54=1|60=20250814-12:00:00.000|38=1000|40=2|44=185.50|59=0|10=081|";

    private static FixMessage logon() {
        return FixMessage.create("A")
                .set(49, "SERVER")
                .set(56, "CLIENT")
                .set(34, 177)
                .set(52, "20090107-18:15:16")
                .set(98, "0")
                .set(108, "30");
    }

    private static FixMessage newOrderSingle() {
        return FixMessage.create("D")
                .set(49, "CLIENT")
                .set(56, "MOCKVENUE")
                .set(34, 1)
                .set(52, "20250814-12:00:00.000")
                .set(11, "C-0001-1")
                .set(1, "ACC-1")
                .set(21, "1")
                .set(55, "AAPL")
                .set(54, "1")
                .set(60, "20250814-12:00:00.000")
                .setQty(38, 1000)
                .set(40, "2")
                .setPrice(44, 185.50)
                .set(59, "0");
    }

    @Test
    @DisplayName("hand-computed vector: full serialized form matches byte for byte")
    void logonVectorMatches() {
        assertEquals(LOGON_VECTOR, FixSerializer.renderPipe(FixSerializer.serialize(logon())));
    }

    @Test
    @DisplayName("BodyLength counts bytes from '35=' through the SOH before '10='")
    void bodyLengthVector() {
        String raw = FixSerializer.serialize(logon());
        assertEquals("65", TestFix.parse(raw).get(FixTags.BODY_LENGTH));
        assertEquals(65, FixSerializer.bodyLength(raw));
        assertEquals(65, TestFix.computedBodyLength(raw));
    }

    @Test
    @DisplayName("CheckSum is the mod-256 byte sum, zero-padded to three digits")
    void checksumVectorIsZeroPadded() {
        String raw = FixSerializer.serialize(logon());
        String checksum = TestFix.parse(raw).get(FixTags.CHECK_SUM);
        assertEquals("062", checksum);
        assertEquals(3, checksum.length());
        assertEquals(TestFix.computedChecksum(raw), checksum);
    }

    @Test
    @DisplayName("NewOrderSingle vector: BodyLength 149, CheckSum 081")
    void newOrderSingleVector() {
        assertEquals(NOS_VECTOR, FixSerializer.renderPipe(FixSerializer.serialize(newOrderSingle())));
    }

    @Test
    @DisplayName("header order is 8, 9, 35, then insertion order, then 10")
    void headerOrder() {
        List<Integer> tags = TestFix.tagOrder(FixSerializer.serialize(newOrderSingle()));
        assertEquals(List.of(8, 9, 35), tags.subList(0, 3));
        assertEquals(10, tags.get(tags.size() - 1));
        assertEquals(List.of(49, 56, 34, 52, 11, 1, 21, 55, 54, 60, 38, 40, 44, 59),
                tags.subList(3, tags.size() - 1));
    }

    @Test
    @DisplayName("every field is SOH-terminated, including the trailer")
    void sohPlacement() {
        String raw = FixSerializer.serialize(newOrderSingle());
        assertEquals(FixTags.SOH, raw.charAt(raw.length() - 1));
        long sohCount = raw.chars().filter(c -> c == FixTags.SOH).count();
        assertEquals(TestFix.tagOrder(raw).size(), sohCount);
        assertTrue(raw.startsWith("8=FIX.4.2" + FixTags.SOH));
    }

    @Test
    @DisplayName("renderPipe swaps SOH for '|' and leaves everything else alone")
    void renderPipe() {
        String raw = FixSerializer.serialize(logon());
        String rendered = FixSerializer.renderPipe(raw);
        assertEquals(raw.length(), rendered.length());
        assertEquals(0, rendered.chars().filter(c -> c == FixTags.SOH).count());
        assertEquals(raw, rendered.replace(FixTags.PIPE, FixTags.SOH));
    }

    @Test
    @DisplayName("values containing '=' survive: parsers split on the first '=' only")
    void valuesMayContainEquals() {
        FixMessage msg = FixMessage.create("8").set(58, "reject: qty=0 not allowed");
        Map<Integer, String> parsed = TestFix.parse(FixSerializer.serialize(msg));
        assertEquals("reject: qty=0 not allowed", parsed.get(58));
        assertTrue(TestFix.framingValid(FixSerializer.serialize(msg)));
    }

    @Test
    @DisplayName("framing tags 9 and 10 are owned by the serializer and cannot be set by hand")
    void framingTagsAreReserved() {
        FixMessage msg = FixMessage.create("D");
        assertThrows(IllegalArgumentException.class, () -> msg.set(FixTags.BODY_LENGTH, "10"));
        assertThrows(IllegalArgumentException.class, () -> msg.set(FixTags.CHECK_SUM, "000"));
    }

    @Test
    @DisplayName("a value containing SOH is rejected rather than corrupting the frame")
    void sohInValueRejected() {
        FixMessage msg = FixMessage.create("D");
        assertThrows(IllegalArgumentException.class, () -> msg.set(58, "a" + FixTags.SOH + "b"));
    }
}
