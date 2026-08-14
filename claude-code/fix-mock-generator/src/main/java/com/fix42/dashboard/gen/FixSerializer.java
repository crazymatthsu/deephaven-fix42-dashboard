package com.fix42.dashboard.gen;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Renders a {@link FixMessage} to the FIX 4.2 wire format.
 *
 * <p>Per {@code docs/01-fix42-messages-and-state-machine.md} §1:
 * <ul>
 *   <li>fields are {@code tag=value} pairs, each terminated by SOH (byte {@code 0x01});</li>
 *   <li>{@code 8 BeginString}, {@code 9 BodyLength}, {@code 35 MsgType} lead, in that order;</li>
 *   <li>{@code 9 BodyLength} counts the bytes from the first character of {@code "35="} through the
 *       SOH before {@code "10="};</li>
 *   <li>{@code 10 CheckSum} is the sum of every byte up to and including that SOH, modulo 256,
 *       zero-padded to three digits.</li>
 * </ul>
 */
public final class FixSerializer {

    private FixSerializer() {}

    /** Serializes to the SOH-delimited wire form, with correct {@code 9} and {@code 10}. */
    public static String serialize(FixMessage message) {
        String msgType = message.msgType();
        if (msgType == null) {
            throw new IllegalArgumentException("message has no 35 MsgType");
        }
        String beginString = message.has(FixTags.BEGIN_STRING)
                ? message.get(FixTags.BEGIN_STRING)
                : FixTags.BEGIN_STRING_FIX42;

        StringBuilder body = new StringBuilder(256);
        appendField(body, FixTags.MSG_TYPE, msgType);
        for (Map.Entry<Integer, String> e : message.fields().entrySet()) {
            int tag = e.getKey();
            if (tag == FixTags.BEGIN_STRING || tag == FixTags.MSG_TYPE) {
                continue;
            }
            appendField(body, tag, e.getValue());
        }

        StringBuilder head = new StringBuilder(body.length() + 32);
        appendField(head, FixTags.BEGIN_STRING, beginString);
        appendField(head, FixTags.BODY_LENGTH, Integer.toString(byteLength(body)));
        head.append(body);

        String withoutChecksum = head.toString();
        return withoutChecksum + FixTags.CHECK_SUM + "=" + checksum(withoutChecksum) + FixTags.SOH;
    }

    /**
     * Computes the three-digit {@code 10 CheckSum} value for everything up to and including the SOH
     * that precedes {@code "10="}.
     */
    public static String checksum(String upToChecksumField) {
        int sum = 0;
        for (byte b : upToChecksumField.getBytes(StandardCharsets.ISO_8859_1)) {
            sum += (b & 0xFF);
        }
        return String.format(java.util.Locale.ROOT, "%03d", sum % 256);
    }

    /** Byte count of {@code 9 BodyLength}: {@code "35="} through the SOH before {@code "10="}. */
    public static int bodyLength(String serialized) {
        int start = serialized.indexOf(FixTags.MSG_TYPE + "=");
        int end = serialized.lastIndexOf(FixTags.SOH + "" + FixTags.CHECK_SUM + "=");
        if (start < 0 || end < 0) {
            throw new IllegalArgumentException("not a serialized FIX message: " + renderPipe(serialized));
        }
        return serialized.substring(start, end + 1).getBytes(StandardCharsets.ISO_8859_1).length;
    }

    /** Replaces SOH with {@code '|'} for human-readable display (dry-run output, audit column). */
    public static String renderPipe(String serialized) {
        return serialized.replace(FixTags.SOH, FixTags.PIPE);
    }

    private static void appendField(StringBuilder sb, int tag, String value) {
        sb.append(tag).append('=').append(value).append(FixTags.SOH);
    }

    private static int byteLength(CharSequence cs) {
        return cs.toString().getBytes(StandardCharsets.ISO_8859_1).length;
    }
}
