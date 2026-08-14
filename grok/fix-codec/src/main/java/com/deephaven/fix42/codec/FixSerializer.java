package com.deephaven.fix42.codec;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Rebuilds a legal FIX 4.2 string (SOH-delimited) with computed 9 and 10. */
public final class FixSerializer {
    public String serialize(FixMessage message) {
        return serialize(message.fields());
    }

    public String serialize(List<FixField> fields) {
        List<FixField> body = new ArrayList<>();
        String begin = FixConstants.BEGIN_STRING_42;
        String msgType = null;
        for (FixField field : fields) {
            switch (field.tag()) {
                case Tags.BEGIN_STRING -> begin = field.value();
                case Tags.BODY_LENGTH, Tags.CHECK_SUM -> {
                    // recomputed
                }
                case Tags.MSG_TYPE -> msgType = field.value();
                default -> body.add(field);
            }
        }
        if (msgType == null) {
            throw new FixParseException("cannot serialize without MsgType (35)");
        }
        StringBuilder bodyAndType = new StringBuilder();
        append(bodyAndType, Tags.MSG_TYPE, msgType);
        for (FixField field : body) {
            append(bodyAndType, field.tag(), field.value());
        }
        String bodyBytes = bodyAndType.toString();
        StringBuilder full = new StringBuilder();
        append(full, Tags.BEGIN_STRING, begin);
        append(full, Tags.BODY_LENGTH, Integer.toString(bodyBytes.getBytes(StandardCharsets.US_ASCII).length));
        full.append(bodyBytes);
        String checksum = FixParser.checksum(full.toString());
        append(full, Tags.CHECK_SUM, checksum);
        return full.toString();
    }

    /** Human-readable form that uses {@code |} instead of SOH. */
    public String serializePretty(List<FixField> fields) {
        return serialize(fields).replace(FixConstants.SOH, FixConstants.PIPE);
    }

    public String serializePretty(FixMessage message) {
        return serializePretty(message.fields());
    }

    private static void append(StringBuilder sb, int tag, String value) {
        sb.append(tag).append('=').append(value).append(FixConstants.SOH);
    }
}
