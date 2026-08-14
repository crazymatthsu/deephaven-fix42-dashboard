package com.deephaven.fix42.codec;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits a FIX 4.2 string on SOH or {@code |}. {@code |} is accepted for tests
 * and logs; the wire form uses SOH.
 */
public final class FixParser {
    private final ParserConfig config;

    public FixParser() {
        this(ParserConfig.defaults());
    }

    public FixParser(ParserConfig config) {
        this.config = config;
    }

    public FixMessage parse(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new FixParseException("empty FIX message");
        }
        String normalized = raw.trim();
        char delimiter = detectDelimiter(normalized);
        String[] pairs = splitPairs(normalized, delimiter);
        List<FixField> fields = new ArrayList<>(pairs.length);
        for (String pair : pairs) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                throw new FixParseException("malformed field: " + pair);
            }
            int tag;
            try {
                tag = Integer.parseInt(pair.substring(0, eq));
            } catch (NumberFormatException e) {
                throw new FixParseException("non-numeric tag: " + pair, e);
            }
            fields.add(new FixField(tag, pair.substring(eq + 1)));
        }
        if (fields.isEmpty()) {
            throw new FixParseException("no fields in FIX message");
        }
        if (config.strictHeader()) {
            validateHeader(fields);
        }
        if (config.validateChecksum()) {
            validateChecksumAndLength(normalized, delimiter, fields);
        }
        return new FixMessage(fields, normalized);
    }

    private static char detectDelimiter(String raw) {
        if (raw.indexOf(FixConstants.SOH) >= 0) {
            return FixConstants.SOH;
        }
        if (raw.indexOf(FixConstants.PIPE) >= 0) {
            return FixConstants.PIPE;
        }
        throw new FixParseException("no SOH or | delimiter found");
    }

    private static String[] splitPairs(String raw, char delimiter) {
        if (raw.charAt(raw.length() - 1) == delimiter) {
            return raw.substring(0, raw.length() - 1).split(java.util.regex.Pattern.quote(String.valueOf(delimiter)), -1);
        }
        return raw.split(java.util.regex.Pattern.quote(String.valueOf(delimiter)), -1);
    }

    private static void validateHeader(List<FixField> fields) {
        if (fields.size() < 4) {
            throw new FixParseException("message shorter than header+trailer");
        }
        requireTagAt(fields, 0, Tags.BEGIN_STRING);
        requireTagAt(fields, 1, Tags.BODY_LENGTH);
        requireTagAt(fields, 2, Tags.MSG_TYPE);
        FixField last = fields.get(fields.size() - 1);
        if (last.tag() != Tags.CHECK_SUM) {
            throw new FixParseException("last field must be CheckSum (10), got " + last.tag());
        }
        if (!FixConstants.BEGIN_STRING_42.equals(fields.get(0).value())) {
            throw new FixParseException("unsupported BeginString: " + fields.get(0).value());
        }
    }

    private static void requireTagAt(List<FixField> fields, int index, int tag) {
        if (fields.get(index).tag() != tag) {
            throw new FixParseException(
                    "field[" + index + "] must be tag " + tag + ", got " + fields.get(index).tag());
        }
    }

    private static void validateChecksumAndLength(String raw, char delimiter, List<FixField> fields) {
        String sohForm = delimiter == FixConstants.SOH ? raw : raw.replace(FixConstants.PIPE, FixConstants.SOH);
        if (!sohForm.endsWith(String.valueOf(FixConstants.SOH))) {
            sohForm = sohForm + FixConstants.SOH;
        }
        int checksumIdx = sohForm.lastIndexOf("10=");
        if (checksumIdx < 0) {
            throw new FixParseException("missing CheckSum");
        }
        String beforeChecksum = sohForm.substring(0, checksumIdx);
        String declared = fields.get(fields.size() - 1).value();
        String actual = checksum(beforeChecksum);
        if (!actual.equals(declared)) {
            throw new FixParseException("checksum mismatch: declared=" + declared + " actual=" + actual);
        }
        String bodyLengthValue = fields.get(1).value();
        int beginBody = sohForm.indexOf("35=");
        if (beginBody < 0) {
            throw new FixParseException("missing MsgType for BodyLength check");
        }
        int expected = Integer.parseInt(bodyLengthValue);
        int actualLen = checksumIdx - beginBody;
        if (expected != actualLen) {
            throw new FixParseException("body length mismatch: declared=" + expected + " actual=" + actualLen);
        }
    }

    static String checksum(String prefixIncludingTrailingSoh) {
        byte[] bytes = prefixIncludingTrailingSoh.getBytes(StandardCharsets.US_ASCII);
        int sum = 0;
        for (byte b : bytes) {
            sum += b & 0xff;
        }
        return String.format("%03d", sum % 256);
    }
}
