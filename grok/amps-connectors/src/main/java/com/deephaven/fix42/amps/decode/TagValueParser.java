package com.deephaven.fix42.amps.decode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Splits AMPS FIX / NVFIX payloads on SOH or {@code |}. Last value wins for a repeated tag.
 */
public final class TagValueParser {
    public static final char SOH = '\u0001';
    public static final char PIPE = '|';

    private TagValueParser() {}

    public static Map<String, String> parse(String raw) {
        Map<String, String> out = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) {
            return out;
        }
        String normalized = raw.trim();
        char delimiter = detectDelimiter(normalized);
        String[] pairs;
        if (delimiter == 0) {
            pairs = new String[] {normalized};
        } else {
            if (normalized.charAt(normalized.length() - 1) == delimiter) {
                normalized = normalized.substring(0, normalized.length() - 1);
            }
            pairs = normalized.split(java.util.regex.Pattern.quote(String.valueOf(delimiter)), -1);
        }
        for (String pair : pairs) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                throw new DecodeException("malformed tag=value field: " + pair);
            }
            out.put(pair.substring(0, eq), pair.substring(eq + 1));
        }
        return out;
    }

    private static char detectDelimiter(String raw) {
        if (raw.indexOf(SOH) >= 0) {
            return SOH;
        }
        if (raw.indexOf(PIPE) >= 0) {
            return PIPE;
        }
        return 0;
    }
}
