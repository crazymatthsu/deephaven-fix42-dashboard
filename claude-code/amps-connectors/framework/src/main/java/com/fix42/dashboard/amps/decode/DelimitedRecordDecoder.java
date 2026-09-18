package com.fix42.dashboard.amps.decode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Decoder for the two delimited formats, FIX and NVFIX.
 *
 * <p>Both are {@code key=value} pairs separated by a field separator (SOH by default); they
 * differ only in whether the key is a FIX tag number or a field name, which this layer does
 * not care about. Splitting on the <em>first</em> {@code =} keeps values containing {@code =}
 * intact.
 *
 * <p>Repeated keys resolve last-one-wins, matching how a FIX engine reads a flat body.
 */
public final class DelimitedRecordDecoder implements RecordDecoder {

    private final char separator;

    /**
     * @param separator the field separator, normally SOH
     */
    public DelimitedRecordDecoder(char separator) {
        this.separator = separator;
    }

    @Override
    public Map<String, String> decode(String payload) {
        Map<String, String> fields = new LinkedHashMap<>();
        if (payload == null || payload.isEmpty()) {
            return fields;
        }
        int start = 0;
        int length = payload.length();
        while (start <= length) {
            int end = payload.indexOf(separator, start);
            if (end < 0) {
                end = length;
            }
            if (end > start) {
                addPair(fields, payload, start, end);
            }
            start = end + 1;
        }
        return fields;
    }

    private static void addPair(Map<String, String> fields, String payload, int start, int end) {
        int equals = payload.indexOf('=', start);
        if (equals < 0 || equals >= end) {
            // A segment with no '=' is not a field; ignore it rather than failing the message.
            return;
        }
        String key = payload.substring(start, equals).trim();
        if (key.isEmpty()) {
            return;
        }
        fields.put(key, payload.substring(equals + 1, end));
    }
}
