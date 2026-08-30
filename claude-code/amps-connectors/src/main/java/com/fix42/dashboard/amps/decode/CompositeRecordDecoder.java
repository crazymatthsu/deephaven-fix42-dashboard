package com.fix42.dashboard.amps.decode;

import com.fix42.dashboard.amps.source.AmpsRecord;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decoder for AMPS composite message types ({@code composite-local} / {@code composite-global}).
 *
 * <p>A composite message is a sequence of parts, each of its own constituent format; the
 * subscriber unframes them ({@code CompositeMessageParser}), so this decoder receives the
 * parts as strings and delegates each to the decoder for its configured format. Every decoded
 * field is registered part-indexed -- part 0's {@code orderId} becomes {@code 0.orderId} --
 * mirroring the {@code /0/orderId} XPaths AMPS uses to filter and key these types, and
 * additionally under its unprefixed tag when that is not already taken, so a
 * {@code composite-global} topic (one merged namespace server-side) can be mapped without
 * prefixes too. First part wins a contested bare tag, the same rule
 * {@link JsonRecordDecoder} applies to bare leaf names.
 *
 * <p>Part-count mismatches are not errors, in either direction: a payload with fewer parts
 * than configured simply carries none of the missing parts' fields (absent, exactly like a
 * field a delta omitted), and parts beyond the configured list are ignored -- the mapping
 * layer is an allowlist, and an unconfigured part is unmapped by definition.
 */
public final class CompositeRecordDecoder implements RecordDecoder {

    private final List<RecordDecoder> partDecoders;

    /**
     * @param partDecoders one decoder per configured part, in wire order
     */
    public CompositeRecordDecoder(List<RecordDecoder> partDecoders) {
        this.partDecoders = List.copyOf(partDecoders);
    }

    @Override
    public Map<String, String> decode(String payload) {
        // The wire framing is 4-byte binary length prefixes, which cannot round-trip through
        // a String; a composite record reaches this decoder already split into parts.
        throw new IllegalArgumentException(
                "a composite payload decodes from its parts, not from a single string");
    }

    @Override
    public Map<String, String> decode(AmpsRecord record) {
        Map<String, String> fields = new LinkedHashMap<>();
        List<String> parts = record.parts();
        if (parts == null) {
            return fields;
        }
        int count = Math.min(parts.size(), partDecoders.size());
        for (int i = 0; i < count; i++) {
            Map<String, String> part;
            try {
                part = partDecoders.get(i).decode(parts.get(i));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("part " + i + ": " + e.getMessage(), e);
            }
            for (Map.Entry<String, String> field : part.entrySet()) {
                fields.put(i + "." + field.getKey(), field.getValue());
                fields.putIfAbsent(field.getKey(), field.getValue());
            }
        }
        return fields;
    }
}
