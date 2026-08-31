package com.fix42.dashboard.amps.decode;

import com.fix42.dashboard.amps.source.AmpsRecord;
import java.util.Map;

/**
 * Decodes an AMPS payload into {@code tag -> raw value}.
 *
 * <p>All formats decode to the same shape, so everything downstream -- mapping,
 * delta merging, batching, publishing -- is written once and is format agnostic. What "tag"
 * means is the format's business: a FIX tag number, an NVFIX field name, a JSON path, or a
 * part-indexed path such as {@code 0.orderId} for a composite message.
 *
 * <p>A key present in the returned map means the payload carried that field, which is how a
 * delta update's "unchanged" is told apart from "cleared": absent key versus empty value.
 */
@FunctionalInterface
public interface RecordDecoder {

    /**
     * @param payload the raw AMPS message data
     * @return the fields it carried, keyed by tag; empty when the payload carries none
     * @throws IllegalArgumentException if the payload is malformed for this format
     */
    Map<String, String> decode(String payload);

    /**
     * Decode a whole record. The default reads {@link AmpsRecord#data()}; a decoder for a
     * multi-part format overrides this to read {@link AmpsRecord#parts()} instead.
     *
     * @param record the record as the subscriber delivered it
     * @return the fields it carried, keyed by tag
     * @throws IllegalArgumentException if the payload is malformed for this format
     */
    default Map<String, String> decode(AmpsRecord record) {
        return decode(record.data());
    }
}
