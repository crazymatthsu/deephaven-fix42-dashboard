package com.fix42.dashboard.amps.decode;

import java.util.Map;

/**
 * Decodes an AMPS payload into {@code tag -> raw value}.
 *
 * <p>All three formats decode to the same shape, so everything downstream -- mapping,
 * delta merging, batching, publishing -- is written once and is format agnostic. What "tag"
 * means is the format's business: a FIX tag number, an NVFIX field name, or a JSON path.
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
}
