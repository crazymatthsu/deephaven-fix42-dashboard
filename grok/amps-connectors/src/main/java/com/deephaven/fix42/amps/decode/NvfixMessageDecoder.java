package com.deephaven.fix42.amps.decode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** AMPS NVFIX: named tags. Only configured tags are returned. */
public final class NvfixMessageDecoder implements MessageDecoder {
    private final Set<String> wantedSources;

    public NvfixMessageDecoder(Set<String> wantedSources) {
        this.wantedSources = Set.copyOf(wantedSources);
    }

    @Override
    public ParsedFields decode(String payload) {
        Map<String, String> parsed = TagValueParser.parse(payload);
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : parsed.entrySet()) {
            if (wantedSources.contains(e.getKey())) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return new ParsedFields(out);
    }
}
