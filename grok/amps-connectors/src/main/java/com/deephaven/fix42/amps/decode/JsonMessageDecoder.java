package com.deephaven.fix42.amps.decode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** AMPS JSON. Only configured field names / paths are returned. */
public final class JsonMessageDecoder implements MessageDecoder {
    private final ObjectMapper mapper;
    private final List<String> wantedSources;

    public JsonMessageDecoder(ObjectMapper mapper, List<String> wantedSources) {
        this.mapper = mapper;
        this.wantedSources = List.copyOf(wantedSources);
    }

    @Override
    public ParsedFields decode(String payload) {
        if (payload == null || payload.isBlank()) {
            return ParsedFields.empty();
        }
        JsonNode root;
        try {
            root = mapper.readTree(payload);
        } catch (JsonProcessingException e) {
            throw new DecodeException("invalid JSON payload", e);
        }
        if (root == null || root.isNull() || root.isMissingNode()) {
            return ParsedFields.empty();
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (String source : wantedSources) {
            JsonNode node = resolve(root, source);
            if (node == null || node.isMissingNode()) {
                continue;
            }
            if (node.isNull()) {
                out.put(source, null);
            } else if (node.isValueNode()) {
                out.put(source, node.asText());
            } else {
                out.put(source, node.toString());
            }
        }
        return new ParsedFields(out);
    }

    static JsonNode resolve(JsonNode root, String source) {
        if (source.startsWith("/")) {
            return root.at(source);
        }
        if (source.indexOf('.') >= 0 || source.indexOf('/') >= 0) {
            String pointer = source.startsWith("/") ? source : "/" + source.replace('.', '/');
            return root.at(pointer);
        }
        return root.get(source);
    }
}
