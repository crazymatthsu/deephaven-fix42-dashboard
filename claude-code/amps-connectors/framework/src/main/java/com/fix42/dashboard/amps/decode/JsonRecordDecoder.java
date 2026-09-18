package com.fix42.dashboard.amps.decode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Decoder for JSON payloads.
 *
 * <p>The object is flattened to dotted paths, so a nested field is addressable as
 * {@code order.price}. Each leaf is additionally registered under its bare name when that name
 * is not already taken, so flat documents can be mapped with just {@code price}.
 *
 * <p>An explicit JSON {@code null} decodes to the empty string: present in the payload, with no
 * value. That is the encoding of "this field was cleared", which a delta update needs to be
 * able to say. Nested objects and arrays that a mapping addresses directly are handed on as
 * their JSON text.
 */
public final class JsonRecordDecoder implements RecordDecoder {

    private final ObjectMapper mapper;

    public JsonRecordDecoder(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Map<String, String> decode(String payload) {
        Map<String, String> fields = new LinkedHashMap<>();
        if (payload == null || payload.isBlank()) {
            return fields;
        }
        JsonNode root;
        try {
            root = mapper.readTree(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("payload is not valid JSON: " + e.getOriginalMessage(), e);
        }
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("JSON payload must be an object");
        }
        flatten(root, "", fields);
        return fields;
    }

    /**
     * Flatten one JSON object to this decoder's field shape: dotted paths, containers
     * registered as their JSON text, bare-name aliases for flat addressing. Shared with
     * {@code RecordExploder}, which applies the same shape to each exploded member value.
     *
     * @param root the object to flatten
     * @return its fields, keyed by path
     */
    public static Map<String, String> flatten(JsonNode root) {
        Map<String, String> fields = new LinkedHashMap<>();
        flatten(root, "", fields);
        return fields;
    }

    private static void flatten(JsonNode node, String prefix, Map<String, String> fields) {
        for (Map.Entry<String, JsonNode> member : node.properties()) {
            String name = member.getKey();
            String path = prefix.isEmpty() ? name : prefix + "." + name;
            JsonNode value = member.getValue();
            if (value.isObject()) {
                // Addressable both as the container (its JSON text) and through its members.
                put(fields, path, name, value.toString());
                flatten(value, path, fields);
            } else if (value.isArray()) {
                put(fields, path, name, value.toString());
            } else if (value.isNull()) {
                put(fields, path, name, "");
            } else {
                put(fields, path, name, value.asText());
            }
        }
    }

    private static void put(Map<String, String> fields, String path, String leaf, String value) {
        fields.put(path, value);
        // Bare-name alias, so a flat document does not need dotted mappings. First writer wins:
        // a shallower field keeps the short name when a deeper one shares it.
        fields.putIfAbsent(leaf, value);
    }
}
