package com.fix42.dashboard.gen;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Test-only FIX helpers: an independent parser and framing checker for asserting generator output. */
final class TestFix {

    private TestFix() {}

    /** Splits an SOH- or pipe-delimited message on the first {@code '='} of each field. */
    static Map<Integer, String> parse(String raw) {
        Map<Integer, String> fields = new LinkedHashMap<>();
        for (String field : raw.replace(FixTags.PIPE, FixTags.SOH).split(String.valueOf(FixTags.SOH))) {
            if (field.isEmpty()) {
                continue;
            }
            int eq = field.indexOf('=');
            fields.put(Integer.parseInt(field.substring(0, eq)), field.substring(eq + 1));
        }
        return fields;
    }

    /** Tag numbers in wire order, including duplicates. */
    static List<Integer> tagOrder(String raw) {
        List<Integer> tags = new ArrayList<>();
        for (String field : raw.replace(FixTags.PIPE, FixTags.SOH).split(String.valueOf(FixTags.SOH))) {
            if (!field.isEmpty()) {
                tags.add(Integer.parseInt(field.substring(0, field.indexOf('='))));
            }
        }
        return tags;
    }

    /** Recomputes {@code 9 BodyLength} independently of {@link FixSerializer}. */
    static int computedBodyLength(String raw) {
        int start = raw.indexOf("35=");
        int end = raw.lastIndexOf(FixTags.SOH + "10=");
        return raw.substring(start, end + 1).getBytes(StandardCharsets.ISO_8859_1).length;
    }

    /** Recomputes {@code 10 CheckSum} independently of {@link FixSerializer}. */
    static String computedChecksum(String raw) {
        int end = raw.lastIndexOf(FixTags.SOH + "10=");
        int sum = 0;
        for (byte b : raw.substring(0, end + 1).getBytes(StandardCharsets.ISO_8859_1)) {
            sum += (b & 0xFF);
        }
        return String.format(java.util.Locale.ROOT, "%03d", sum % 256);
    }

    /** Asserts nothing; returns true when both framing tags of a serialized message are correct. */
    static boolean framingValid(String raw) {
        Map<Integer, String> fields = parse(raw);
        return Integer.toString(computedBodyLength(raw)).equals(fields.get(FixTags.BODY_LENGTH))
                && computedChecksum(raw).equals(fields.get(FixTags.CHECK_SUM));
    }

    /** All messages of one chain, in wire order. */
    static List<Map<Integer, String>> chain(ScenarioEngine.GeneratedBatch batch, String chainKey) {
        return batch.messages().stream()
                .filter(m -> m.chainKey().equals(chainKey))
                .map(m -> parse(FixSerializer.serialize(m.message())))
                .toList();
    }

    /** Values of {@code tag} across a chain, skipping messages that lack it. */
    static List<String> values(List<Map<Integer, String>> chain, int tag) {
        return chain.stream().filter(m -> m.containsKey(tag)).map(m -> m.get(tag)).toList();
    }

    /** {@code 35 MsgType} sequence of a chain. */
    static List<String> msgTypes(List<Map<Integer, String>> chain) {
        return chain.stream().map(m -> m.get(FixTags.MSG_TYPE)).toList();
    }

    /** Generates one chain for a single scenario and returns its parsed messages. */
    static List<Map<Integer, String>> singleChain(ScenarioCatalog scenario, long seed) {
        ScenarioEngine engine = new ScenarioEngine(seed, java.time.Instant.parse("2025-08-14T12:00:00Z"));
        ScenarioEngine.GeneratedBatch batch = engine.generate(1, scenario.cliName());
        return chain(batch, batch.chains().get(0).chainKey());
    }
}
