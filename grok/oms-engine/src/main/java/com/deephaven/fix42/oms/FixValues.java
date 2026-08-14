package com.deephaven.fix42.oms;

import com.deephaven.fix42.codec.FixMessage;

final class FixValues {
    private FixValues() {}

    static String str(FixMessage msg, int tag) {
        return msg.getOrEmpty(tag);
    }

    static boolean present(FixMessage msg, int tag) {
        String v = msg.getOrEmpty(tag);
        return v != null && !v.isEmpty();
    }

    static Double number(FixMessage msg, int tag) {
        String v = msg.getOrEmpty(tag);
        if (v == null || v.isEmpty()) {
            return null;
        }
        try {
            return Double.valueOf(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("tag " + tag + " is not numeric: " + v, e);
        }
    }

    static void mergeString(java.util.function.Consumer<String> setter, String incoming) {
        if (incoming != null && !incoming.isEmpty()) {
            setter.accept(incoming);
        }
    }

    static void mergeNumber(java.util.function.DoubleConsumer setter, Double incoming) {
        if (incoming != null) {
            setter.accept(incoming);
        }
    }

    /**
     * FIX UTCTimestamp {@code YYYYMMDD-HH:MM:SS[.sss]} compares lexicographically.
     *
     * @return negative if a &lt; b, 0 if equal, positive if a &gt; b. Empty is oldest.
     */
    static int compareTransactTime(String a, String b) {
        if (a == null || a.isEmpty()) {
            return (b == null || b.isEmpty()) ? 0 : -1;
        }
        if (b == null || b.isEmpty()) {
            return 1;
        }
        return a.compareTo(b);
    }
}
