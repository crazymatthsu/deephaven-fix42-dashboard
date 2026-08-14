package com.deephaven.fix42.oms;

import com.deephaven.fix42.codec.FixField;
import com.deephaven.fix42.codec.Tags;

import java.util.ArrayList;
import java.util.List;

/** Builds pipe-delimited FIX bodies for tests (lenient parser). */
final class FixTape {
    private FixTape() {}

    static String msg(String type, String... tagValues) {
        List<FixField> fields = new ArrayList<>();
        fields.add(new FixField(Tags.BEGIN_STRING, "FIX.4.2"));
        fields.add(new FixField(Tags.MSG_TYPE, type));
        for (String tv : tagValues) {
            int eq = tv.indexOf('=');
            fields.add(new FixField(Integer.parseInt(tv.substring(0, eq)), tv.substring(eq + 1)));
        }
        StringBuilder sb = new StringBuilder();
        for (FixField field : fields) {
            sb.append(field.tag()).append('=').append(field.value()).append('|');
        }
        sb.append("10=000|");
        return sb.toString();
    }

    static String d(String cl, String acct, String sym, String qty, String px) {
        return msg(
                "D",
                "11=" + cl,
                "1=" + acct,
                "55=" + sym,
                "54=1",
                "38=" + qty,
                "40=2",
                "44=" + px,
                "59=0",
                "60=20260814-10:00:00");
    }

    static String er(
            String cl,
            String orderId,
            String execId,
            String execType,
            String status,
            String cum,
            String leaves,
            String... extra) {
        List<String> fields = new ArrayList<>();
        fields.add("11=" + cl);
        fields.add("37=" + orderId);
        fields.add("17=" + execId);
        fields.add("20=0");
        fields.add("150=" + execType);
        fields.add("39=" + status);
        fields.add("14=" + cum);
        fields.add("151=" + leaves);
        fields.add("6=0");
        List<String> extraList = List.of(extra);
        boolean hasTx = extraList.stream().anyMatch(s -> s.startsWith("60="));
        if (!hasTx) {
            fields.add("60=20260814-10:00:01");
        }
        fields.addAll(extraList);
        return msg("8", fields.toArray(String[]::new));
    }
}
