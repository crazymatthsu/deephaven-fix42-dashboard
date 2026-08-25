package com.deephaven.fix42.amps.map;

import java.util.List;

public final class RowKey {
    private static final char SEP = '\u001f';

    private RowKey() {}

    public static String of(MappedRow row, List<String> keyColumns) {
        if (keyColumns == null || keyColumns.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyColumns.size(); i++) {
            if (i > 0) {
                sb.append(SEP);
            }
            Object v = row.get(keyColumns.get(i));
            if (v == null) {
                return null;
            }
            sb.append(v);
        }
        return sb.toString();
    }

    public static MappedRow keyOnly(MappedRow row, List<String> keyColumns) {
        java.util.Map<String, Object> values = new java.util.LinkedHashMap<>();
        java.util.Set<String> present = new java.util.LinkedHashSet<>();
        for (String col : keyColumns) {
            present.add(col);
            values.put(col, row.get(col));
        }
        return new MappedRow(values, present);
    }
}
