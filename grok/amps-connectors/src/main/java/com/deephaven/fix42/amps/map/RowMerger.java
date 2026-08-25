package com.deephaven.fix42.amps.map;

import com.deephaven.fix42.amps.config.FieldMappingProperties;
import com.deephaven.fix42.amps.config.UpdateMode;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds the row that is published to Deephaven.
 *
 * <p>Full publisher: every configured column is sent (missing AMPS fields become null).
 * Delta publisher: previously published values are kept for columns the AMPS message omitted.
 */
public final class RowMerger {
    private final List<FieldMappingProperties> fields;
    private final UpdateMode publisherMode;

    public RowMerger(List<FieldMappingProperties> fields, UpdateMode publisherMode) {
        this.fields = List.copyOf(fields);
        this.publisherMode = publisherMode;
    }

    public MappedRow merge(MappedRow previous, MappedRow incoming, boolean fullSnapshot) {
        if (fullSnapshot || publisherMode == UpdateMode.FULL || previous == null) {
            return completeFrom(incoming);
        }
        Map<String, Object> values = new LinkedHashMap<>();
        Set<String> present = new LinkedHashSet<>();
        for (FieldMappingProperties field : fields) {
            String col = field.getColumn();
            if (incoming.isPresent(col)) {
                values.put(col, incoming.get(col));
                present.add(col);
            } else if (previous.isPresent(col)) {
                values.put(col, previous.get(col));
                present.add(col);
            }
        }
        return new MappedRow(values, present);
    }

    private MappedRow completeFrom(MappedRow incoming) {
        Map<String, Object> values = new LinkedHashMap<>();
        Set<String> present = new LinkedHashSet<>();
        for (FieldMappingProperties field : fields) {
            String col = field.getColumn();
            present.add(col);
            if (incoming.isPresent(col)) {
                values.put(col, incoming.get(col));
            } else {
                values.put(col, null);
            }
        }
        return new MappedRow(values, present);
    }
}
