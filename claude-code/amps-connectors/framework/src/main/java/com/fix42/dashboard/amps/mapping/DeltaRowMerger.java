package com.fix42.dashboard.amps.mapping;

import java.util.HashMap;
import java.util.Map;

/**
 * Merges partial rows over the last complete row published for the same key.
 *
 * <p>This is what makes {@code deephaven.publish-mode: DELTA} safe. Adding a row to a
 * Deephaven keyed input table replaces that key's row wholesale, so a partial row published
 * as-is would null out every column the update did not mention. The merger keeps the last
 * complete row per key and fills the gaps before publishing.
 *
 * <p>Not thread safe: each connector owns one merger, driven from its own message thread.
 */
public final class DeltaRowMerger {

    private final TableSchema schema;
    private final Map<String, Object[]> lastByKey = new HashMap<>();

    public DeltaRowMerger(TableSchema schema) {
        this.schema = schema;
    }

    /**
     * Merge a row over the stored state for its key and remember the result.
     *
     * <p>A {@link MappedRow.Action#DELETE} forgets the key instead, and is returned unchanged.
     *
     * @param row the freshly mapped row
     * @return a row whose every column is populated from the merged state
     */
    public MappedRow merge(MappedRow row) {
        String key = row.rowKey();
        if (key == null) {
            return row;
        }
        if (row.action() == MappedRow.Action.DELETE) {
            lastByKey.remove(key);
            return row;
        }

        Object[] previous = lastByKey.get(key);
        Object[] merged;
        if (previous == null) {
            merged = row.values().clone();
        } else {
            merged = new Object[schema.size()];
            for (int i = 0; i < merged.length; i++) {
                merged[i] = row.present()[i] ? row.values()[i] : previous[i];
            }
        }
        lastByKey.put(key, merged);
        return row.withValues(merged, schema.rowKey(merged));
    }

    /** Drop all remembered state -- used when a connector restarts and replays from scratch. */
    public void clear() {
        lastByKey.clear();
    }

    /** Number of keys currently remembered. */
    public int size() {
        return lastByKey.size();
    }
}
