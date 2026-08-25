package com.fix42.dashboard.amps.mapping;

import java.util.Arrays;

/**
 * One AMPS message mapped onto a {@link TableSchema}'s columns.
 *
 * <p>{@code present} is what makes delta handling correct: it distinguishes "this field was
 * not in the payload" (leave the stored value alone) from "this field was in the payload and
 * was empty" (clear the stored value). Both look like {@code null} in {@code values}.
 */
public final class MappedRow {

    /** What the row does to the target table. */
    public enum Action {
        /** Add the row (append-only) or replace the row for its key (keyed). */
        UPSERT,
        /** Remove the row for its key. Only meaningful for keyed tables. */
        DELETE
    }

    private final Object[] values;
    private final boolean[] present;
    private final Action action;
    private final String rowKey;

    /**
     * @param values column values in {@link TableSchema} order
     * @param present per-column flag: was this column's source field carried by the payload
     * @param action what to do with the row
     * @param rowKey the composite key, or {@code null} for an unkeyed table
     */
    public MappedRow(Object[] values, boolean[] present, Action action, String rowKey) {
        this.values = values;
        this.present = present;
        this.action = action;
        this.rowKey = rowKey;
    }

    /** Column values in schema order. Not copied -- treat as owned by this row. */
    public Object[] values() {
        return values;
    }

    /** Per-column "carried by the payload" flags, in schema order. */
    public boolean[] present() {
        return present;
    }

    public Action action() {
        return action;
    }

    /** The composite key, or {@code null} when the target table is not keyed. */
    public String rowKey() {
        return rowKey;
    }

    /** A copy of this row with different values and a fully-present mask. */
    public MappedRow withValues(Object[] merged, String key) {
        boolean[] all = new boolean[merged.length];
        Arrays.fill(all, true);
        return new MappedRow(merged, all, action, key);
    }

    @Override
    public String toString() {
        return action + " " + (rowKey == null ? "" : rowKey + " ") + Arrays.toString(values);
    }
}
