package com.fix42.dashboard.amps.mapping;

import com.fix42.dashboard.amps.config.ColumnType;
import com.fix42.dashboard.amps.config.FieldMapping;
import java.util.Map;

/**
 * One resolved column of a connector's target table.
 *
 * @param name Deephaven column name
 * @param type Deephaven column type
 * @param origin where the value comes from
 * @param sourceTag the source field identifier for {@link Origin#FIELD} columns, else {@code null}
 * @param valueTable code -> value rewrites applied to the raw payload value; empty for none
 * @param defaultValue the value to publish when the payload omits the field; {@code null} for
 *     none. Already coerced to {@code type}, so a bad default fails at startup rather than on
 *     the first message that lacks the field
 */
public record ColumnSpec(
        String name,
        ColumnType type,
        Origin origin,
        String sourceTag,
        Map<String, String> valueTable,
        Object defaultValue) {

    /** Where a column's value is taken from. */
    public enum Origin {
        /** A mapped field of the AMPS payload. */
        FIELD,
        /** The AMPS SOW key of the message. */
        SOW_KEY,
        /** The connector's ingest timestamp. */
        INGEST_TIMESTAMP
    }

    /**
     * A column fed by a mapped payload field.
     *
     * @param mapping the configured mapping
     * @return the resolved column
     * @throws IllegalArgumentException if {@code default-value} does not coerce to the column
     *     type -- deliberately at construction, so the problem surfaces at startup
     */
    public static ColumnSpec field(FieldMapping mapping) {
        return field(mapping, mapping.getTag());
    }

    /**
     * A column fed by a mapped payload field, read under a tag other than the configured one.
     * {@code RecordExploder} uses this to resolve a member-relative mapping from the synthetic
     * per-member tag it registers the value under.
     *
     * @param mapping the configured mapping
     * @param sourceTag the tag to read the value from
     * @return the resolved column
     * @throws IllegalArgumentException if {@code default-value} does not coerce to the column
     *     type -- deliberately at construction, so the problem surfaces at startup
     */
    public static ColumnSpec field(FieldMapping mapping, String sourceTag) {
        ColumnType type = mapping.getType();
        Object coercedDefault;
        try {
            coercedDefault = mapping.getDefaultValue() == null
                    ? null
                    : type.coerce(mapping.getDefaultValue());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("column " + mapping.getColumn()
                    + ": default-value " + e.getMessage(), e);
        }
        return new ColumnSpec(mapping.getColumn(), type, Origin.FIELD, sourceTag,
                mapping.resolveValueTable(), coercedDefault);
    }

    /** A column fed by the message's AMPS SOW key. */
    public static ColumnSpec sowKey(String name) {
        return new ColumnSpec(name, ColumnType.STRING, Origin.SOW_KEY, null, Map.of(), null);
    }

    /** A column fed by the connector's ingest timestamp. */
    public static ColumnSpec ingestTimestamp(String name) {
        return new ColumnSpec(name, ColumnType.INSTANT, Origin.INGEST_TIMESTAMP, null,
                Map.of(), null);
    }

    /**
     * Apply this column's code -> value table to one raw payload value.
     *
     * <p>A value the table does not name passes through unchanged, so an unrecognised code
     * stays visible in the column rather than becoming a null.
     *
     * @param raw the value as the payload carried it
     * @return the rewritten value, or {@code raw} when no table applies
     */
    public String rewrite(String raw) {
        if (raw == null || valueTable.isEmpty()) {
            return raw;
        }
        return valueTable.getOrDefault(raw.trim(), raw);
    }

    /** Whether the payload omitting this field publishes a configured value instead of null. */
    public boolean hasDefault() {
        return defaultValue != null;
    }
}
