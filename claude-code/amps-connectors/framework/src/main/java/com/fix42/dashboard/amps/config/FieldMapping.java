package com.fix42.dashboard.amps.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One source-field -> Deephaven-column mapping.
 *
 * <p>The mapping list is an <strong>allowlist</strong>: a field present in the AMPS payload
 * but absent from the list is never published (doc 07 section 5). {@link #getTag()} is
 * interpreted per {@link SourceFormat} -- a FIX tag number, an NVFIX field name, or a JSON
 * field name (optionally a dotted path such as {@code order.price}).
 *
 * <p>Two optional knobs shape the value on its way to the column (doc 07 section 5.2):
 * {@link #getDecode()} / {@link #getValues()} rewrite the wire code to a readable name, and
 * {@link #getDefaultValue()} supplies a value for a field the payload does not carry at all.
 */
public class FieldMapping {

    /** The source field identifier: FIX tag number, NVFIX name, or JSON name/path. */
    @NotBlank
    private String tag;

    /** The Deephaven column this field is published as. */
    @NotBlank
    private String column;

    /** The Deephaven column type. */
    @NotNull
    private ColumnType type = ColumnType.STRING;

    /**
     * A built-in FIX 4.2 code -> name table to publish instead of the raw code, so
     * {@code 54=1} lands in the column as {@code BUY}. A code the table does not name passes
     * through unchanged.
     */
    private FixValueDecode decode;

    /**
     * Inline code -> value rewrites, for a feed the built-in tables do not cover or a venue
     * that deviates from them. Applied <em>over</em> {@link #getDecode()} when both are set,
     * so a named table can be extended or corrected one code at a time.
     */
    private Map<String, String> values = new LinkedHashMap<>();

    /**
     * The value to publish when the payload does not carry this field at all.
     *
     * <p>Written as the finished value, not as a wire code: it is coerced to {@link #getType()}
     * but never passed through {@link #getDecode()}. A field the payload carries <em>empty</em>
     * is not defaulted -- that is an explicit clear, which delta publishing has to preserve.
     */
    private String defaultValue;

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public FixValueDecode getDecode() {
        return decode;
    }

    public void setDecode(FixValueDecode decode) {
        this.decode = decode;
    }

    public Map<String, String> getValues() {
        return values;
    }

    public void setValues(Map<String, String> values) {
        this.values = values == null ? new LinkedHashMap<>() : values;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    /**
     * The code -> value table this mapping publishes through: the named table, with the inline
     * entries applied over it.
     *
     * @return the merged table, empty when neither is configured
     */
    public Map<String, String> resolveValueTable() {
        if (decode == null && values.isEmpty()) {
            return Map.of();
        }
        Map<String, String> merged = new LinkedHashMap<>();
        if (decode != null) {
            merged.putAll(decode.table());
        }
        merged.putAll(values);
        // Insertion order is kept: the named table first, inline entries over it.
        return Collections.unmodifiableMap(merged);
    }

    @Override
    public String toString() {
        return tag + " -> " + column + " (" + type + ")"
                + (decode == null ? "" : " decode=" + decode)
                + (values.isEmpty() ? "" : " values=" + values.size())
                + (defaultValue == null ? "" : " default=" + defaultValue);
    }
}
