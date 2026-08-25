package com.fix42.dashboard.amps.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * One source-field -> Deephaven-column mapping.
 *
 * <p>The mapping list is an <strong>allowlist</strong>: a field present in the AMPS payload
 * but absent from the list is never published (doc 07 section 5). {@link #getTag()} is
 * interpreted per {@link SourceFormat} -- a FIX tag number, an NVFIX field name, or a JSON
 * field name (optionally a dotted path such as {@code order.price}).
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

    @Override
    public String toString() {
        return tag + " -> " + column + " (" + type + ")";
    }
}
