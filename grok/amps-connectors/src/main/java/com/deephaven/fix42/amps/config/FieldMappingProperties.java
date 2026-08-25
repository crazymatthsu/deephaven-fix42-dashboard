package com.deephaven.fix42.amps.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class FieldMappingProperties {
    /** FIX tag number, NVFIX tag name, or JSON field name / path. */
    @NotBlank
    private String source;

    @NotBlank
    private String column;

    @NotNull
    private ColumnType type = ColumnType.STRING;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
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
}
