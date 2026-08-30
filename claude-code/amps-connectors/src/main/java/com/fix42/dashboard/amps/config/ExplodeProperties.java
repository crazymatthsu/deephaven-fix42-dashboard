package com.fix42.dashboard.amps.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.util.ArrayList;
import java.util.List;

/**
 * One record exploded into one row per member of an object-valued field (doc 07 section 5.4).
 *
 * <p>This is the tabular rendering of a JSON field that is itself a map with <em>dynamic</em>
 * keys -- {@code {"AAPL": {"qty": 250}, "MSFT": {"qty": 100}}} -- where a static column list
 * cannot name the members. Each member becomes a Deephaven row: the connector's ordinary
 * {@code fields} are mapped from the record as usual and repeat on every row, the member's
 * name lands in {@link #getKeyColumn()}, and {@link #getFields()} are mapped from inside the
 * member's value. The member value itself is addressable as the tag {@code "."}.
 *
 * <p>On a keyed target the exploder also remembers which members each record last published,
 * so a member that disappears from a republished record -- or the whole record leaving the
 * SOW -- deletes its rows instead of leaving them stale.
 */
public class ExplodeProperties {

    /**
     * The source field whose members become rows. Interpreted like any field tag: a JSON
     * path such as {@code value}, or a part-indexed path such as {@code 0.value} for a
     * composite format. Must resolve to a JSON object.
     */
    @NotBlank
    private String tag;

    /** The Deephaven column the member's name is published as (always a string). */
    @NotBlank
    private String keyColumn;

    /**
     * Mappings resolved <em>inside</em> each member's value: {@code qty} is the member's
     * {@code qty} field, {@code "."} is the member value itself (as text for a scalar, as
     * JSON text for an object). May be empty, which publishes membership alone.
     */
    @Valid
    private List<FieldMapping> fields = new ArrayList<>();

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(String keyColumn) {
        this.keyColumn = keyColumn;
    }

    public List<FieldMapping> getFields() {
        return fields;
    }

    public void setFields(List<FieldMapping> fields) {
        this.fields = fields == null ? new ArrayList<>() : fields;
    }
}
