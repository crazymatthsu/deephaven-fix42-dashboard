package com.fix42.dashboard.amps.mapping;

import com.fix42.dashboard.amps.source.AmpsRecord;
import java.time.Instant;
import java.util.Map;

/**
 * Turns a decoded AMPS payload into a {@link MappedRow} for one {@link TableSchema}.
 *
 * <p>Strictly an allowlist: only columns the schema declares are produced, so a payload field
 * with no mapping is dropped rather than published (doc 07 section 5). A mapped field that the
 * payload does not carry comes out {@code null} and not-present.
 */
public final class FieldMapper {

    private final TableSchema schema;

    public FieldMapper(TableSchema schema) {
        this.schema = schema;
    }

    /** The schema this mapper produces rows for. */
    public TableSchema schema() {
        return schema;
    }

    /**
     * Map one record.
     *
     * @param record the AMPS record, already decoded into tag -> raw value
     * @param fields the decoded payload; ignored for a {@link MappedRow.Action#DELETE}
     * @param ingestTime the timestamp for an ingest-timestamp column
     * @return the mapped row
     * @throws IllegalArgumentException if a field value does not coerce to its column type
     */
    public MappedRow map(AmpsRecord record, Map<String, String> fields, Instant ingestTime) {
        int size = schema.size();
        Object[] values = new Object[size];
        boolean[] present = new boolean[size];

        for (int i = 0; i < size; i++) {
            ColumnSpec column = schema.columns().get(i);
            switch (column.origin()) {
                case FIELD -> {
                    String raw = fields.get(column.sourceTag());
                    if (raw != null) {
                        present[i] = true;
                        try {
                            values[i] = column.type().coerce(raw);
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                    "column " + column.name() + " (tag " + column.sourceTag()
                                            + "): " + e.getMessage(), e);
                        }
                    }
                }
                case SOW_KEY -> {
                    values[i] = record.sowKey();
                    present[i] = record.sowKey() != null;
                }
                case INGEST_TIMESTAMP -> {
                    values[i] = ingestTime;
                    present[i] = true;
                }
            }
        }

        MappedRow.Action action = record.action() == AmpsRecord.Action.DELETE
                ? MappedRow.Action.DELETE
                : MappedRow.Action.UPSERT;
        return new MappedRow(values, present, action, schema.rowKey(values));
    }
}
