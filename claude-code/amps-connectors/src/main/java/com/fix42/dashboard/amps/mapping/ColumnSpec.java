package com.fix42.dashboard.amps.mapping;

import com.fix42.dashboard.amps.config.ColumnType;

/**
 * One resolved column of a connector's target table.
 *
 * @param name Deephaven column name
 * @param type Deephaven column type
 * @param origin where the value comes from
 * @param sourceTag the source field identifier for {@link Origin#FIELD} columns, else {@code null}
 */
public record ColumnSpec(String name, ColumnType type, Origin origin, String sourceTag) {

    /** Where a column's value is taken from. */
    public enum Origin {
        /** A mapped field of the AMPS payload. */
        FIELD,
        /** The AMPS SOW key of the message. */
        SOW_KEY,
        /** The connector's ingest timestamp. */
        INGEST_TIMESTAMP
    }

    /** A column fed by a mapped payload field. */
    public static ColumnSpec field(String name, ColumnType type, String sourceTag) {
        return new ColumnSpec(name, type, Origin.FIELD, sourceTag);
    }

    /** A column fed by the message's AMPS SOW key. */
    public static ColumnSpec sowKey(String name) {
        return new ColumnSpec(name, ColumnType.STRING, Origin.SOW_KEY, null);
    }

    /** A column fed by the connector's ingest timestamp. */
    public static ColumnSpec ingestTimestamp(String name) {
        return new ColumnSpec(name, ColumnType.INSTANT, Origin.INGEST_TIMESTAMP, null);
    }
}
