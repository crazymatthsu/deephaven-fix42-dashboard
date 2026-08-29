package com.fix42.dashboard.amps.mapping;

import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.DeephavenTableProperties;
import com.fix42.dashboard.amps.config.DeephavenTableType;
import com.fix42.dashboard.amps.config.FieldMapping;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A connector's target table resolved from configuration: its kind, ordered columns and keys.
 *
 * <p>Single source of truth for column <em>order</em>. The python that creates the table, the
 * {@code Object[]} a mapped row is built into, and the Arrow batch handed to Deephaven all
 * index by the same positions, so they cannot drift.
 */
public final class TableSchema {

    /** ASCII unit separator, joining composite key values. Never appears in a field value. */
    private static final char KEY_SEPARATOR = (char) 0x1F;

    private final String tableName;
    private final DeephavenTableType tableType;
    private final int ringCapacity;
    private final List<ColumnSpec> columns;
    private final List<String> keyColumns;
    private final Map<String, Integer> indexByName;
    private final int[] keyIndexes;

    private TableSchema(
            String tableName,
            DeephavenTableType tableType,
            int ringCapacity,
            List<ColumnSpec> columns,
            List<String> keyColumns) {
        this.tableName = tableName;
        this.tableType = tableType;
        this.ringCapacity = ringCapacity;
        this.columns = List.copyOf(columns);
        this.keyColumns = List.copyOf(keyColumns);
        Map<String, Integer> index = new HashMap<>();
        for (int i = 0; i < this.columns.size(); i++) {
            index.put(this.columns.get(i).name(), i);
        }
        this.indexByName = Map.copyOf(index);
        this.keyIndexes = this.keyColumns.stream().mapToInt(this::indexOf).toArray();
    }

    /**
     * Resolve the schema of a connector's target table.
     *
     * <p>Column order is: every mapped field in configuration order, then the SOW-key column,
     * then the ingest-timestamp column, when those are configured.
     *
     * <p>Key columns are kept only for a {@code KEYED} target, so a schema is internally
     * consistent even for a configuration {@link com.fix42.dashboard.amps.config.ConnectorValidator}
     * would reject -- there is no such thing here as a blink table with keys.
     *
     * @param connector the connector configuration
     * @return the resolved schema
     */
    public static TableSchema of(ConnectorProperties connector) {
        DeephavenTableProperties target = connector.getDeephaven();
        List<ColumnSpec> columns = new ArrayList<>();
        for (FieldMapping field : connector.getFields()) {
            columns.add(ColumnSpec.field(field.getColumn(), field.getType(), field.getTag()));
        }
        if (isSet(target.getSowKeyColumn())) {
            columns.add(ColumnSpec.sowKey(target.getSowKeyColumn().trim()));
        }
        if (isSet(target.getIngestTimestampColumn())) {
            columns.add(ColumnSpec.ingestTimestamp(target.getIngestTimestampColumn().trim()));
        }
        DeephavenTableType type = target.resolveTableType(connector.getSource().isSow());
        return new TableSchema(
                target.getTable(),
                type,
                target.getRingCapacity(),
                columns,
                type.keyed() ? target.getKeyColumns() : List.of());
    }

    private static boolean isSet(String value) {
        return value != null && !value.isBlank();
    }

    /** Global name of the Deephaven table. */
    public String tableName() {
        return tableName;
    }

    /** The kind of table to create and publish into. */
    public DeephavenTableType tableType() {
        return tableType;
    }

    /** Rows a {@code RING} target retains; meaningless for every other type. */
    public int ringCapacity() {
        return ringCapacity;
    }

    /** The columns, in publish order. */
    public List<ColumnSpec> columns() {
        return columns;
    }

    /** The key columns; empty for every type but {@code KEYED}. */
    public List<String> keyColumns() {
        return keyColumns;
    }

    /** Whether the target is a keyed input table -- the only type that supports removal. */
    public boolean keyed() {
        return tableType.keyed();
    }

    /** Number of columns. */
    public int size() {
        return columns.size();
    }

    /**
     * Position of a column in the publish order.
     *
     * @param column the column name
     * @return its index
     * @throws IllegalArgumentException if the schema has no such column
     */
    public int indexOf(String column) {
        Integer index = indexByName.get(column);
        if (index == null) {
            throw new IllegalArgumentException(
                    "table " + tableName + " has no column '" + column + "'");
        }
        return index;
    }

    /**
     * Build the row-key of a mapped row: its key-column values joined by the unit separator.
     *
     * <p>A row missing any key value has no key at all, so this returns {@code null} rather than
     * rendering the gap as text. Without that, every keyless record would join on the same
     * literal and collapse onto a single row of the target table -- the caller drops them
     * instead. This is reachable in practice: a SOW key column is empty whenever AMPS delivers a
     * message without one.
     *
     * @param values a row's values, indexed by this schema's column order
     * @return the composite key; {@code null} when the table is not keyed, or when any key
     *     column of this row is null
     */
    public String rowKey(Object[] values) {
        if (keyIndexes.length == 0) {
            return null;
        }
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < keyIndexes.length; i++) {
            Object value = values[keyIndexes[i]];
            if (value == null) {
                return null;
            }
            if (i > 0) {
                key.append(KEY_SEPARATOR);
            }
            key.append(value);
        }
        return key.toString();
    }
}
