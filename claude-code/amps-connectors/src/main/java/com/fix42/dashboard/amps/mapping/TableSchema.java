package com.fix42.dashboard.amps.mapping;

import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.DeephavenTableProperties;
import com.fix42.dashboard.amps.config.FieldMapping;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A connector's target table resolved from configuration: ordered columns plus key columns.
 *
 * <p>Single source of truth for column <em>order</em>. The python that creates the table, the
 * {@code Object[]} a mapped row is built into, and the Arrow batch handed to Deephaven all
 * index by the same positions, so they cannot drift.
 */
public final class TableSchema {

    /** ASCII unit separator, joining composite key values. Never appears in a field value. */
    private static final char KEY_SEPARATOR = (char) 0x1F;

    private final String tableName;
    private final List<ColumnSpec> columns;
    private final List<String> keyColumns;
    private final Map<String, Integer> indexByName;
    private final int[] keyIndexes;

    private TableSchema(String tableName, List<ColumnSpec> columns, List<String> keyColumns) {
        this.tableName = tableName;
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
        return new TableSchema(target.getTable(), columns, target.getKeyColumns());
    }

    private static boolean isSet(String value) {
        return value != null && !value.isBlank();
    }

    /** Global name of the Deephaven table. */
    public String tableName() {
        return tableName;
    }

    /** The columns, in publish order. */
    public List<ColumnSpec> columns() {
        return columns;
    }

    /** The key columns; empty for an append-only table. */
    public List<String> keyColumns() {
        return keyColumns;
    }

    /** Whether the target is a keyed input table. */
    public boolean keyed() {
        return !keyColumns.isEmpty();
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
     * @param values a row's values, indexed by this schema's column order
     * @return the composite key, or {@code null} when the table is not keyed
     */
    public String rowKey(Object[] values) {
        if (keyIndexes.length == 0) {
            return null;
        }
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < keyIndexes.length; i++) {
            if (i > 0) {
                key.append(KEY_SEPARATOR);
            }
            key.append(values[keyIndexes[i]]);
        }
        return key.toString();
    }
}
