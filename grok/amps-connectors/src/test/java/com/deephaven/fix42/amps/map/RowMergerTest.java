package com.deephaven.fix42.amps.map;

import com.deephaven.fix42.amps.config.ColumnType;
import com.deephaven.fix42.amps.config.FieldMappingProperties;
import com.deephaven.fix42.amps.config.UpdateMode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RowMergerTest {
    private static FieldMappingProperties field(String column, ColumnType type) {
        FieldMappingProperties f = new FieldMappingProperties();
        f.setSource(column);
        f.setColumn(column);
        f.setType(type);
        return f;
    }

    private final List<FieldMappingProperties> fields = List.of(
            field("OrderId", ColumnType.STRING),
            field("Symbol", ColumnType.STRING),
            field("Price", ColumnType.DOUBLE));

    @Test
    void fullReplaceNullsMissing() {
        RowMerger merger = new RowMerger(fields, UpdateMode.FULL);
        MappedRow previous = new MappedRow(
                Map.of("OrderId", "O9", "Symbol", "IBM", "Price", 10.5), Set.of("OrderId", "Symbol", "Price"));
        MappedRow incoming = new MappedRow(Map.of("OrderId", "O9", "Price", 11.0), Set.of("OrderId", "Price"));
        MappedRow merged = merger.merge(previous, incoming, false);
        assertEquals("O9", merged.get("OrderId"));
        assertNull(merged.get("Symbol"));
        assertEquals(11.0, merged.get("Price"));
    }

    @Test
    void deltaKeepsPrevious() {
        RowMerger merger = new RowMerger(fields, UpdateMode.DELTA);
        MappedRow previous = new MappedRow(
                Map.of("OrderId", "O9", "Symbol", "IBM", "Price", 10.5), Set.of("OrderId", "Symbol", "Price"));
        MappedRow incoming = new MappedRow(Map.of("OrderId", "O9", "Price", 11.0), Set.of("OrderId", "Price"));
        MappedRow merged = merger.merge(previous, incoming, false);
        assertEquals("IBM", merged.get("Symbol"));
        assertEquals(11.0, merged.get("Price"));
    }

    @Test
    void sowSnapshotIsFullEvenInDeltaMode() {
        RowMerger merger = new RowMerger(fields, UpdateMode.DELTA);
        MappedRow previous = new MappedRow(
                Map.of("OrderId", "O9", "Symbol", "IBM", "Price", 10.5), Set.of("OrderId", "Symbol", "Price"));
        MappedRow incoming = new MappedRow(Map.of("OrderId", "O9", "Price", 11.0), Set.of("OrderId", "Price"));
        MappedRow merged = merger.merge(previous, incoming, true);
        assertNull(merged.get("Symbol"));
        assertEquals(11.0, merged.get("Price"));
    }
}
