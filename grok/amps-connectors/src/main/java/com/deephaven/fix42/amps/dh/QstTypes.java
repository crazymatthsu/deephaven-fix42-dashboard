package com.deephaven.fix42.amps.dh;

import com.deephaven.fix42.amps.config.ColumnType;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.type.Type;
import com.deephaven.fix42.amps.config.FieldMappingProperties;
import com.deephaven.fix42.amps.map.MappedRow;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class QstTypes {
    private QstTypes() {}

    public static Type<?> type(ColumnType columnType) {
        return switch (columnType) {
            case STRING -> Type.stringType();
            case BYTE -> Type.byteType();
            case SHORT -> Type.shortType();
            case INT -> Type.intType();
            case LONG -> Type.longType();
            case FLOAT -> Type.floatType();
            case DOUBLE -> Type.doubleType();
            case BOOLEAN -> Type.booleanType();
            case CHAR -> Type.charType();
            case INSTANT -> Type.instantType();
        };
    }

    public static TableHeader tableHeader(List<FieldMappingProperties> fields) {
        TableHeader.Builder builder = TableHeader.builder();
        for (FieldMappingProperties field : fields) {
            builder.putHeaders(field.getColumn(), type(field.getType()));
        }
        return builder.build();
    }

    public static NewTable toNewTable(List<FieldMappingProperties> fields, MappedRow row) {
        List<Column<?>> columns = new ArrayList<>(fields.size());
        for (FieldMappingProperties field : fields) {
            columns.add(column(field, row.get(field.getColumn())));
        }
        return NewTable.of(columns);
    }

    public static NewTable keyTable(List<FieldMappingProperties> fields, MappedRow keyRow) {
        List<Column<?>> columns = new ArrayList<>();
        for (FieldMappingProperties field : fields) {
            if (keyRow.isPresent(field.getColumn())) {
                columns.add(column(field, keyRow.get(field.getColumn())));
            }
        }
        return NewTable.of(columns);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Column<?> column(FieldMappingProperties field, Object value) {
        String name = field.getColumn();
        return switch (field.getType()) {
            case STRING -> Column.of(name, (String) value);
            case BYTE -> Column.of(name, (Byte) value);
            case SHORT -> Column.of(name, (Short) value);
            case INT -> Column.of(name, (Integer) value);
            case LONG -> Column.of(name, (Long) value);
            case FLOAT -> Column.of(name, (Float) value);
            case DOUBLE -> Column.of(name, (Double) value);
            case BOOLEAN -> Column.of(name, (Boolean) value);
            case CHAR -> Column.of(name, (Character) value);
            case INSTANT -> Column.of(name, (Instant) value);
        };
    }
}
