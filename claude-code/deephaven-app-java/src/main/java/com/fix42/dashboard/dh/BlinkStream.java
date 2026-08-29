package com.fix42.dashboard.dh;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.stream.TablePublisher;
import io.deephaven.util.QueryConstants;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * One publisher: its blink table, schema-bound column builders and batch assembly.
 *
 * <p>Port of the {@code _Stream} class in {@code dh_app.pipeline}. The column builders are resolved
 * once, at construction, so the per-cycle path is a straight loop with no type dispatch.
 *
 * <p>Coercion follows the python original exactly:
 *
 * <ul>
 *   <li>a missing key or {@code null} in a string column becomes {@code ""} (doc 01 section 6 says
 *       ids are {@code ""} when absent, never null);
 *   <li>{@code null} in a double or long column becomes Deephaven's null sentinel, so
 *       {@code fix_messages} can distinguish "tag absent" from "tag present and zero";
 *   <li>{@code null} in a boolean column stays null only for {@link Schemas#NULLABLE_BOOLEAN_COLUMNS}
 *       ({@code ChecksumOk}); every other boolean is contractually populated and defaults to false,
 *       which is what keeps {@code where("!Terminal")} null-safe.
 * </ul>
 */
final class BlinkStream {

    /** Publisher chunk size; matches the python default. */
    private static final int CHUNK_SIZE = 2048;

    private final String name;
    private final List<String> columns;
    private final List<Function<List<Map<String, Object>>, ColumnHolder<?>>> builders;
    private final TableDefinition definition;
    private final TablePublisher publisher;
    private final Table table;

    BlinkStream(String name, TableDefinition definition) {
        this.name = name;
        this.definition = definition;
        this.columns = definition.getColumnNames();
        this.builders = new ArrayList<>(columns.size());
        for (String column : columns) {
            builders.add(columnBuilder(column, Schemas.kindOf(definition, column)));
        }
        // The 6-arg form pins the UpdateGraph explicitly. The 4-arg form resolves it from
        // ExecutionContext.getContext(), which on a thread carrying the DEFAULT (poisoned) context
        // throws out of StreamToBlinkTableAdapter.initialize. Both callbacks are null: the five
        // pipeline publishers batch inside the listener, so none of them needs an onFlush hook
        // (AmpsRawSource is the one place that does). 2048 matches the python chunk-size default.
        this.publisher = TablePublisher.of(
                name,
                definition,
                /* onFlushCallback */ null,
                /* onShutdownCallback */ null,
                ExecutionContext.getContext().getUpdateGraph(),
                CHUNK_SIZE);
        // Take the strong reference ONCE, here. StreamToBlinkTableAdapter.table() reads the table
        // out of a WeakReference and nulls its own strong field on the way past, so the caller of
        // the first table() call becomes the only thing keeping the blink table alive. Re-deriving
        // it per call would leave this class holding no reference to the very tables its javadoc
        // promises to hold, and a later call could return null after a collection.
        this.table = publisher.table();
    }

    String name() {
        return name;
    }

    /** The blink table this publisher feeds; the same instance for the life of this stream. */
    Table table() {
        return table;
    }

    /** Publishes {@code rows} as one batch. A no-op when there is nothing to publish. */
    void publish(List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            return;
        }
        publisher.add(build(rows));
    }

    /** Builds a single static batch table from {@code rows} (missing keys become null). */
    Table build(List<Map<String, Object>> rows) {
        ColumnHolder<?>[] holders = new ColumnHolder<?>[builders.size()];
        for (int i = 0; i < builders.size(); i++) {
            holders[i] = builders.get(i).apply(rows);
        }
        return TableTools.newTable(definition, holders);
    }

    // ------------------------------------------------------------------ coercion

    private static Function<List<Map<String, Object>>, ColumnHolder<?>> columnBuilder(
            String column, Schemas.Kind kind) {
        return switch (kind) {
            case STRING -> rows -> {
                String[] values = new String[rows.size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = asString(rows.get(i).get(column));
                }
                return TableTools.stringCol(column, values);
            };
            case DOUBLE -> rows -> {
                double[] values = new double[rows.size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = asDouble(rows.get(i).get(column));
                }
                return TableTools.doubleCol(column, values);
            };
            case LONG -> rows -> {
                long[] values = new long[rows.size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = asLong(rows.get(i).get(column));
                }
                return TableTools.longCol(column, values);
            };
            case BOOLEAN -> {
                boolean nullable = Schemas.NULLABLE_BOOLEAN_COLUMNS.contains(column);
                yield rows -> {
                    Boolean[] values = new Boolean[rows.size()];
                    for (int i = 0; i < values.length; i++) {
                        values[i] = asBoolean(rows.get(i).get(column), nullable);
                    }
                    return TableTools.booleanCol(column, values);
                };
            }
            case INSTANT -> rows -> {
                Instant[] values = new Instant[rows.size()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = asInstant(rows.get(i).get(column));
                }
                return TableTools.instantCol(column, values);
            };
        };
    }

    /** Coerces to a non-null string ({@code ""} when absent, per doc 01 section 6). */
    static String asString(Object value) {
        if (value == null) {
            return "";
        }
        return value instanceof String s ? s : String.valueOf(value);
    }

    /** Coerces to a double, mapping {@code null}/garbage to Deephaven's null double. */
    static double asDouble(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        if (value == null) {
            return QueryConstants.NULL_DOUBLE;
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException notANumber) {
            return QueryConstants.NULL_DOUBLE;
        }
    }

    /** Coerces to a long, mapping {@code null}/garbage to Deephaven's null long. */
    static long asLong(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        if (value == null) {
            return QueryConstants.NULL_LONG;
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException notANumber) {
            return QueryConstants.NULL_LONG;
        }
    }

    /** Coerces to a Boolean; {@code null} stays null only for tri-state columns. */
    static Boolean asBoolean(Object value, boolean nullable) {
        if (value == null) {
            return nullable ? null : Boolean.FALSE;
        }
        return value instanceof Boolean b ? b : Boolean.valueOf(String.valueOf(value));
    }

    /** Passes an {@link Instant} through; anything else becomes a null cell. */
    static Instant asInstant(Object value) {
        return value instanceof Instant instant ? instant : null;
    }
}
