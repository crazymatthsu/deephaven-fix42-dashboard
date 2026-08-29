package com.fix42.dashboard.amps.deephaven;

import com.fix42.dashboard.amps.config.DeephavenTableType;
import com.fix42.dashboard.amps.mapping.ColumnSpec;
import com.fix42.dashboard.amps.mapping.TableSchema;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

/**
 * Generates the server-side python that creates a connector's table if it does not exist yet.
 *
 * <p>Pure string building, so the contract with the server is unit-testable without one.
 *
 * <p>What gets created is the connector's {@code deephaven.table-type} (doc 07 section 3):
 *
 * <ul>
 *   <li>{@code KEYED} / {@code APPEND_ONLY} -- an {@code input_table}, which the connector then
 *       writes to directly over Arrow Flight;
 *   <li>{@code BLINK} / {@code RING} -- a {@code table_publisher()}, whose {@code TablePublisher}
 *       is kept in a module-private global because it is the only way to put rows into a blink
 *       table. {@code RING} wraps the blink table in a {@code ring_table} of {@code ring-capacity}
 *       rows and publishes <em>that</em> under the configured name, holding the blink table in a
 *       global of its own so it stays reachable.
 * </ul>
 *
 * <p>Creation is idempotent. An existing table that disagrees with the current configuration --
 * on columns, on column types, or on keys -- is a hard error rather than a source of silently
 * mismatched rows.
 */
public final class TableBootstrapScript {

    private TableBootstrapScript() {
    }

    /**
     * Build the create-if-missing script for one table.
     *
     * @param schema the resolved target schema
     * @param connectorName the connector name, used in the server-side log line and error text
     * @return python to run in the server's console session
     */
    public static String createIfMissing(TableSchema schema, String connectorName) {
        String table = schema.tableName();
        DeephavenTableType type = schema.tableType();
        String defs = defsVariable(table);
        String existing = "_amps_existing_" + table;
        String id = "[amps-connectors] " + connectorName + ": ";

        StringBuilder script = new StringBuilder();
        script.append("from deephaven import dtypes as dht\n");
        if (type.publisherBacked()) {
            script.append("from deephaven.stream.table_publisher import table_publisher\n");
            if (type.bounded()) {
                script.append("from deephaven import ring_table\n");
            }
        } else {
            script.append("from deephaven import input_table\n");
        }

        script.append(defs).append(" = {\n");
        for (ColumnSpec column : schema.columns()) {
            script.append("    ").append(quote(column.name())).append(": ")
                    .append(column.type().deephavenDType()).append(",\n");
        }
        script.append("}\n");

        script.append("try:\n");
        script.append("    ").append(existing).append(" = ").append(table).append("\n");
        script.append("except NameError:\n");
        script.append("    ").append(existing).append(" = None\n");

        script.append("if ").append(existing).append(" is None:\n");
        if (type.publisherBacked()) {
            appendPublisherCreation(script, schema, id);
        } else {
            appendInputTableCreation(script, schema, id);
        }
        if (type.publisherBacked()) {
            // A blink table can only be written through the publisher that made it, so adopting
            // one we do not hold the publisher for is not something we can do -- say so plainly
            // instead of failing later on every batch.
            script.append("elif ").append(publisherVariable(table))
                    .append(" not in globals():\n");
            script.append("    raise RuntimeError(").append(quote(id + table
                    + " already exists but was not created by this connector; a "
                    + type.name().toLowerCase(Locale.ROOT)
                    + " table can only be published into through the TablePublisher that made"
                    + " it, so remove " + table + " and let the connector re-create it"))
                    .append(")\n");
        }
        script.append("else:\n");
        appendVerification(script, schema, id);
        return script.toString();
    }

    private static void appendInputTableCreation(
            StringBuilder script, TableSchema schema, String id) {
        String table = schema.tableName();
        script.append("    ").append(table).append(" = input_table(col_defs=")
                .append(defsVariable(table));
        if (schema.keyed()) {
            script.append(", key_cols=").append(pyList(schema.keyColumns()));
        }
        script.append(")\n");
        script.append("    print(").append(quote(id + "created "
                + (schema.keyed() ? "keyed" : "append-only") + " input table " + table))
                .append(")\n");
    }

    private static void appendPublisherCreation(
            StringBuilder script, TableSchema schema, String id) {
        String table = schema.tableName();
        String blink = blinkVariable(table);
        // The blink table is bound to a global even when it is not the published table: the
        // publisher holds it only weakly, so an unreferenced blink table is collected and every
        // subsequent add silently goes nowhere.
        script.append("    ").append(blink).append(", ").append(publisherName(table))
                .append(" = table_publisher(").append(quote("amps-connectors:" + table))
                .append(", ").append(defsVariable(table)).append(")\n");
        if (schema.tableType().bounded()) {
            script.append("    ").append(table).append(" = ring_table(").append(blink)
                    .append(", ").append(schema.ringCapacity()).append(")\n");
            script.append("    print(").append(quote(id + "created ring table " + table
                            + " (capacity " + schema.ringCapacity() + ") over a blink table"))
                    .append(")\n");
        } else {
            script.append("    ").append(table).append(" = ").append(blink).append("\n");
            script.append("    print(").append(quote(id + "created blink table " + table))
                    .append(")\n");
        }
    }

    /**
     * The adopt-an-existing-table check.
     *
     * <p>Column order is part of the contract -- rows are published positionally -- and so are
     * the column types and the keys: a keyed table adopted for an append-only connector would
     * quietly collapse rows onto their keys, and a mistyped column fails much later, inside
     * {@code addToInputTable}, with nothing to point at the configuration.
     */
    private static void appendVerification(StringBuilder script, TableSchema schema, String id) {
        String table = schema.tableName();
        String defs = defsVariable(table);
        String existing = "_amps_existing_" + table;
        String have = "_amps_have_" + table;
        String want = "_amps_want_" + table;
        String bad = "_amps_bad_" + table;
        String keys = "_amps_keys_" + table;

        script.append("    ").append(have).append(" = list(").append(existing)
                .append(".definition.keys())\n");
        script.append("    ").append(want).append(" = list(").append(defs).append(".keys())\n");
        script.append("    if ").append(have).append(" != ").append(want).append(":\n");
        script.append("        raise RuntimeError(").append(quote(id + "existing table " + table
                        + " has columns %s but the connector is configured for %s"))
                .append(" % (").append(have).append(", ").append(want).append("))\n");
        // j_name rather than the DType object: two DType instances can wrap the same java class
        // (int32 / int_), and the name is what actually decides whether the column matches.
        script.append("    ").append(bad).append(" = [n for n in ").append(want)
                .append(" if ").append(existing).append(".definition[n].data_type.j_name != ")
                .append(defs).append("[n].j_name]\n");
        script.append("    if ").append(bad).append(":\n");
        script.append("        raise RuntimeError(").append(quote(id + "existing table " + table
                        + " disagrees with the connector on the type of column(s) %s"))
                .append(" % (").append(bad).append(",))\n");
        // Only an input table answers key_names at all; anything else reads as unkeyed, which is
        // right -- we would not be able to upsert into it.
        script.append("    ").append(keys).append(" = list(getattr(").append(existing)
                .append(", \"key_names\", []))\n");
        script.append("    if ").append(keys).append(" != ").append(pyList(schema.keyColumns()))
                .append(":\n");
        script.append("        raise RuntimeError(").append(quote(id + "existing table " + table
                        + " is keyed on %s but the connector is configured for %s"))
                .append(" % (").append(keys).append(", ").append(pyList(schema.keyColumns()))
                .append("))\n");
    }

    /**
     * Build a script that fails unless every named global is present.
     *
     * <p>This is how a Deephaven restart is detected: the server answers gRPC again, but its
     * python scope is empty, so the probe raises and the connectors are restarted to rehydrate
     * (doc 07 section 6).
     *
     * @param tableNames the globals that must exist
     * @return python that raises when any of them is missing
     */
    public static String probe(List<String> tableNames) {
        StringBuilder script = new StringBuilder();
        script.append("_amps_missing = [n for n in ").append(pyList(tableNames))
                .append(" if n not in globals()]\n");
        script.append("if _amps_missing:\n");
        script.append("    raise RuntimeError(")
                .append(quote("[amps-connectors] tables missing from the python scope: "))
                .append(" + repr(_amps_missing))\n");
        return script.toString();
    }

    /**
     * The scratch global one Arrow batch is bound to on its way into a {@code TablePublisher}.
     *
     * <p>Unique per batch: two flushes of the same connector can overlap -- the scheduled flush
     * and a full-buffer flush by a submitting thread -- and a shared name would let one batch
     * overwrite the other's rows before either was published.
     *
     * @param tableName the target table
     * @param sequence a number unique among in-flight batches for this table
     * @return the global's name
     */
    public static String batchVariable(String tableName, long sequence) {
        return "_amps_batch_" + tableName + "_" + sequence;
    }

    /**
     * Build the python that moves one uploaded batch into the table's publisher.
     *
     * @param tableName the target table
     * @param batchVariable the global holding the batch, from
     *     {@link #batchVariable(String, long)}
     * @return python to run in the server's console session
     */
    public static String publishBatch(String tableName, String batchVariable) {
        return "try:\n"
                + "    " + publisherName(tableName) + ".add(" + batchVariable + ")\n"
                + "finally:\n"
                + "    del " + batchVariable + "\n";
    }

    /** The global holding a table's {@code TablePublisher}. */
    private static String publisherName(String tableName) {
        return "_amps_pub_" + tableName;
    }

    /** The same name, quoted for a {@code globals()} membership test. */
    private static String publisherVariable(String tableName) {
        return quote(publisherName(tableName));
    }

    /** The global holding the blink table a ring table is built over. */
    private static String blinkVariable(String tableName) {
        return "_amps_blink_" + tableName;
    }

    /** The global holding a table's column definitions. */
    private static String defsVariable(String tableName) {
        return "_amps_defs_" + tableName;
    }

    private static String pyList(List<String> values) {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        values.forEach(value -> joiner.add(quote(value)));
        return joiner.toString();
    }

    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
