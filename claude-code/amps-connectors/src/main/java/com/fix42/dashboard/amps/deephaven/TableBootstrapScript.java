package com.fix42.dashboard.amps.deephaven;

import com.fix42.dashboard.amps.mapping.ColumnSpec;
import com.fix42.dashboard.amps.mapping.TableSchema;
import java.util.List;
import java.util.StringJoiner;

/**
 * Generates the server-side python that creates a connector's table if it does not exist yet.
 *
 * <p>Pure string building, so the contract with the server is unit-testable without one.
 *
 * <p>The table is a Deephaven {@code input_table}: <em>keyed</em> when the connector declares
 * key columns, which makes an add replace that key's row (SOW semantics), and
 * <em>append-only</em> otherwise, which is what a journal topic wants (doc 07 section 3).
 *
 * <p>Creation is idempotent, and an existing table whose columns disagree with the current
 * configuration is a hard error rather than a source of silently mismatched rows.
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
        String defsVar = "_amps_defs_" + table;
        String existingVar = "_amps_existing_" + table;

        StringBuilder script = new StringBuilder();
        script.append("from deephaven import input_table\n");
        script.append("from deephaven import dtypes as dht\n");
        script.append(defsVar).append(" = {\n");
        for (ColumnSpec column : schema.columns()) {
            script.append("    ").append(quote(column.name())).append(": ")
                    .append(column.type().deephavenDType()).append(",\n");
        }
        script.append("}\n");

        script.append("try:\n");
        script.append("    ").append(existingVar).append(" = ").append(table).append("\n");
        script.append("except NameError:\n");
        script.append("    ").append(existingVar).append(" = None\n");

        script.append("if ").append(existingVar).append(" is None:\n");
        script.append("    ").append(table).append(" = input_table(col_defs=").append(defsVar);
        if (schema.keyed()) {
            script.append(", key_cols=").append(pyList(schema.keyColumns()));
        }
        script.append(")\n");
        script.append("    print(").append(quote("[amps-connectors] " + connectorName
                + ": created " + (schema.keyed() ? "keyed" : "append-only")
                + " input table " + table)).append(")\n");
        script.append("else:\n");
        // Column order is part of the contract: rows are published positionally.
        script.append("    _amps_have = list(").append(existingVar).append(".definition.keys())\n");
        script.append("    _amps_want = list(").append(defsVar).append(".keys())\n");
        script.append("    if _amps_have != _amps_want:\n");
        script.append("        raise RuntimeError(").append(quote("[amps-connectors] "
                + connectorName + ": existing table " + table
                + " has columns %s but the connector is configured for %s"))
                .append(" % (_amps_have, _amps_want))\n");
        return script.toString();
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

    private static String pyList(List<String> values) {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        values.forEach(value -> joiner.add(quote(value)));
        return joiner.toString();
    }

    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
