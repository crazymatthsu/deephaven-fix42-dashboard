package com.fix42.dashboard.amps.deephaven;

import com.fix42.dashboard.amps.mapping.TableSchema;
import java.util.List;

/**
 * The connector application's whole view of the Deephaven server.
 *
 * <p>An interface because the connector runtime is worth testing without a server: the tests
 * substitute a recording implementation and assert on the rows that would have been published.
 */
public interface DeephavenGateway extends AutoCloseable {

    /**
     * Establish or re-validate the connection and report which incarnation of the server we are
     * talking to.
     *
     * <p>The returned number changes whenever the connection had to be rebuilt or the server's
     * python scope lost the connectors' tables -- i.e. whenever Deephaven restarted. Callers use
     * a change as the signal to re-create tables and resubscribe from AMPS.
     *
     * @return the current generation, or {@code 0} when the server is unreachable
     */
    long refresh();

    /** The current generation, without probing. {@code 0} when unavailable. */
    long generation();

    /** Whether the last {@link #refresh()} found a usable server. */
    boolean isAvailable();

    /**
     * Create the connector's table on the server if it does not exist yet.
     *
     * @param schema the resolved target schema
     * @param connectorName the connector name, for server-side log and error text
     * @throws DeephavenUnavailableException if the script could not be run or it failed
     */
    void ensureTable(TableSchema schema, String connectorName);

    /**
     * Add rows to a connector's table. For a keyed table this replaces the row of each key.
     *
     * @param schema the target schema; row values are indexed by its column order
     * @param rows the rows to add
     * @throws DeephavenUnavailableException if the rows could not be published
     */
    void addRows(TableSchema schema, List<Object[]> rows);

    /**
     * Delete rows from a keyed table.
     *
     * @param schema the target schema; only its key columns are sent
     * @param rows the rows whose keys should be removed
     * @throws DeephavenUnavailableException if the deletion could not be published
     */
    void deleteRows(TableSchema schema, List<Object[]> rows);

    @Override
    void close();
}
