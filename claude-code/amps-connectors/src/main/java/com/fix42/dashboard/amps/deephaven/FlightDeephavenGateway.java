package com.fix42.dashboard.amps.deephaven;

import com.fix42.dashboard.amps.config.AmpsConnectorsProperties;
import com.fix42.dashboard.amps.config.DeephavenServerProperties;
import com.fix42.dashboard.amps.mapping.ColumnSpec;
import com.fix42.dashboard.amps.mapping.TableSchema;
import io.deephaven.client.impl.ClientConfig;
import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.ExportId;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.SessionConfig;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.NewTable;
import io.deephaven.uri.DeephavenTarget;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * {@link DeephavenGateway} over the Deephaven java client.
 *
 * <p>Two server APIs are in play, and the split is deliberate:
 *
 * <ul>
 *   <li>a <strong>console session</strong> runs python to create the tables, because
 *       {@code input_table} and {@code table_publisher} are server-side constructors with no
 *       gRPC equivalent;
 *   <li><strong>Arrow Flight</strong> carries the rows, because it is the native bulk path --
 *       rows go over as an Arrow batch rather than as generated python.
 * </ul>
 *
 * <p>How the batch lands depends on the table type. An input table takes it directly
 * ({@code addToInputTable}). A blink table cannot: it is not an input table, and the only way
 * into one is the {@code TablePublisher} that created it. So for {@code BLINK} and {@code RING}
 * the batch is uploaded as an export, bound into the script scope under a scratch name, and
 * moved into the publisher by one line of python -- three round trips per <em>batch</em>, and
 * the rows still travel as Arrow.
 */
@Component
public class FlightDeephavenGateway implements DeephavenGateway {

    private static final Logger log = LoggerFactory.getLogger(FlightDeephavenGateway.class);

    private final DeephavenServerProperties properties;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicLong generation = new AtomicLong();
    private final AtomicLong batchSequence = new AtomicLong();
    private final List<String> bootstrappedTables = new ArrayList<>();

    private BufferAllocator allocator;
    private ScheduledExecutorService scheduler;
    private volatile FlightSession flight;
    private volatile ConsoleSession console;

    public FlightDeephavenGateway(AmpsConnectorsProperties properties) {
        this.properties = properties.getDeephaven();
    }

    @Override
    public long refresh() {
        lock.lock();
        try {
            if (flight == null) {
                return connect() ? generation.get() : 0L;
            }
            if (probe()) {
                return generation.get();
            }
            // Either the connection died or the server came back with an empty python scope.
            // Both mean the same thing to callers: this is a different Deephaven than before.
            disconnectLocked();
            return connect() ? generation.get() : 0L;
        } finally {
            lock.unlock();
        }
    }

    /** @return {@code true} when the server answered and still holds every bootstrapped table */
    private boolean probe() {
        if (bootstrappedTables.isEmpty()) {
            // Nothing to check for yet; a liveness probe is enough.
            return execute("pass", "liveness probe") == null;
        }
        String failure = execute(TableBootstrapScript.probe(bootstrappedTables), "table probe");
        if (failure != null) {
            log.warn("Deephaven probe failed, treating it as a restart: {}", failure);
            return false;
        }
        return true;
    }

    private boolean connect() {
        try {
            if (allocator == null) {
                allocator = new RootAllocator();
            }
            if (scheduler == null) {
                scheduler = Executors.newScheduledThreadPool(2, runnable -> {
                    Thread thread = new Thread(runnable, "dh-client");
                    thread.setDaemon(true);
                    return thread;
                });
            }
            Duration timeout = properties.getTimeout();
            ClientConfig clientConfig = ClientConfig.builder()
                    .target(DeephavenTarget.builder()
                            .host(properties.getHost())
                            .port(properties.getPort())
                            .isSecure(properties.isTls())
                            .build())
                    .build();
            SessionConfig sessionConfig = SessionConfig.builder()
                    .authenticationTypeAndValue(properties.getAuthentication())
                    .executeTimeout(timeout)
                    .closeTimeout(timeout)
                    .build();
            FlightSession session = FlightSessionFactoryConfig.builder()
                    .clientConfig(clientConfig)
                    .allocator(allocator)
                    .scheduler(scheduler)
                    .sessionConfig(sessionConfig)
                    .build()
                    .factory()
                    .newFlightSession();

            ConsoleSession consoleSession = session.session()
                    .console(properties.getConsoleType())
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            this.flight = session;
            this.console = consoleSession;
            this.bootstrappedTables.clear();
            long current = generation.incrementAndGet();
            log.info("Connected to Deephaven at {} (generation {})", properties.target(), current);
            return true;
        } catch (Exception e) {
            log.warn("Deephaven at {} is not available: {}", properties.target(), rootMessage(e));
            disconnectLocked();
            return false;
        }
    }

    @Override
    public long generation() {
        return flight == null ? 0L : generation.get();
    }

    @Override
    public boolean isAvailable() {
        return flight != null;
    }

    @Override
    public void ensureTable(TableSchema schema, String connectorName) {
        lock.lock();
        try {
            requireSession();
            String failure = execute(
                    TableBootstrapScript.createIfMissing(schema, connectorName),
                    "create table " + schema.tableName());
            if (failure != null) {
                throw new DeephavenUnavailableException(
                        "could not create table " + schema.tableName() + ": " + failure);
            }
            if (!bootstrappedTables.contains(schema.tableName())) {
                bootstrappedTables.add(schema.tableName());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void addRows(TableSchema schema, List<Object[]> rows) {
        if (rows.isEmpty()) {
            return;
        }
        FlightSession session = requireSession();
        NewTable batch = toNewTable(schema.columns(), rows);
        if (schema.tableType().publisherBacked()) {
            addViaPublisher(session, schema, batch, rows.size());
            return;
        }
        try {
            session.addToInputTable(new ScopeId(schema.tableName()), batch, allocator)
                    .get(properties.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeephavenUnavailableException("interrupted publishing to " + schema.tableName(), e);
        } catch (Exception e) {
            throw new DeephavenUnavailableException(
                    "failed publishing " + rows.size() + " row(s) to " + schema.tableName()
                            + ": " + rootMessage(e), e);
        }
    }

    /**
     * Hand one Arrow batch to a table's server-side {@code TablePublisher}.
     *
     * <p>The scratch global is unique per batch because two flushes of the same connector can
     * overlap -- the scheduled one and a full-buffer flush by a submitting thread. The python
     * deletes it in a {@code finally}, so a failed {@code add} does not leave the rows pinned in
     * the script scope.
     */
    private void addViaPublisher(
            FlightSession session, TableSchema schema, NewTable batch, int rowCount) {
        String scratch =
                TableBootstrapScript.batchVariable(schema.tableName(), batchSequence.incrementAndGet());
        ExportId export = null;
        try {
            export = session.putExportManual(batch, allocator);
            session.session().publish(new ScopeId(scratch), export)
                    .get(properties.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
            String failure = execute(
                    TableBootstrapScript.publishBatch(schema.tableName(), scratch),
                    "publish to " + schema.tableName());
            if (failure != null) {
                throw new DeephavenUnavailableException("failed publishing " + rowCount
                        + " row(s) to " + schema.tableName() + ": " + failure);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeephavenUnavailableException(
                    "interrupted publishing to " + schema.tableName(), e);
        } catch (DeephavenUnavailableException e) {
            throw e;
        } catch (Exception e) {
            throw new DeephavenUnavailableException(
                    "failed publishing " + rowCount + " row(s) to " + schema.tableName()
                            + ": " + rootMessage(e), e);
        } finally {
            if (export != null) {
                // The scope now holds its own reference; ours is only in the way.
                session.release(export);
            }
        }
    }

    @Override
    public void deleteRows(TableSchema schema, List<Object[]> rows) {
        // Only a keyed table has anything to remove: an append-only table forbids deletion, and
        // a blink or ring table has already forgotten the row by the time the removal arrives.
        if (rows.isEmpty() || !schema.keyed()) {
            return;
        }
        FlightSession session = requireSession();
        // Only the key columns identify what to remove; sending the rest would be noise.
        List<ColumnSpec> keyColumns = schema.keyColumns().stream()
                .map(name -> schema.columns().get(schema.indexOf(name)))
                .toList();
        List<Object[]> keyRows = new ArrayList<>(rows.size());
        for (Object[] row : rows) {
            Object[] keys = new Object[keyColumns.size()];
            for (int i = 0; i < keyColumns.size(); i++) {
                keys[i] = row[schema.indexOf(keyColumns.get(i).name())];
            }
            keyRows.add(keys);
        }
        try {
            session.deleteFromInputTable(
                            new ScopeId(schema.tableName()), toNewTable(keyColumns, keyRows), allocator)
                    .get(properties.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DeephavenUnavailableException("interrupted deleting from " + schema.tableName(), e);
        } catch (Exception e) {
            throw new DeephavenUnavailableException(
                    "failed deleting " + rows.size() + " row(s) from " + schema.tableName()
                            + ": " + rootMessage(e), e);
        }
    }

    /** Transpose row-major values into the column-major Arrow batch the client wants. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static NewTable toNewTable(List<ColumnSpec> columns, List<Object[]> rows) {
        List<Column<?>> arrowColumns = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            ColumnSpec spec = columns.get(i);
            List values = new ArrayList<>(rows.size());
            for (Object[] row : rows) {
                values.add(row[i]);
            }
            arrowColumns.add(Column.of(spec.name(), (Class) spec.type().javaType(), values));
        }
        return NewTable.of(arrowColumns);
    }

    /**
     * Run python in the console session.
     *
     * <p>Package-private so {@code LiveTableTypeTest} can assert on the server rather than
     * reading tables back: {@code executeCode} reports failures, not values.
     *
     * @param script the python to run
     * @param what what the script is doing, for the interruption message
     * @return {@code null} on success, else the failure text
     */
    String execute(String script, String what) {
        ConsoleSession current = console;
        if (current == null) {
            return "no console session";
        }
        try {
            Changes changes = current.executeCode(script);
            Optional<String> error = changes.errorMessage();
            return error.orElse(null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "interrupted during " + what;
        } catch (Exception e) {
            return rootMessage(e);
        }
    }

    private FlightSession requireSession() {
        FlightSession session = flight;
        if (session == null) {
            throw new DeephavenUnavailableException(
                    "not connected to Deephaven at " + properties.target());
        }
        return session;
    }

    private void disconnectLocked() {
        ConsoleSession currentConsole = console;
        FlightSession currentFlight = flight;
        console = null;
        flight = null;
        bootstrappedTables.clear();
        if (currentConsole != null) {
            try {
                currentConsole.close();
            } catch (RuntimeException e) {
                log.debug("closing the console session failed", e);
            }
        }
        if (currentFlight != null) {
            try {
                currentFlight.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                log.debug("closing the flight session failed", e);
            }
        }
    }

    private static String rootMessage(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return message == null ? cause.getClass().getSimpleName() : message;
    }

    @Override
    public void close() {
        lock.lock();
        try {
            disconnectLocked();
            if (scheduler != null) {
                scheduler.shutdownNow();
                scheduler = null;
            }
            if (allocator != null) {
                allocator.close();
                allocator = null;
            }
        } finally {
            lock.unlock();
        }
    }
}
