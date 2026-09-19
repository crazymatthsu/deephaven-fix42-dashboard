package com.fix42.dashboard.connectors.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.JdbcSourceProperties;
import com.fix42.dashboard.connectors.source.RecordHandler;
import com.fix42.dashboard.connectors.source.RecordSource;
import com.fix42.dashboard.connectors.source.SourceRecord;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RecordSource} over a database query.
 *
 * <p>Reads its endpoint, its query and its poll mode from {@code source.jdbc}. A query is a
 * <strong>snapshot, not a stream</strong>: there is nothing to subscribe to, so this source
 * polls, and {@code source.jdbc.mode} decides what each poll emits.
 *
 * <table border="1">
 *   <caption>Poll modes</caption>
 *   <tr><th>{@code mode}</th><th>each poll emits</th><th>the framework analog</th></tr>
 *   <tr><td>{@code SNAPSHOT}</td>
 *       <td>every row of the query, as an {@code UPSERT}; with {@code key-column} set, also a
 *           {@code DELETE} for every key that appeared last poll and not this one</td>
 *       <td>an AMPS SOW replay, and its out-of-focus message</td></tr>
 *   <tr><td>{@code INCREMENTAL}</td>
 *       <td>only the rows whose {@code incremental-column} is above the high-water mark of
 *           the previous polls, as {@code UPSERT}s</td>
 *       <td>a journal topic read forward from a bookmark</td></tr>
 * </table>
 *
 * <h2>The rows are turned into JSON here</h2>
 *
 * <p>A result-set row has no wire format, so this source synthesises one: each row becomes a
 * flat JSON object keyed by result-set column <em>label</em>, which is why the validator
 * requires {@code format: JSON} and why field mappings address columns by label (an
 * {@code AS} alias included). Numbers stay numbers and booleans stay booleans, dates and times
 * become ISO-8601 text -- a timestamp in its {@code Instant} form -- and SQL {@code NULL}
 * becomes an explicit JSON {@code null}, which the JSON decoder already reads as "present, and
 * cleared".
 *
 * <h2>The query is never rewritten</h2>
 *
 * <p>{@code INCREMENTAL} filters in this process rather than wrapping the configured SQL: what
 * the database is asked is exactly what the operator wrote, so a query that is already correct
 * cannot be broken by string surgery, and an expensive one is visibly expensive. The
 * high-water mark lives in memory only, so a restarted connector reads from the beginning
 * again -- the framework's rehydration contract, where the Deephaven table is the state and
 * every restart rebuilds it from nothing.
 *
 * <p>{@link #start} returns as soon as the poll thread is running, rather than connecting on
 * the caller's thread the way {@code AmpsRecordSource} does. For a transport whose normal state
 * includes "the database is not up yet", a failed first connect is the same event as a dropped
 * one and both belong in the same backoff -- so {@link #isConnected()}, not a thrown
 * {@code start}, is what reports the connection.
 */
public class JdbcRecordSource implements RecordSource {

    private static final Logger log = LoggerFactory.getLogger(JdbcRecordSource.class);

    /** How long {@link #close()} waits for the poll thread before giving up on it. */
    private static final long CLOSE_JOIN_MILLIS = 5_000;

    private final ConnectorProperties connector;
    private final JdbcSourceProperties source;
    private final ObjectMapper mapper = new ObjectMapper();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);

    /** Monitor the poll interval and the reconnect backoff wait on, so a close cuts them short. */
    private final Object idle = new Object();

    /**
     * Keys seen by the previous {@code SNAPSHOT} poll, to diff this poll against. Touched only
     * by the poll thread, and replaced rather than mutated, so a failed poll leaves the last
     * good key set standing -- a poll that threw halfway is not "everything vanished".
     */
    private Set<String> previousKeys = Set.of();

    /**
     * The {@code INCREMENTAL} high-water mark, normalised by {@link #mark(Object)}. Touched
     * only by the poll thread; deliberately not persisted anywhere.
     */
    private Object watermark;

    private volatile Connection connection;
    private volatile Thread thread;

    public JdbcRecordSource(ConnectorProperties connector) {
        this.connector = connector;
        this.source = connector.getSource().getJdbc();
    }

    @Override
    public void start(RecordHandler handler) {
        log.info("[{}] starting JDBC source: {} ({} poll every {})", connector.getName(),
                source.getUrl(), source.getMode(), source.getPollInterval());
        Thread runner = new Thread(() -> run(handler), connector.getName() + "-jdbc");
        runner.setDaemon(true);
        this.thread = runner;
        runner.start();
    }

    /**
     * Connect, poll until the connection or {@link #close()} ends it, back off, connect again.
     * One iteration of the outer loop is one connection's lifetime.
     */
    private void run(RecordHandler handler) {
        while (!closed.get()) {
            Connection open = null;
            try {
                open = connect();
                this.connection = open;
                if (closed.get()) {
                    // close() publishes `closed` and then reads `connection`; this reads them
                    // the other way round, so between the two at least one of us sees the
                    // other. Without the check, a close landing in this window would leave a
                    // connection nobody closes.
                    break;
                }
                connected.set(true);
                log.info("[{}] connected to {}", connector.getName(), source.getUrl());
                while (!closed.get()) {
                    poll(open, handler);
                    if (!sleep(source.getPollInterval())) {
                        break;
                    }
                }
            } catch (SQLException | RuntimeException e) {
                if (!closed.get()) {
                    log.error("[{}] JDBC source failed on {}",
                            connector.getName(), source.getUrl(), e);
                }
            } finally {
                connected.set(false);
                this.connection = null;
                closeQuietly(open);
            }
            if (!closed.get()) {
                log.info("[{}] reconnecting to {} in {}", connector.getName(), source.getUrl(),
                        source.getReconnectDelay());
                if (!sleep(source.getReconnectDelay())) {
                    break;
                }
            }
        }
        connected.set(false);
        log.info("[{}] JDBC source stopped", connector.getName());
    }

    /**
     * Open the configured connection.
     *
     * <p>A blank username hands the URL the whole job, which is what an embedded or
     * trust-authenticated database expects; passing {@code ("", "")} to those is not the same
     * thing and is rejected by some drivers.
     *
     * @return a new connection
     * @throws SQLException when the database cannot be reached or refuses the credentials
     */
    private Connection connect() throws SQLException {
        String username = source.getUsername();
        if (username == null || username.isBlank()) {
            return DriverManager.getConnection(source.getUrl());
        }
        return DriverManager.getConnection(source.getUrl(), username, source.getPassword());
    }

    /**
     * Run the query once and emit what it says.
     *
     * @param open the connection to query
     * @param handler where the records go
     * @throws SQLException when the query fails, which ends this connection's lifetime
     */
    private void poll(Connection open, RecordHandler handler) throws SQLException {
        String keyColumn = source.getKeyColumn();
        boolean tracking = snapshot() && keyColumn != null && !keyColumn.isBlank();
        Set<String> keys = tracking ? new LinkedHashSet<>() : Set.of();
        // Fixed for the whole poll: a result set has no order unless the query gave it one,
        // so every row is compared against the mark the PREVIOUS polls left, never against a
        // mark this poll has already moved.
        Object mark = watermark;

        try (Statement statement = open.createStatement()) {
            statement.setFetchSize(source.getFetchSize());
            try (ResultSet rows = statement.executeQuery(source.getQuery())) {
                ResultSetMetaData meta = rows.getMetaData();
                while (rows.next() && !closed.get()) {
                    if (!snapshot()) {
                        Object value = mark(rows.getObject(source.getIncrementalColumn()));
                        if (value == null) {
                            // A null in the column that orders the feed cannot be placed
                            // against the mark, and emitting it would re-emit it every poll
                            // for as long as it sits in the table.
                            log.warn("[{}] skipping a row whose incremental column '{}' is null",
                                    connector.getName(), source.getIncrementalColumn());
                            continue;
                        }
                        if (!above(value, mark)) {
                            continue;
                        }
                        if (above(value, watermark)) {
                            watermark = value;
                        }
                    }
                    String key = tracking ? key(rows, keyColumn, keys) : null;
                    emit(json(rows, meta), key, handler);
                }
            }
        }

        if (tracking) {
            for (String gone : previousKeys) {
                if (!keys.contains(gone)) {
                    // The JDBC spelling of an out-of-focus message: the key alone, with an
                    // empty payload for the DELETE path's decode to read as nothing.
                    emit("", gone, SourceRecord.Action.DELETE, handler);
                }
            }
            previousKeys = keys;
        }
    }

    /** Whether this source re-reads the whole query every poll. */
    private boolean snapshot() {
        return source.getMode() == JdbcSourceProperties.Mode.SNAPSHOT;
    }

    /**
     * The current row's key, also registering it as seen this poll.
     *
     * @return the key, or {@code null} when the row has none
     */
    private String key(ResultSet rows, String keyColumn, Set<String> keys) throws SQLException {
        Object value = rows.getObject(keyColumn);
        if (value == null) {
            // One unkeyable row, not a broken feed: it is published without a key (so it is
            // never a candidate for a vanish-delete either) and the poll carries on.
            log.warn("[{}] row with a null key column '{}': published without a source key, "
                    + "and not tracked for deletes", connector.getName(), keyColumn);
            return null;
        }
        String key = String.valueOf(value);
        keys.add(key);
        return key;
    }

    private void emit(String data, String key, RecordHandler handler) {
        emit(data, key, SourceRecord.Action.UPSERT, handler);
    }

    private void emit(
            String data, String key, SourceRecord.Action action, RecordHandler handler) {
        try {
            if (action == SourceRecord.Action.DELETE) {
                handler.onRecord(SourceRecord.delete(data, key));
            } else if (key == null) {
                handler.onRecord(SourceRecord.of(data));
            } else {
                // The key rides along on upserts too, the way a Kafka message key does, so
                // deephaven.source-key-column holds a value on every row and not just on the
                // ones that leave.
                handler.onRecord(SourceRecord.of(data, key));
            }
        } catch (RuntimeException e) {
            log.error("[{}] failed to handle a JDBC row", connector.getName(), e);
        }
    }

    /**
     * Serialise the current row as a flat JSON object keyed by column label.
     *
     * @param rows positioned on the row to serialise
     * @param meta that result set's metadata
     * @return the row's JSON text
     */
    private String json(ResultSet rows, ResultSetMetaData meta) throws SQLException {
        ObjectNode row = mapper.createObjectNode();
        for (int column = 1; column <= meta.getColumnCount(); column++) {
            put(row, meta.getColumnLabel(column), rows.getObject(column));
        }
        return row.toString();
    }

    /**
     * One column's value, in the JSON type that keeps its meaning.
     *
     * <p>Numbers and booleans stay themselves, so a mapping to a numeric column has a number
     * to coerce. Dates and times become ISO-8601 text, because JSON has no date type and the
     * decoders read instants from text; a timestamp without a zone is read in the JVM's zone,
     * which is what {@link Timestamp#toInstant()} does and what the database round-tripped it
     * through. A SQL {@code NULL} becomes an explicit JSON null -- an explicit clear, not an
     * absent field. Everything else (a BLOB, a driver's own vendor type) gets its string form,
     * which is as much as this source can honestly claim to know about it.
     */
    private static void put(ObjectNode row, String label, Object value) {
        switch (value) {
            case null -> row.putNull(label);
            case Boolean v -> row.put(label, v);
            case BigDecimal v -> row.put(label, v);
            case BigInteger v -> row.put(label, v);
            case Byte v -> row.put(label, v.intValue());
            case Short v -> row.put(label, v.intValue());
            case Integer v -> row.put(label, v);
            case Long v -> row.put(label, v);
            case Float v -> row.put(label, v);
            case Double v -> row.put(label, v);
            case Number v -> row.put(label, v.doubleValue());
            case Timestamp v -> row.put(label, v.toInstant().toString());
            case java.sql.Date v -> row.put(label, v.toLocalDate().toString());
            case Time v -> row.put(label, v.toLocalTime().toString());
            case Instant v -> row.put(label, v.toString());
            case OffsetDateTime v -> row.put(label, v.toInstant().toString());
            case LocalDateTime v -> row.put(label, v.atZone(ZoneId.systemDefault())
                    .toInstant().toString());
            case LocalDate v -> row.put(label, v.toString());
            case LocalTime v -> row.put(label, v.toString());
            default -> row.put(label, String.valueOf(value));
        }
    }

    /**
     * Normalise a watermark value so two polls compare like with like.
     *
     * <p>Numbers collapse to {@code BigDecimal} and timestamps to {@code Instant}, so a
     * {@code BIGINT} sequence and a {@code NUMERIC} one order the same way and a driver that
     * hands back {@code LocalDateTime} on one call and {@code Timestamp} on another still
     * makes progress.
     *
     * @param value the raw column value
     * @return the comparable form, or {@code null} for SQL {@code NULL}
     */
    private static Object mark(Object value) {
        return switch (value) {
            case null -> null;
            case BigDecimal v -> v;
            case Number v -> new BigDecimal(v.toString());
            case Timestamp v -> v.toInstant();
            case OffsetDateTime v -> v.toInstant();
            case LocalDateTime v -> v.atZone(ZoneId.systemDefault()).toInstant();
            default -> value;
        };
    }

    /**
     * Whether {@code candidate} is past the mark. An unset mark is past by definition -- the
     * first poll of an incremental feed reads everything, which is the rehydration contract.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static boolean above(Object candidate, Object mark) {
        if (mark == null) {
            return true;
        }
        if (candidate instanceof Comparable && candidate.getClass() == mark.getClass()) {
            return ((Comparable) candidate).compareTo(mark) > 0;
        }
        // Mixed types out of one column is already odd; string order is the only ordering
        // left that cannot throw.
        return String.valueOf(candidate).compareTo(String.valueOf(mark)) > 0;
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public void close() {
        closed.set(true);
        connected.set(false);
        Connection open = this.connection;
        this.connection = null;
        if (open != null) {
            try {
                // A query in flight has no timeout; closing the connection under it is what
                // unblocks it, the way closing the socket unblocks the TCP source's read.
                open.close();
            } catch (SQLException e) {
                log.debug("[{}] JDBC connection close failed", connector.getName(), e);
            }
        }
        synchronized (idle) {
            idle.notifyAll();
        }
        Thread runner = this.thread;
        this.thread = null;
        if (runner == null || runner == Thread.currentThread()) {
            return;
        }
        try {
            runner.join(CLOSE_JOIN_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        if (runner.isAlive()) {
            log.warn("[{}] JDBC poll thread did not stop within {}ms",
                    connector.getName(), CLOSE_JOIN_MILLIS);
        }
    }

    /** The connection belongs to its poll thread, so only that thread ever closes it here. */
    private void closeQuietly(Connection open) {
        if (open == null) {
            return;
        }
        try {
            open.close();
        } catch (SQLException e) {
            log.debug("[{}] JDBC connection close failed", connector.getName(), e);
        }
    }

    /**
     * Wait out the poll interval or the reconnect backoff, returning early when
     * {@link #close()} rings the monitor.
     *
     * @param delay how long to wait
     * @return {@code false} if the source should stop instead of carrying on
     */
    private boolean sleep(Duration delay) {
        synchronized (idle) {
            if (closed.get()) {
                return false;
            }
            try {
                idle.wait(Math.max(1L, delay.toMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return !closed.get();
    }
}
