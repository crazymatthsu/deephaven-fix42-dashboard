package com.fix42.dashboard.connectors.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

/**
 * The JDBC side of one connector: a database, one query, and how that query's result set is
 * read as a feed.
 *
 * <p>Lives under {@code source.jdbc}; its presence is what selects the JDBC source
 * ({@link SourceProperties}).
 *
 * <p>A query is a snapshot, not a stream, so this transport <em>polls</em>, and {@link #getMode()}
 * is JDBC's answer to the question {@code source.amps.sow} answers for AMPS and
 * {@code source.kafka.compacted} answers for Kafka -- whether the feed carries state or a stream
 * of events. {@link Mode#SNAPSHOT} re-runs the whole query every poll, which is state, so it is
 * what this transport reports as {@link SourceProperties#stateful()}; {@link Mode#INCREMENTAL}
 * reads forward from a high-water mark, which is a journal.
 *
 * <p>A result-set row has no wire format of its own, so the source synthesises one: each row is
 * serialised as a flat JSON object keyed by result-set column <em>label</em>. That is why a
 * connector on this transport must declare {@code format: JSON} ({@link ConnectorValidator}
 * enforces it) and why its field mappings address columns by label, aliases included.
 */
public class JdbcSourceProperties {

    /** Which of the query's rows a poll emits. */
    public enum Mode {
        /**
         * Every poll runs the full query and upserts every row: the result set <em>is</em> the
         * state. With {@link #getKeyColumn()} set it also reports the keys that stopped
         * appearing, which is this transport's spelling of an out-of-focus message.
         */
        SNAPSHOT,
        /**
         * Every poll emits only the rows whose {@link #getIncrementalColumn()} has passed the
         * mark left by the polls before it: an append-only journal, read forward.
         */
        INCREMENTAL
    }

    /** JDBC URL of the database to dial. The driver it names has to be on the classpath. */
    @NotBlank
    private String url;

    /** Database user. Blank leaves authentication to whatever the URL itself carries. */
    private String username;

    /**
     * Database password.
     *
     * <p>Arrives from the environment, never as a literal: the config tree is plaintext in git,
     * so {@code ConfigTreeTest} allows a credential key there to carry a single
     * {@code ${...}} placeholder and refuses anything else. A deployment writes
     * {@code password: "${JDBC_PASSWORD:}"} and sets the variable, overrides the whole property
     * as {@code DH_CONNECTORS_CONNECTORS_<n>_SOURCE_JDBC_PASSWORD}, or -- in a custom
     * application -- binds it from a secret store of its own.
     */
    private String password;

    /**
     * The query every poll runs, verbatim: the source never rewrites it, not even to apply the
     * {@link Mode#INCREMENTAL} mark. What the database is asked is exactly what is written
     * here, so an expensive query is expensive once per {@link #getPollInterval()}.
     */
    @NotBlank
    private String query;

    /** Whether a poll re-reads everything or only what is new. */
    @NotNull
    private Mode mode = Mode.SNAPSHOT;

    /**
     * {@link Mode#SNAPSHOT} only: the result-set column carrying each row's source key.
     *
     * <p>Setting it is what lets a snapshot express a <em>removal</em> -- the source remembers
     * the key set between polls and emits a delete for every key that vanished. Left unset there
     * are no deletes at all and a row deleted in the database lingers in the table: the
     * configured trade-off for a query with no stable key.
     */
    private String keyColumn;

    /**
     * {@link Mode#INCREMENTAL} only: a monotonically increasing column (a timestamp, a sequence)
     * that says what is new.
     *
     * <p>The mark lives in memory only, so a restarted connector reads from the beginning again.
     * That is the framework's rehydration contract rather than a gap: the Deephaven table is the
     * state, and every restart rebuilds it from nothing.
     */
    private String incrementalColumn;

    /** How long a poll waits before running the query again. */
    @NotNull
    private Duration pollInterval = Duration.ofSeconds(5);

    /** How long to wait before redialling after a failed connection or a failed poll. */
    @NotNull
    private Duration reconnectDelay = Duration.ofSeconds(5);

    /**
     * Rows the driver fetches per round trip. Positive rather than JDBC's {@code 0}, which means
     * "the driver's default" -- and several drivers default to materialising the entire result
     * set, the one thing this knob exists to bound.
     */
    @Min(1)
    private int fetchSize = 1_000;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public String getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(String keyColumn) {
        this.keyColumn = keyColumn;
    }

    public String getIncrementalColumn() {
        return incrementalColumn;
    }

    public void setIncrementalColumn(String incrementalColumn) {
        this.incrementalColumn = incrementalColumn;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }

    public Duration getReconnectDelay() {
        return reconnectDelay;
    }

    public void setReconnectDelay(Duration reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }
}
