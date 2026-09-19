package com.fix42.dashboard.connectors.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.JdbcSourceProperties;
import com.fix42.dashboard.connectors.config.SourceFormat;
import com.fix42.dashboard.connectors.config.SourceProperties;
import com.fix42.dashboard.connectors.source.SourceRecord;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The JDBC source against a real in-memory H2 database -- no mock driver, no containers.
 *
 * <p>What is worth asserting here is everything the source decides for itself: the JSON it
 * synthesises from a result set and the types it keeps, what a key that stopped appearing turns
 * into, and where the incremental mark leaves off. The driver's own behaviour is H2's to test.
 *
 * <p>Identifiers are quoted in the DDL because H2 folds unquoted ones to upper case: the label a
 * mapping addresses is whatever {@code ResultSetMetaData} reports, so the tests spell the
 * columns the way the payload will carry them.
 */
class JdbcRecordSourceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Fast enough that "the next poll" is not a wait; slow enough not to spin the CPU. */
    private static final Duration POLL = Duration.ofMillis(50);

    /** One database per test, so nothing leaks between them. */
    private static final AtomicInteger DATABASES = new AtomicInteger();

    // ---- fixtures ------------------------------------------------------------------

    /**
     * A fresh in-memory database. {@code DB_CLOSE_DELAY=-1} keeps it alive between the test's
     * own connections and the source's, which open and close independently.
     */
    private static String database() {
        return "jdbc:h2:mem:jdbc-source-" + DATABASES.incrementAndGet() + ";DB_CLOSE_DELAY=-1";
    }

    private static void execute(String url, String... statements) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url);
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private static ConnectorProperties connector(String url, String query) {
        JdbcSourceProperties jdbc = new JdbcSourceProperties();
        jdbc.setUrl(url);
        jdbc.setQuery(query);
        jdbc.setPollInterval(POLL);
        jdbc.setReconnectDelay(Duration.ofMillis(50));

        SourceProperties source = new SourceProperties();
        source.setJdbc(jdbc);

        ConnectorProperties connector = new ConnectorProperties();
        connector.setName("positions-jdbc");
        connector.setFormat(SourceFormat.JSON);
        connector.setSource(source);
        return connector;
    }

    private static JdbcRecordSource started(
            ConnectorProperties connector, List<SourceRecord> received) {
        JdbcRecordSource source = new JdbcRecordSource(connector);
        source.start(received::add);
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(source::isConnected);
        return source;
    }

    private static void awaitRecords(List<SourceRecord> received, int count) {
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> received.size() >= count);
    }

    private static JsonNode payload(SourceRecord record) throws Exception {
        return MAPPER.readTree(record.data());
    }

    /** Let several more polls run, for the assertions about what does NOT arrive. */
    private static void severalMorePolls() throws InterruptedException {
        Thread.sleep(POLL.toMillis() * 6);
    }

    private static boolean pollThreadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.getName().equals("positions-jdbc-jdbc")
                        && thread.isAlive());
    }

    // ---- the row, as JSON ------------------------------------------------------------

    @Test
    @DisplayName("a snapshot poll emits every row as JSON, each column in its own type")
    void snapshotEmitsEveryRowWithItsTypes() throws Exception {
        String url = database();
        execute(url,
                "CREATE TABLE positions (\"account\" VARCHAR(16), \"quantity\" INTEGER, "
                        + "\"avg_cost\" DECIMAL(12,4), \"active\" BOOLEAN, "
                        + "\"updated_at\" TIMESTAMP, \"note\" VARCHAR(32))",
                "INSERT INTO positions VALUES ('ACC-1', 250, 101.2500, TRUE, "
                        + "TIMESTAMP '2026-09-18 12:34:56', NULL)");

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (JdbcRecordSource source = started(
                connector(url, "SELECT * FROM positions"), received)) {
            awaitRecords(received, 1);

            SourceRecord record = received.get(0);
            assertThat(record.action()).isEqualTo(SourceRecord.Action.UPSERT);
            assertThat(record.key()).as("no key-column configured").isNull();

            JsonNode row = payload(record);
            assertThat(row.get("account").asText()).isEqualTo("ACC-1");
            assertThat(row.get("quantity").isInt()).as("a number, not its text").isTrue();
            assertThat(row.get("quantity").intValue()).isEqualTo(250);
            assertThat(row.get("avg_cost").isNumber()).isTrue();
            assertThat(row.get("avg_cost").decimalValue()).isEqualByComparingTo("101.2500");
            assertThat(row.get("active").isBoolean()).isTrue();
            assertThat(row.get("active").booleanValue()).isTrue();
            // ISO-8601 in its Instant form, resolved through the zone the database
            // round-tripped the value in -- which is what Timestamp.toInstant() uses.
            assertThat(row.get("updated_at").asText())
                    .isEqualTo(Timestamp.valueOf("2026-09-18 12:34:56").toInstant().toString());
            // An explicit null, which the JSON decoder reads as "present, and cleared" --
            // not an absent field, which would leave the column untouched.
            assertThat(row.get("note").isNull()).isTrue();
        }
    }

    @Test
    @DisplayName("key-column rides along on upserts, the way a Kafka message key does")
    void theKeyColumnIsCarriedOnUpserts() throws Exception {
        String url = database();
        execute(url,
                "CREATE TABLE positions (\"account\" VARCHAR(16), \"symbol\" VARCHAR(16), "
                        + "\"quantity\" INTEGER)",
                "INSERT INTO positions VALUES ('ACC-1', 'AAPL', 250)",
                "INSERT INTO positions VALUES ('ACC-1', 'MSFT', 100)");

        ConnectorProperties connector = connector(url, keyedQuery());
        connector.getSource().getJdbc().setKeyColumn("position_key");

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (JdbcRecordSource source = started(connector, received)) {
            awaitRecords(received, 2);

            assertThat(received.subList(0, 2)).extracting(SourceRecord::key)
                    .containsExactly("ACC-1:AAPL", "ACC-1:MSFT");
            assertThat(received.subList(0, 2)).extracting(SourceRecord::action)
                    .containsOnly(SourceRecord.Action.UPSERT);
            assertThat(payload(received.get(0)).get("position_key").asText())
                    .isEqualTo("ACC-1:AAPL");
        }
    }

    // ---- what a snapshot can say about a row that left -------------------------------

    @Test
    @DisplayName("a row deleted between polls arrives as a DELETE carrying its key")
    void aVanishedKeyBecomesADelete() throws Exception {
        String url = database();
        execute(url,
                "CREATE TABLE positions (\"account\" VARCHAR(16), \"symbol\" VARCHAR(16), "
                        + "\"quantity\" INTEGER)",
                "INSERT INTO positions VALUES ('ACC-1', 'AAPL', 250)",
                "INSERT INTO positions VALUES ('ACC-1', 'MSFT', 100)");

        ConnectorProperties connector = connector(url, keyedQuery());
        connector.getSource().getJdbc().setKeyColumn("position_key");

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (JdbcRecordSource source = started(connector, received)) {
            awaitRecords(received, 2);
            execute(url, "DELETE FROM positions WHERE \"symbol\" = 'MSFT'");

            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> received.stream()
                    .anyMatch(record -> record.action() == SourceRecord.Action.DELETE));

            SourceRecord delete = received.stream()
                    .filter(record -> record.action() == SourceRecord.Action.DELETE)
                    .findFirst()
                    .orElseThrow();
            assertThat(delete.key()).isEqualTo("ACC-1:MSFT");
            // Empty rather than null: the DELETE path still decodes the payload, and an
            // empty document is the quiet answer -- a removal is addressed by its key.
            assertThat(delete.data()).isEmpty();
        }
    }

    @Test
    @DisplayName("a row inserted between polls arrives on the next one")
    void aNewRowArrivesOnTheNextPoll() throws Exception {
        String url = database();
        execute(url,
                "CREATE TABLE positions (\"account\" VARCHAR(16), \"symbol\" VARCHAR(16), "
                        + "\"quantity\" INTEGER)",
                "INSERT INTO positions VALUES ('ACC-1', 'AAPL', 250)");

        ConnectorProperties connector = connector(url, keyedQuery());
        connector.getSource().getJdbc().setKeyColumn("position_key");

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (JdbcRecordSource source = started(connector, received)) {
            awaitRecords(received, 1);
            execute(url, "INSERT INTO positions VALUES ('ACC-2', 'TSLA', 40)");

            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> received.stream()
                    .anyMatch(record -> "ACC-2:TSLA".equals(record.key())));
            assertThat(received).extracting(SourceRecord::action)
                    .doesNotContain(SourceRecord.Action.DELETE);
        }
    }

    @Test
    @DisplayName("without a key-column a snapshot cannot delete, so a removed row lingers")
    void withoutAKeyColumnThereAreNoDeletes() throws Exception {
        String url = database();
        execute(url,
                "CREATE TABLE positions (\"account\" VARCHAR(16), \"quantity\" INTEGER)",
                "INSERT INTO positions VALUES ('ACC-1', 250)",
                "INSERT INTO positions VALUES ('ACC-2', 100)");

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (JdbcRecordSource source = started(
                connector(url, "SELECT * FROM positions"), received)) {
            awaitRecords(received, 2);
            execute(url, "DELETE FROM positions WHERE \"account\" = 'ACC-2'");
            severalMorePolls();

            // The configured trade-off: no key means nothing a removal could address, so the
            // row simply stops being refreshed and stays in the table.
            assertThat(received).extracting(SourceRecord::action)
                    .containsOnly(SourceRecord.Action.UPSERT);
            assertThat(received).extracting(SourceRecord::key).containsOnlyNulls();
        }
    }

    // ---- incremental -----------------------------------------------------------------

    @Test
    @DisplayName("an incremental poll emits only what is above the mark, and never again")
    void incrementalEmitsOnlyAboveTheWatermark() throws Exception {
        String url = database();
        execute(url,
                "CREATE TABLE trades (\"trade_id\" VARCHAR(16), \"seq\" BIGINT)",
                "INSERT INTO trades VALUES ('T-1', 1)",
                "INSERT INTO trades VALUES ('T-2', 2)");

        ConnectorProperties connector =
                connector(url, "SELECT * FROM trades ORDER BY \"seq\"");
        connector.getSource().getJdbc().setMode(JdbcSourceProperties.Mode.INCREMENTAL);
        connector.getSource().getJdbc().setIncrementalColumn("seq");

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (JdbcRecordSource source = started(connector, received)) {
            // Poll 1: no mark yet, so the whole query -- the rehydration read.
            awaitRecords(received, 2);
            // Poll 2 and onwards: nothing is above the mark, so nothing is emitted.
            severalMorePolls();
            assertThat(received).as("the rows already read are never re-emitted").hasSize(2);

            execute(url, "INSERT INTO trades VALUES ('T-3', 3)");
            awaitRecords(received, 3);
            severalMorePolls();

            assertThat(received).hasSize(3);
            assertThat(received).extracting(record -> {
                try {
                    return payload(record).get("trade_id").asText();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }).containsExactly("T-1", "T-2", "T-3");
            assertThat(received).extracting(SourceRecord::action)
                    .containsOnly(SourceRecord.Action.UPSERT);
        }
    }

    // ---- lifecycle ---------------------------------------------------------------------

    @Test
    @DisplayName("a URL nothing can dial is retried, not fatal")
    void aBadUrlLoopsTheReconnectBackoff() throws Exception {
        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        // No driver on the classpath answers this, which is what a typo or a missing driver
        // jar looks like: a connection failure, and those belong in the backoff.
        JdbcRecordSource source =
                new JdbcRecordSource(connector("jdbc:nosuchdb://localhost:5432/trading",
                        "SELECT 1"));
        source.start(received::add);
        severalMorePolls();

        assertThat(source.isConnected()).isFalse();
        assertThat(received).isEmpty();
        assertThat(pollThreadAlive()).as("still retrying rather than dead").isTrue();

        source.close();
        assertThat(pollThreadAlive()).as("no connector thread is left behind").isFalse();
    }

    @Test
    @DisplayName("close() cuts the poll interval short instead of waiting it out")
    void closeStopsThePollThreadMidInterval() throws Exception {
        String url = database();
        execute(url,
                "CREATE TABLE positions (\"account\" VARCHAR(16))",
                "INSERT INTO positions VALUES ('ACC-1')");

        ConnectorProperties connector = connector(url, "SELECT * FROM positions");
        // Long enough that a close waiting out the interval would be obvious.
        connector.getSource().getJdbc().setPollInterval(Duration.ofSeconds(30));

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        JdbcRecordSource source = started(connector, received);
        awaitRecords(received, 1);

        long start = System.nanoTime();
        source.close();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isLessThan(Duration.ofSeconds(5));
        assertThat(source.isConnected()).isFalse();
        assertThat(pollThreadAlive()).as("no connector thread is left behind").isFalse();
        // Idempotent, the way ConnectorManager calls it.
        source.close();
    }

    // ---- helpers -------------------------------------------------------------------------

    /**
     * The composite-key shape the config tree's example uses: a key the query builds, quoted so
     * the label stays as written.
     */
    private static String keyedQuery() {
        return "SELECT \"account\" || ':' || \"symbol\" AS \"position_key\", "
                + "\"account\", \"symbol\", \"quantity\" "
                + "FROM positions ORDER BY \"position_key\"";
    }
}
