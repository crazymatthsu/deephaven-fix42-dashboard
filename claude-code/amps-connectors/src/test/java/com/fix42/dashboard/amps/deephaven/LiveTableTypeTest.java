package com.fix42.dashboard.amps.deephaven;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.config.AmpsConnectorsProperties;
import com.fix42.dashboard.amps.config.ColumnType;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.mapping.TableSchema;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * The generated bootstrap python and both publish paths, run against a real Deephaven server.
 *
 * <p>The rest of the suite fakes the server, so the one question it cannot answer is whether the
 * server accepts what we emit -- which is the whole risk of a table type that has to be built out
 * of python. Assertions are made <em>in</em> python, because {@code executeCode} reports failures
 * rather than values. Opt in with a server on {@code amps.live.port} (default 10000):
 *
 * <pre>
 *   podman run -d --name dh -p 10000:10000 \
 *     -e START_OPTS="-Ddeephaven.console.type=python \
 *        -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler" \
 *     ghcr.io/deephaven/server:42.4
 *   ./gradlew :amps-connectors:test --tests '*LiveTableTypeTest' -Damps.live=true
 * </pre>
 */
@EnabledIfSystemProperty(named = "amps.live", matches = "true")
class LiveTableTypeTest {

    private static FlightDeephavenGateway gateway;

    @BeforeAll
    static void connect() {
        AmpsConnectorsProperties properties = new AmpsConnectorsProperties();
        properties.getDeephaven().setHost("localhost");
        properties.getDeephaven().setPort(Integer.getInteger("amps.live.port", 10_000));
        gateway = new FlightDeephavenGateway(properties);
        assertThat(gateway.refresh()).as("connected to Deephaven").isNotZero();
    }

    @AfterAll
    static void disconnect() {
        if (gateway != null) {
            gateway.close();
        }
    }

    @Test
    @DisplayName("a keyed input table upserts by key")
    void keyed() {
        TableSchema schema = schemaFor(TestConnectors.fixOrders(), "live_keyed");
        gateway.ensureTable(schema, "live-keyed");

        gateway.addRows(schema, List.<Object[]>of(order("A", 10.0), order("B", 20.0)));
        gateway.addRows(schema, List.<Object[]>of(order("A", 11.0)));

        awaitSize("live_keyed", 2);
        assertSize("live_keyed", 2);
    }

    @Test
    @DisplayName("an append-only input table keeps every row")
    void appendOnly() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getDeephaven().setKeyColumns(List.of());
        connector.getSource().setSow(false);
        TableSchema schema = schemaFor(connector, "live_append");
        gateway.ensureTable(schema, "live-append");

        gateway.addRows(schema, List.<Object[]>of(order("A", 10.0), order("B", 20.0)));
        gateway.addRows(schema, List.<Object[]>of(order("A", 11.0)));

        awaitSize("live_append", 3);
    }

    @Test
    @DisplayName("a blink table is fed through its publisher and retains nothing")
    void blink() {
        TableSchema schema = schemaFor(TestConnectors.jsonTicks(), "live_blink");
        gateway.ensureTable(schema, "live-blink");

        // An append-only view proves the rows arrived even after the blink table drops them.
        run("from deephaven.stream import blink_to_append_only\n"
                + "live_blink_seen = blink_to_append_only(live_blink)\n");
        gateway.addRows(schema, List.<Object[]>of(tick("AAPL", 1.0), tick("MSFT", 2.0)));
        gateway.addRows(schema, List.<Object[]>of(tick("AAPL", 3.0)));

        awaitSize("live_blink_seen", 3);
        awaitSize("live_blink", 0);
    }

    @Test
    @DisplayName("a ring table keeps only its capacity")
    void ring() {
        TableSchema schema = schemaFor(TestConnectors.jsonTicksRing(3), "live_ring");
        gateway.ensureTable(schema, "live-ring");

        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            rows.add(tick("SYM" + i, i));
        }
        gateway.addRows(schema, rows);

        awaitSize("live_ring", 3);
        // ...and they are the last three, not the first.
        run("assert live_ring.to_string().count('SYM9') == 1, live_ring.to_string()\n"
                + "assert live_ring.to_string().count('SYM0') == 0, live_ring.to_string()\n");
    }

    @Test
    @DisplayName("bootstrapping twice is a no-op; a changed column type is refused")
    void adoptingAnExistingTable() {
        TableSchema schema = schemaFor(TestConnectors.fixOrders(), "live_adopt");
        gateway.ensureTable(schema, "live-adopt");
        gateway.ensureTable(schema, "live-adopt");

        ConnectorProperties retyped = TestConnectors.fixOrders();
        retyped.setFields(new ArrayList<>(retyped.getFields()));
        retyped.getFields().set(2, TestConnectors.field("38", "OrderQty", ColumnType.LONG));
        assertThatThrownBy(() -> gateway.ensureTable(schemaFor(retyped, "live_adopt"), "live-adopt"))
                .isInstanceOf(DeephavenUnavailableException.class)
                .hasMessageContaining("type of column(s)")
                .hasMessageContaining("OrderQty");
    }

    @Test
    @DisplayName("a blink table nobody holds the publisher for is refused, not adopted")
    void refusesToAdoptAForeignBlinkTable() {
        run("from deephaven import empty_table\n"
                + "live_foreign = empty_table(0).update(['Symbol = ``', 'Price = 0.0'])\n");
        TableSchema schema = schemaFor(TestConnectors.jsonTicks(), "live_foreign");

        assertThatThrownBy(() -> gateway.ensureTable(schema, "live-foreign"))
                .isInstanceOf(DeephavenUnavailableException.class)
                .hasMessageContaining("was not created by this connector");
    }

    private static TableSchema schemaFor(ConnectorProperties connector, String table) {
        connector.getDeephaven().setTable(table);
        return TableSchema.of(connector);
    }

    private static Object[] order(String clOrdId, double price) {
        return new Object[] {clOrdId, "IBM", 100.0, price, Instant.parse("2024-01-15T14:30:00Z")};
    }

    private static Object[] tick(String symbol, double price) {
        return new Object[] {symbol, price};
    }

    /** Assert a table's size server-side; the failure text is the python assertion's. */
    private static void assertSize(String table, long expected) {
        run("assert " + table + ".size == " + expected
                + ", 'size of " + table + " is %d, expected " + expected + "' % " + table + ".size\n");
    }

    /** Retry {@link #assertSize} while the update graph settles. */
    private static void awaitSize(String table, long expected) {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(15).toNanos();
        while (true) {
            String failure = gateway.execute(
                    "assert " + table + ".size == " + expected + "\n", "size check");
            if (failure == null) {
                return;
            }
            if (System.nanoTime() > deadline) {
                assertSize(table, expected);
                throw new AssertionError("size of " + table + " never reached " + expected);
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private static void run(String script) {
        String failure = gateway.execute(script, "test setup");
        if (failure != null) {
            throw new IllegalStateException(failure);
        }
    }
}
