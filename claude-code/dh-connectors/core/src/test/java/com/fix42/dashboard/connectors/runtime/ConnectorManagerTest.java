package com.fix42.dashboard.connectors.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.connectors.TestConnectors;
import com.fix42.dashboard.connectors.config.ConnectorsProperties;
import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.decode.RecordDecoderFactory;
import com.fix42.dashboard.connectors.deephaven.RecordingDeephavenGateway;
import com.fix42.dashboard.connectors.source.FakeSourceFactory;
import com.fix42.dashboard.connectors.source.SourceRecord;
import com.fix42.dashboard.connectors.transform.RecordTransform;
import com.fix42.dashboard.connectors.transform.TransformRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The Deephaven-lifecycle contract: connectors follow the server up, down and back up, and every
 * "back up" is a full resubscribe so the tables rehydrate.
 */
class ConnectorManagerTest {

    private final RecordingDeephavenGateway gateway = new RecordingDeephavenGateway();
    private final FakeSourceFactory subscribers = new FakeSourceFactory();

    private ConnectorManager manager(ConnectorProperties... connectors) {
        return manager(new TransformRegistry(Map.of()), connectors);
    }

    private ConnectorManager manager(
            TransformRegistry transforms, ConnectorProperties... connectors) {
        ConnectorsProperties properties = new ConnectorsProperties();
        for (ConnectorProperties connector : connectors) {
            connector.getDeephaven().setFlushInterval(Duration.ofHours(1));
        }
        properties.setConnectors(List.of(connectors));
        return new ConnectorManager(
                properties,
                gateway,
                new RecordDecoderFactory(new ObjectMapper()),
                subscribers,
                transforms,
                Clock.fixed(Instant.parse("2024-01-15T14:30:00Z"), ZoneOffset.UTC));
    }

    @Test
    @DisplayName("connectors start once Deephaven is reachable")
    void startsConnectorsWhenDeephavenComesUp() {
        ConnectorManager manager = manager(TestConnectors.fixOrders(), TestConnectors.jsonTrades());

        manager.onGeneration(gateway.refresh());

        assertThat(manager.connectors()).allMatch(Connector::isStarted);
        assertThat(gateway.createdTables()).containsExactlyInAnyOrder("amps_orders", "amps_trades");
        assertThat(subscribers.get("orders-fix").startCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a steady server does not restart anything")
    void repeatedTicksAtTheSameGenerationAreNoOps() {
        ConnectorManager manager = manager(TestConnectors.fixOrders());

        manager.onGeneration(gateway.refresh());
        manager.onGeneration(gateway.refresh());
        manager.onGeneration(gateway.refresh());

        assertThat(subscribers.get("orders-fix").startCount()).isEqualTo(1);
        assertThat(gateway.createdTables()).containsExactly("amps_orders");
    }

    @Test
    @DisplayName("restarting Deephaven re-creates the tables and resubscribes to rehydrate them")
    void restartsConnectorsWhenDeephavenRestarts() {
        ConnectorManager manager = manager(TestConnectors.fixOrders());
        manager.onGeneration(gateway.refresh());

        manager.onGeneration(gateway.restart());

        assertThat(subscribers.get("orders-fix").startCount()).isEqualTo(2);
        assertThat(subscribers.get("orders-fix").closeCount()).isEqualTo(1);
        assertThat(gateway.createdTables()).containsExactly("amps_orders");
        assertThat(manager.connectors()).allMatch(Connector::isStarted);
    }

    @Test
    @DisplayName("losing Deephaven stops the connectors instead of publishing into the void")
    void stopsConnectorsWhenDeephavenGoesAway() {
        ConnectorManager manager = manager(TestConnectors.fixOrders());
        manager.onGeneration(gateway.refresh());

        gateway.setAvailable(false);
        manager.onGeneration(gateway.refresh());

        assertThat(manager.connectors()).noneMatch(Connector::isStarted);
        assertThat(subscribers.get("orders-fix").closeCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("connectors come back on their own when the server returns")
    void restartsAfterDeephavenComesBack() {
        ConnectorManager manager = manager(TestConnectors.fixOrders());
        manager.onGeneration(gateway.refresh());

        gateway.setAvailable(false);
        manager.onGeneration(gateway.refresh());

        gateway.setAvailable(true);
        manager.onGeneration(gateway.restart());

        assertThat(manager.connectors()).allMatch(Connector::isStarted);
        assertThat(subscribers.get("orders-fix").startCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("one connector failing to start does not hold up the others, and is retried")
    void retriesAFailingConnectorWithoutBlockingTheRest() {
        ConnectorManager manager = manager(TestConnectors.fixOrders(), TestConnectors.jsonTrades());
        subscribers.get("orders-fix").failingToStart(new IllegalStateException("broker unreachable"));

        manager.onGeneration(gateway.refresh());

        assertThat(manager.connectors()).filteredOn(c -> c.name().equals("trades-json"))
                .allMatch(Connector::isStarted);
        assertThat(manager.connectors()).filteredOn(c -> c.name().equals("orders-fix"))
                .noneMatch(Connector::isStarted);

        // The AMPS server comes back; the next tick picks the connector up without a restart.
        subscribers.get("orders-fix").failingToStart(null);
        manager.onGeneration(gateway.refresh());

        assertThat(manager.connectors()).allMatch(Connector::isStarted);
        assertThat(subscribers.get("trades-json").startCount())
                .as("the healthy connector was not restarted").isEqualTo(1);
    }

    @Test
    void disabledConnectorsAreNeverStarted() {
        ConnectorProperties disabled = TestConnectors.jsonTrades();
        disabled.setEnabled(false);
        ConnectorManager manager = manager(TestConnectors.fixOrders(), disabled);

        manager.onGeneration(gateway.refresh());

        assertThat(manager.connectors()).extracting(Connector::name).containsExactly("orders-fix");
    }

    @Test
    void closeStopsEverything() {
        ConnectorManager manager = manager(TestConnectors.fixOrders());
        manager.onGeneration(gateway.refresh());

        manager.close();

        assertThat(manager.connectors()).noneMatch(Connector::isStarted);
    }

    @Test
    void validateRejectsABadConfiguration() {
        ConnectorProperties broken = TestConnectors.fixOrders();
        broken.getDeephaven().setKeyColumns(List.of());
        ConnectorManager manager = manager(broken);

        assertThatThrownBy(manager::validate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid dh-connectors configuration")
                .hasMessageContaining("requires deephaven.key-columns");
    }

    @Test
    @DisplayName("a transform a connector names but nobody registered fails that connector")
    void unknownTransformFailsOnlyItsOwnConnector() {
        // The same treatment as an unreachable broker: the rest of the application runs, and
        // the mistake is retried (and re-logged) on every health check rather than swallowed.
        ConnectorProperties named = TestConnectors.fixOrders();
        named.setTransforms(List.of("enrich"));
        ConnectorManager manager = manager(
                new TransformRegistry(Map.of()), named, TestConnectors.jsonTrades());

        manager.onGeneration(gateway.refresh());

        assertThat(manager.connectors()).filteredOn(c -> c.name().equals("orders-fix"))
                .noneMatch(Connector::isStarted);
        assertThat(manager.connectors()).filteredOn(c -> c.name().equals("trades-json"))
                .allMatch(Connector::isStarted);
        assertThat(gateway.createdTables())
                .as("the failing connector never got as far as creating its table")
                .containsExactly("amps_trades");
    }

    @Test
    @DisplayName("a registered transform is resolved and applied in the connector's order")
    void resolvesRegisteredTransforms() {
        RecordTransform stamp = (record, fields) -> {
            Map<String, String> copy = new LinkedHashMap<>(fields);
            copy.put("55", "STAMPED");
            return copy;
        };
        ConnectorProperties named = TestConnectors.fixOrders();
        named.setTransforms(List.of("stamp"));
        ConnectorManager manager = manager(new TransformRegistry(Map.of("stamp", stamp)), named);

        manager.onGeneration(gateway.refresh());
        subscribers.get("orders-fix").deliver(
                SourceRecord.of(
                        TestConnectors.delimited("11", "C-1", "55", "AAPL"), "SOW-1"));
        manager.close();

        assertThat(gateway.rowsFor("amps_orders", "add")).singleElement()
                .satisfies(row -> assertThat(row[1]).isEqualTo("STAMPED"));
    }

    @Test
    void statusReportsEachConnector() {
        ConnectorManager manager = manager(TestConnectors.fixOrders());
        manager.onGeneration(gateway.refresh());

        assertThat(manager.status()).contains("orders-fix").contains("RUNNING").contains("amps_orders");
    }
}
