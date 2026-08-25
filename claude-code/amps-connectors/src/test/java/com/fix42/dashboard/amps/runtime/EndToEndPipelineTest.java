package com.fix42.dashboard.amps.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.config.AmpsConnectorsProperties;
import com.fix42.dashboard.amps.config.AmpsSourceProperties;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.decode.RecordDecoderFactory;
import com.fix42.dashboard.amps.deephaven.RecordingDeephavenGateway;
import com.fix42.dashboard.amps.source.AmpsSubscriberFactory;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The whole application wired as it runs, with the simulated AMPS source at one end and a
 * recording gateway at the other: config -> subscribe -> decode -> map -> batch -> publish.
 */
class EndToEndPipelineTest {

    private final RecordingDeephavenGateway gateway = new RecordingDeephavenGateway();

    private ConnectorManager manager(ConnectorProperties... connectors) {
        AmpsConnectorsProperties properties = new AmpsConnectorsProperties();
        for (ConnectorProperties connector : connectors) {
            connector.getSource().setDriver(AmpsSourceProperties.Driver.SIMULATED);
            connector.getDeephaven().setFlushInterval(Duration.ofMillis(20));
        }
        properties.setConnectors(List.of(connectors));
        ConnectorManager manager = new ConnectorManager(
                properties,
                gateway,
                new RecordDecoderFactory(new ObjectMapper()),
                new AmpsSubscriberFactory(),
                Clock.systemUTC());
        manager.validate();
        return manager;
    }

    @Test
    @DisplayName("all three formats flow end to end into their tables")
    void runsEveryFormatEndToEnd() {
        ConnectorProperties fix = TestConnectors.fixOrders();
        ConnectorProperties nvfix = TestConnectors.nvfixPositions();
        ConnectorProperties json = TestConnectors.jsonTrades();
        fix.getSource().setSimulatedKeys(4);
        nvfix.getSource().setSimulatedKeys(3);
        json.getSource().setSimulatedRate(200);

        ConnectorManager manager = manager(fix, nvfix, json);
        try {
            manager.onGeneration(gateway.refresh());

            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                assertThat(gateway.rowsFor("amps_orders", "add")).isNotEmpty();
                assertThat(gateway.rowsFor("amps_positions", "add")).isNotEmpty();
                assertThat(gateway.rowsFor("amps_trades", "add")).isNotEmpty();
            });

            assertThat(gateway.createdTables())
                    .containsExactlyInAnyOrder("amps_orders", "amps_positions", "amps_trades");
            // Every row carries exactly the configured columns, in schema order.
            assertThat(gateway.rowsFor("amps_orders", "add")).allSatisfy(row ->
                    assertThat(row).hasSize(5));
            assertThat(gateway.rowsFor("amps_trades", "add")).allSatisfy(row ->
                    assertThat(row).hasSize(4));
        } finally {
            manager.close();
        }
    }

    @Test
    @DisplayName("a SOW replay publishes one row per key before any live update")
    void sowReplayRehydratesTheKeyedTable() {
        ConnectorProperties fix = TestConnectors.fixOrders();
        fix.getSource().setSimulatedKeys(6);
        fix.getSource().setSimulatedRate(1);

        ConnectorManager manager = manager(fix);
        try {
            manager.onGeneration(gateway.refresh());

            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(gateway.rowsFor("amps_orders", "add")).hasSizeGreaterThanOrEqualTo(6));

            assertThat(gateway.rowsFor("amps_orders", "add").subList(0, 6))
                    .extracting(row -> row[0])
                    .doesNotHaveDuplicates();
        } finally {
            manager.close();
        }
    }

    @Test
    @DisplayName("a Deephaven restart replays the SOW again into the re-created table")
    void deephavenRestartRehydratesFromScratch() {
        ConnectorProperties fix = TestConnectors.fixOrders();
        fix.getSource().setSimulatedKeys(5);
        fix.getSource().setSimulatedRate(1);

        ConnectorManager manager = manager(fix);
        try {
            manager.onGeneration(gateway.refresh());
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(gateway.rowsFor("amps_orders", "add")).hasSizeGreaterThanOrEqualTo(5));
            int beforeRestart = gateway.rowsFor("amps_orders", "add").size();

            manager.onGeneration(gateway.restart());

            assertThat(gateway.createdTables())
                    .as("the table is re-created after the restart").containsExactly("amps_orders");
            Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertThat(gateway.rowsFor("amps_orders", "add"))
                            .hasSizeGreaterThanOrEqualTo(beforeRestart + 5));
        } finally {
            manager.close();
        }
    }
}
