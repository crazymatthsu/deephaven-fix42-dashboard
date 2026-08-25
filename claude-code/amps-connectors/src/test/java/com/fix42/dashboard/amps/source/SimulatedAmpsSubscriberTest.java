package com.fix42.dashboard.amps.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.decode.DelimitedRecordDecoder;
import com.fix42.dashboard.amps.decode.JsonRecordDecoder;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SimulatedAmpsSubscriberTest {

    @Test
    @DisplayName("a SOW topic replays one record per key before any live update")
    void replaysTheSowOnStart() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getSource().setSimulatedKeys(5);
        connector.getSource().setSimulatedRate(1);

        List<AmpsRecord> received = new ArrayList<>();
        try (SimulatedAmpsSubscriber subscriber = new SimulatedAmpsSubscriber(connector)) {
            subscriber.start(received::add);
            assertThat(received).hasSize(5);
            assertThat(received).extracting(AmpsRecord::sowKey)
                    .containsExactly("SIM-0", "SIM-1", "SIM-2", "SIM-3", "SIM-4");
        }
    }

    @Test
    @DisplayName("a journal topic has no SOW to replay")
    void journalTopicsDoNotReplayOnStart() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getSource().setSimulatedRate(1);

        List<AmpsRecord> received = new ArrayList<>();
        try (SimulatedAmpsSubscriber subscriber = new SimulatedAmpsSubscriber(connector)) {
            subscriber.start(received::add);
            assertThat(received).isEmpty();
        }
    }

    @Test
    @DisplayName("generated FIX payloads decode back to every configured tag")
    void generatesDecodableFixPayloads() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        String payload = new SimulatedAmpsSubscriber(connector).encode(3, 7);

        var fields = new DelimitedRecordDecoder(connector.getSource().getFieldSeparator())
                .decode(payload);
        assertThat(fields).containsKeys("11", "55", "38", "44", "60");
        assertThat(Double.parseDouble(fields.get("38"))).isPositive();
    }

    @Test
    @DisplayName("generated JSON resolves every configured tag, dotted paths included")
    void generatesDecodableJsonPayloads() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        String payload = new SimulatedAmpsSubscriber(connector).encode(2, 4);

        var fields = new JsonRecordDecoder(new ObjectMapper()).decode(payload);
        // Exactly the tags the connector maps -- otherwise a column silently publishes null.
        assertThat(fields).containsKeys(
                connector.getFields().stream().map(f -> f.getTag()).toArray(String[]::new));
        assertThat(Long.parseLong(fields.get("quantity"))).isPositive();
    }

    @Test
    @DisplayName("a dotted tag nests in the generated document")
    void nestsDottedJsonTags() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        String payload = new SimulatedAmpsSubscriber(connector).encode(1, 1);

        assertThat(payload).contains("\"execution\":{\"venue\":");
        assertThat(new JsonRecordDecoder(new ObjectMapper()).decode(payload))
                .containsKey("execution.venue");
    }

    @Test
    @DisplayName("generated FIX/NVFIX payloads resolve every configured tag too")
    void generatesDecodableNvfixPayloads() {
        ConnectorProperties connector = TestConnectors.nvfixPositions();
        String payload = new SimulatedAmpsSubscriber(connector).encode(1, 1);

        var fields = new DelimitedRecordDecoder(connector.getSource().getFieldSeparator())
                .decode(payload);
        assertThat(fields).containsKeys(
                connector.getFields().stream().map(f -> f.getTag()).toArray(String[]::new));
    }

    @Test
    void isConnectedOnlyWhileRunning() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        SimulatedAmpsSubscriber subscriber = new SimulatedAmpsSubscriber(connector);
        assertThat(subscriber.isConnected()).isFalse();
        subscriber.start(record -> { });
        assertThat(subscriber.isConnected()).isTrue();
        subscriber.close();
        assertThat(subscriber.isConnected()).isFalse();
    }
}
