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
import com.fix42.dashboard.amps.decode.RecordDecoder;
import com.fix42.dashboard.amps.mapping.FieldMapper;
import com.fix42.dashboard.amps.mapping.MappedRow;
import com.fix42.dashboard.amps.mapping.TableSchema;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import com.fix42.dashboard.amps.config.ColumnType;
import com.fix42.dashboard.amps.config.FieldMapping;
import com.fix42.dashboard.amps.config.FixValueDecode;
import java.util.Map;
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

    /**
     * The records a SOW replay actually produces.
     *
     * <p>Deliberately not {@code encode(key, tick)} with values of the test's choosing: the
     * runtime derives {@code key} from the same counter as {@code tick}, and calling encode
     * directly hides the correlation that gives -- which is exactly how an index of
     * {@code key + tick} looked fine here while addressing only half a table in the demo.
     */
    private static List<Map<String, String>> replay(ConnectorProperties connector) {
        List<Map<String, String>> records = new ArrayList<>();
        RecordDecoder decoder = new DelimitedRecordDecoder(TestConnectors.SOH);
        SimulatedAmpsSubscriber source = new SimulatedAmpsSubscriber(connector);
        source.start(record -> records.add(decoder.decode(record.data())));
        source.close();
        return records;
    }

    @Test
    @DisplayName("a decoded field is given a code the mapping can actually decode")
    void generatesCodesForDecodedFields() {
        ConnectorProperties connector = TestConnectors.fixShapedOrders();
        connector.getSource().setSimulatedKeys(8);
        TableSchema schema = TableSchema.of(connector);
        FieldMapper mapper = new FieldMapper(schema);

        Set<Object> sides = new HashSet<>();
        for (Map<String, String> fields : replay(connector)) {
            sides.add(mapper.map(AmpsRecord.of("p"), fields, Instant.EPOCH)
                    .values()[schema.indexOf("Side")]);
        }
        // Decoded names, not the "Side-N" a plain STRING field would have produced, and more
        // than one of them -- an index correlated with the key count would deliver far fewer.
        // Not specific names: a replay of N keys visits N of the table's entries, not all.
        assertThat(sides).hasSizeGreaterThan(2)
                .allSatisfy(side -> assertThat(FixValueDecode.SIDE.table().values())
                        .contains(String.valueOf(side)));
    }

    @Test
    @DisplayName("an even key count still cycles a two-entry table through both values")
    void cyclesSmallTablesEvenlyForAnEvenKeyCount() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getSource().setSow(true);
        connector.getSource().setSimulatedKeys(30);
        connector.getDeephaven().setKeyColumns(List.of("TradeID"));
        FieldMapping side = TestConnectors.field("side", "Side", ColumnType.STRING);
        side.setValues(Map.of("B", "BUY", "S", "SELL"));
        connector.setFields(List.of(
                TestConnectors.field("tradeId", "TradeID", ColumnType.STRING), side));

        List<Map<String, String>> records = new ArrayList<>();
        RecordDecoder decoder = new JsonRecordDecoder(new ObjectMapper());
        SimulatedAmpsSubscriber source = new SimulatedAmpsSubscriber(connector);
        source.start(record -> records.add(decoder.decode(record.data())));
        source.close();

        assertThat(records).extracting(fields -> fields.get("side"))
                .as("both codes appear across a replay")
                .contains("B", "S");
    }

    @Test
    @DisplayName("a defaulted field is left out often enough for the default to show")
    void omitsDefaultedFieldsSometimes() {
        ConnectorProperties connector = TestConnectors.fixShapedOrders();
        connector.getSource().setSimulatedKeys(8);

        List<Map<String, String>> records = replay(connector);
        long withoutAccount = records.stream().filter(f -> !f.containsKey("1")).count();

        assertThat(withoutAccount).as("some records omit the defaulted tag").isPositive();
        assertThat(withoutAccount).as("but not all of them").isLessThan(records.size());
    }

    @Test
    @DisplayName("a composite connector emits parts, each decodable in its own format")
    void compositeRecordsCarryDecodableParts() {
        ConnectorProperties connector = TestConnectors.compositeOrders();
        connector.getSource().setSimulatedKeys(4);

        List<AmpsRecord> records = new ArrayList<>();
        try (SimulatedAmpsSubscriber source = new SimulatedAmpsSubscriber(connector)) {
            source.start(records::add);
        }
        RecordDecoder decoder = new com.fix42.dashboard.amps.decode.RecordDecoderFactory(
                new ObjectMapper()).create(connector);

        assertThat(records).hasSize(4);
        for (AmpsRecord record : records) {
            assertThat(record.data()).isNull();
            assertThat(record.parts()).hasSize(2);
            Map<String, String> fields = decoder.decode(record);
            assertThat(fields).containsKeys("0.orderId", "0.account", "1.54", "1.38");
        }
    }

    @Test
    @DisplayName("an explode connector gets an object whose membership shifts across ticks")
    void explodeObjectsCycleMembership() {
        ConnectorProperties connector = TestConnectors.jsonPortfolios();
        connector.getSource().setSimulatedKeys(10);

        List<Map<String, String>> records = new ArrayList<>();
        RecordDecoder decoder = new JsonRecordDecoder(new ObjectMapper());
        try (SimulatedAmpsSubscriber source = new SimulatedAmpsSubscriber(connector)) {
            source.start(record -> records.add(decoder.decode(record.data())));
        }

        Set<String> memberSets = new HashSet<>();
        for (Map<String, String> fields : records) {
            String value = fields.get("value");
            assertThat(value).as("the explode tag carries an object").startsWith("{");
            memberSets.add(value.replaceAll("[^A-Z,]", ""));
            assertThat(fields.keySet())
                    .as("members nest inside the object")
                    .anyMatch(tag -> tag.startsWith("value."));
        }
        assertThat(memberSets.size())
                .as("membership varies, so vanish-deletes have something to do")
                .isGreaterThan(1);
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
