package com.fix42.dashboard.amps.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.amps.TestConnectors;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConnectorValidatorTest {

    @Test
    void acceptsTheExampleConnectors() {
        assertThat(ConnectorValidator.validate(TestConnectors.fixOrders())).isEmpty();
        assertThat(ConnectorValidator.validate(TestConnectors.nvfixPositions())).isEmpty();
        assertThat(ConnectorValidator.validate(TestConnectors.jsonTrades())).isEmpty();
    }

    @Test
    void acceptsTheBlinkAndRingConnectors() {
        assertThat(ConnectorValidator.validate(TestConnectors.jsonTicks())).isEmpty();
        assertThat(ConnectorValidator.validate(TestConnectors.jsonTicksRing(5_000))).isEmpty();
    }

    @Test
    @DisplayName("a SOW topic defaults to KEYED, which needs key columns")
    void requiresKeyColumnsForSowTopics() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getDeephaven().setKeyColumns(List.of());
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("table-type=KEYED requires deephaven.key-columns")
                        && error.contains("source.sow=true defaults"));
    }

    @Test
    @DisplayName("a journal topic defaults to APPEND_ONLY, which cannot have key columns")
    void rejectsKeyColumnsForJournalTopics() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getDeephaven().setKeyColumns(List.of("TradeID"));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("key-columns is only meaningful for "
                        + "table-type=KEYED, but this connector resolves to APPEND_ONLY"));
    }

    @Test
    @DisplayName("an explicit table-type overrides what the topic implies")
    void allowsAKeyedTableOverAJournalTopic() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getDeephaven().setTableType(DeephavenTableType.KEYED);
        connector.getDeephaven().setKeyColumns(List.of("TradeID"));
        assertThat(ConnectorValidator.validate(connector)).isEmpty();
    }

    @Test
    @DisplayName("a SOW topic may be rendered as a blink table, without keys")
    void allowsABlinkTableOverASowTopic() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getDeephaven().setTableType(DeephavenTableType.BLINK);
        connector.getDeephaven().setKeyColumns(List.of());
        assertThat(ConnectorValidator.validate(connector)).isEmpty();
    }

    @Test
    @DisplayName("keys on a table that has none is a configuration with no meaning")
    void rejectsKeyColumnsOnABlinkTable() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getDeephaven().setTableType(DeephavenTableType.RING);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("resolves to RING"));
    }

    @Test
    @DisplayName("a delta subscription published in FULL mode would blank the omitted columns")
    void rejectsDeltaSubscriptionWithFullPublish() {
        ConnectorProperties connector = TestConnectors.nvfixPositions();
        connector.getDeephaven().setPublishMode(UpdateMode.FULL);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("needs deephaven.publish-mode=DELTA"));
    }

    @Test
    void rejectsDeltaPublishIntoAnAppendOnlyTable() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getDeephaven().setPublishMode(UpdateMode.DELTA);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("publish-mode=DELTA requires a keyed table"));
    }

    @Test
    @DisplayName("a blink table with the bootstrap turned off could never be published into")
    void rejectsPublisherBackedTypesWithoutCreateIfMissing() {
        ConnectorProperties connector = TestConnectors.jsonTicks();
        connector.getDeephaven().setCreateIfMissing(false);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("table-type=BLINK requires deephaven.create-if-missing"));

        ConnectorProperties inputTable = TestConnectors.jsonTrades();
        inputTable.getDeephaven().setCreateIfMissing(false);
        assertThat(ConnectorValidator.validate(inputTable)).isEmpty();
    }

    @Test
    @DisplayName("there is nowhere to merge a delta into a blink table either")
    void rejectsDeltaPublishIntoABlinkTable() {
        ConnectorProperties connector = TestConnectors.nvfixPositions();
        connector.getDeephaven().setTableType(DeephavenTableType.BLINK);
        connector.getDeephaven().setKeyColumns(List.of());
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("publish-mode=DELTA requires a keyed table"));
    }

    @Test
    void rejectsDeltaSubscriptionOnAJournalTopic() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getSource().setSubscriptionMode(UpdateMode.DELTA);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("subscription-mode=DELTA requires a SOW topic"));
    }

    @Test
    void rejectsKeyColumnsThatAreNotMapped() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getDeephaven().setKeyColumns(List.of("NotMapped"));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("key column 'NotMapped' is not one of the mapped columns"));
    }

    @Test
    void rejectsDuplicateTagsAndColumns() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.setFields(List.of(
                TestConnectors.field("11", "ClOrdID", ColumnType.STRING),
                TestConnectors.field("11", "Duplicate", ColumnType.STRING),
                TestConnectors.field("55", "ClOrdID", ColumnType.STRING)));
        List<String> errors = ConnectorValidator.validate(connector);
        assertThat(errors).anyMatch(error -> error.contains("duplicate source tag '11'"));
        assertThat(errors).anyMatch(error -> error.contains("duplicate deephaven column 'ClOrdID'"));
    }

    @Test
    void rejectsNonNumericFixTags() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.setFields(List.of(TestConnectors.field("ClOrdID", "ClOrdID", ColumnType.STRING)));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("is not a tag number"));
    }

    @Test
    @DisplayName("the table name is interpolated into python, so it must be an identifier")
    void rejectsTableNamesThatAreNotIdentifiers() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getDeephaven().setTable("amps orders; import os");
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("is not a valid identifier"));
    }

    @Test
    void rejectsColumnNamesThatAreNotIdentifiers() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.setFields(List.of(TestConnectors.field("11", "Cl OrdID", ColumnType.STRING)));
        connector.getDeephaven().setKeyColumns(List.of("Cl OrdID"));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("'Cl OrdID' is not a valid identifier"));
    }

    @Test
    void rejectsSyntheticColumnsThatCollideWithMappedOnes() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getDeephaven().setSowKeyColumn("Symbol");
        connector.getDeephaven().setIngestTimestampColumn("Price");
        List<String> errors = ConnectorValidator.validate(connector);
        assertThat(errors).anyMatch(error -> error.contains("sow-key-column 'Symbol' collides"));
        assertThat(errors).anyMatch(error -> error.contains("ingest-timestamp-column 'Price' collides"));
    }

    @Test
    void rejectsDuplicateConnectorNames() {
        AmpsConnectorsProperties properties = new AmpsConnectorsProperties();
        properties.setConnectors(List.of(TestConnectors.fixOrders(), TestConnectors.fixOrders()));
        assertThat(ConnectorValidator.validate(properties))
                .anyMatch(error -> error.contains("duplicate connector name: orders-fix"));
    }
}
