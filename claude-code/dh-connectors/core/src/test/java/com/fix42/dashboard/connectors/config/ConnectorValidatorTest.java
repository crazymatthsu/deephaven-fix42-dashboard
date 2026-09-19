package com.fix42.dashboard.connectors.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.connectors.TestConnectors;
import java.util.List;
import java.util.Map;
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
                        && error.contains("a stateful source defaults"));
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
    void acceptsTheValueShapingExample() {
        assertThat(ConnectorValidator.validate(TestConnectors.fixShapedOrders())).isEmpty();
    }

    @Test
    @DisplayName("a built-in decode publishes a name, so it needs a STRING column")
    void rejectsADecodeOnANonStringColumn() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        FieldMapping qty = connector.getFields().get(2);
        qty.setDecode(FixValueDecode.SIDE);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("decode=SIDE publishes a name, so type must be "
                        + "STRING, not DOUBLE"));
    }

    @Test
    @DisplayName("an inline values map carries no such restriction")
    void allowsInlineValuesOnANonStringColumn() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getFields().get(2).setValues(Map.of("Y", "1", "N", "0"));
        assertThat(ConnectorValidator.validate(connector)).isEmpty();
    }

    @Test
    @DisplayName("a default that does not coerce is listed, not thrown")
    void rejectsADefaultThatDoesNotCoerce() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getFields().get(2).setDefaultValue("not-a-number");
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("column 'OrderQty': default-value")
                        && error.contains("not a valid DOUBLE"));
    }

    @Test
    @DisplayName("a defaulted key column would collapse every keyless record onto one row")
    void rejectsADefaultOnAKeyColumn() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getFields().get(0).setDefaultValue("DUMMY");
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("column 'ClOrdID' is a key column, so it must "
                        + "not set default-value"));
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
        connector.getSource().getAmps().setSubscriptionMode(UpdateMode.DELTA);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains(
                        "source.amps.subscription-mode=DELTA requires a SOW topic"));
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
        connector.getDeephaven().setSourceKeyColumn("Symbol");
        connector.getDeephaven().setIngestTimestampColumn("Price");
        List<String> errors = ConnectorValidator.validate(connector);
        assertThat(errors).anyMatch(error -> error.contains("source-key-column 'Symbol' collides"));
        assertThat(errors).anyMatch(error -> error.contains("ingest-timestamp-column 'Price' collides"));
    }

    @Test
    void acceptsTheCompositeAndExplodeExamples() {
        assertThat(ConnectorValidator.validate(TestConnectors.compositeOrders())).isEmpty();
        assertThat(ConnectorValidator.validate(TestConnectors.jsonPortfolios())).isEmpty();
    }

    @Test
    void rejectsCompositeWithoutParts() {
        ConnectorProperties connector = TestConnectors.compositeOrders();
        connector.setCompositeParts(List.of());
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("format=COMPOSITE requires composite-parts"));
    }

    @Test
    void rejectsNestedCompositeParts() {
        ConnectorProperties connector = TestConnectors.compositeOrders();
        connector.setCompositeParts(List.of(SourceFormat.JSON, SourceFormat.COMPOSITE));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("cannot themselves be COMPOSITE"));
    }

    @Test
    @DisplayName("'composite' is not an AMPS type name; the registered name must be given")
    void requiresAMessageTypeForComposite() {
        ConnectorProperties connector = TestConnectors.compositeOrders();
        connector.getSource().getAmps().setMessageType(null);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("requires source.amps.message-type"));

        connector.getSource().getAmps().setUri("tcp://localhost:9007/amps/my-composite");
        assertThat(ConnectorValidator.validate(connector)).isEmpty();
    }

    @Test
    void rejectsCompositePartsOnOtherFormats() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.setCompositeParts(List.of(SourceFormat.JSON));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("only meaningful for format=COMPOSITE"));
    }

    @Test
    void rejectsATagBeyondTheDeclaredParts() {
        ConnectorProperties connector = TestConnectors.compositeOrders();
        connector.getFields().get(0).setTag("2.orderId");
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("references part 2")
                        && error.contains("declares only 2"));
    }

    @Test
    void rejectsANonNumericTagIntoAFixPart() {
        ConnectorProperties connector = TestConnectors.compositeOrders();
        connector.getFields().get(2).setTag("1.Side");
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("part 1 is FIX")
                        && error.contains("'Side' is not a tag number"));
    }

    @Test
    void rejectsExplodeOnADelimitedFormat() {
        ConnectorProperties connector = TestConnectors.jsonPortfolios();
        connector.setFormat(SourceFormat.NVFIX);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("explode requires a JSON object"));
    }

    @Test
    void rejectsExplodeUnderADeltaSubscription() {
        ConnectorProperties connector = TestConnectors.jsonPortfolios();
        connector.getSource().getAmps().setSubscriptionMode(UpdateMode.DELTA);
        connector.getDeephaven().setPublishMode(UpdateMode.DELTA);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains(
                        "explode requires source.amps.subscription-mode=FULL"));
    }

    @Test
    @DisplayName("a keyed explode target must key on the member name")
    void requiresTheExplodeKeyColumnAmongTheKeys() {
        ConnectorProperties connector = TestConnectors.jsonPortfolios();
        connector.getDeephaven().setKeyColumns(List.of("OuterKey"));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains(
                        "key-columns must include explode.key-column 'Symbol'"));
    }

    @Test
    void rejectsExplodeColumnCollisions() {
        ConnectorProperties connector = TestConnectors.jsonPortfolios();
        connector.getExplode().setKeyColumn("OuterKey");
        connector.getDeephaven().setKeyColumns(List.of("OuterKey"));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains(
                        "explode.key-column 'OuterKey' collides"));
    }

    @Test
    void explodeTagMustSitInAJsonPart() {
        ConnectorProperties connector = TestConnectors.compositeOrders();
        ExplodeProperties explode = new ExplodeProperties();
        explode.setTag("1.value");
        explode.setKeyColumn("Member");
        connector.setExplode(explode);
        connector.getDeephaven().setKeyColumns(List.of("OrderId", "Member"));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("must sit in a JSON part")
                        && error.contains("part 1 is FIX"));
    }

    // ---- which transport, and what each transport can back -------------------------

    @Test
    @DisplayName("a connector with no transport block names no feed at all")
    void rejectsASourceWithNoTransportBlock() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getSource().setAmps(null);
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains(
                        "source needs exactly one of amps/kafka/tcp, and has none"));
    }

    @Test
    @DisplayName("one table is fed by one source, so two blocks is a configuration mistake")
    void rejectsTwoTransportBlocks() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getSource().setKafka(kafka("orders", false));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("source configures [amps, kafka]")
                        && error.contains("keep exactly one"));
    }

    @Test
    @DisplayName("the demo's SIMULATED connectors keep their transport block and its rules")
    void theSimulatedDriverDoesNotExemptTheTransportRules() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        assertThat(connector.getSource().getDriver())
                .isEqualTo(SourceProperties.Driver.SIMULATED);
        assertThat(ConnectorValidator.validate(connector)).isEmpty();

        connector.getSource().setAmps(null);
        assertThat(ConnectorValidator.validate(connector)).isNotEmpty();
    }

    @Test
    @DisplayName("composite message types are an AMPS feature, so they name the amps block")
    void rejectsCompositeWithoutTheAmpsBlock() {
        ConnectorProperties connector = TestConnectors.compositeOrders();
        connector.getSource().setAmps(null);
        connector.getSource().setKafka(kafka("orders.composite", false));
        List<String> errors = ConnectorValidator.validate(connector);
        assertThat(errors).anyMatch(error ->
                error.contains("format=COMPOSITE requires source.amps"));
        assertThat(errors).anyMatch(error ->
                error.contains("composite-parts requires source.amps"));
    }

    @Test
    @DisplayName("subscription-mode lives on the amps block, so no other source can ask for it")
    void deltaSubscriptionIsStructurallyAmpsOnly() {
        ConnectorProperties connector = TestConnectors.nvfixPositions();
        assertThat(connector.getSource().subscriptionMode()).isEqualTo(UpdateMode.DELTA);

        // Move the same connector to Kafka: the DELTA request goes with the block it was
        // written in rather than being silently carried over onto a transport that has no
        // such thing -- which is why there is no rule to fire here.
        connector.getSource().setAmps(null);
        connector.getSource().setKafka(kafka("Positions", true));
        connector.getDeephaven().setPublishMode(UpdateMode.FULL);
        assertThat(connector.getSource().subscriptionMode()).isEqualTo(UpdateMode.FULL);
        assertThat(ConnectorValidator.validate(connector))
                .noneMatch(error -> error.contains("subscription-mode"));
    }

    @Test
    @DisplayName("a TCP stream carries no per-message key, so there is none to publish")
    void rejectsSourceKeyColumnOnTcp() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getSource().setAmps(null);
        connector.getSource().setTcp(tcp());
        connector.getDeephaven().setSourceKeyColumn("SourceKey");
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains(
                        "deephaven.source-key-column is not available for source.tcp"));
    }

    @Test
    @DisplayName("source-key-column is fine over amps or kafka")
    void allowsSourceKeyColumnOnAmpsAndKafka() {
        ConnectorProperties amps = TestConnectors.fixOrders();
        amps.getDeephaven().setSourceKeyColumn("SourceKey");
        assertThat(ConnectorValidator.validate(amps)).isEmpty();

        ConnectorProperties viaKafka = TestConnectors.jsonTrades();
        viaKafka.getSource().setAmps(null);
        viaKafka.getSource().setKafka(kafka("Trades", false));
        viaKafka.getDeephaven().setSourceKeyColumn("SourceKey");
        assertThat(ConnectorValidator.validate(viaKafka)).isEmpty();
    }

    @Test
    @DisplayName("a tombstone carries no payload, so a compacted topic must key on the key")
    void compactedKafkaMustKeyOnTheSourceKey() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getSource().setAmps(null);
        connector.getSource().setKafka(kafka("Trades", true));
        connector.getDeephaven().setSourceKeyColumn("SourceKey");
        connector.getDeephaven().setKeyColumns(List.of("TradeID"));
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("source.kafka.compacted=true with a keyed "
                        + "table requires deephaven.key-columns to include source-key-column"));

        connector.getDeephaven().setKeyColumns(List.of("SourceKey"));
        assertThat(ConnectorValidator.validate(connector)).isEmpty();
    }

    @Test
    @DisplayName("a compacted Kafka topic is state, so it defaults to a keyed table")
    void compactedKafkaDefaultsToKeyed() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getSource().setAmps(null);
        connector.getSource().setKafka(kafka("Trades", true));
        assertThat(connector.getSource().stateful()).isTrue();
        assertThat(ConnectorValidator.validate(connector))
                .anyMatch(error -> error.contains("table-type=KEYED requires deephaven.key-columns")
                        && error.contains("a stateful source defaults"));
    }

    @Test
    @DisplayName("a TCP stream is not state, but its rows can still be keyed on payload fields")
    void tcpIsNeverStatefulButMayStillBeKeyed() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getSource().setAmps(null);
        connector.getSource().setTcp(tcp());
        assertThat(connector.getSource().stateful()).isFalse();
        connector.getDeephaven().setTableType(DeephavenTableType.KEYED);
        connector.getDeephaven().setKeyColumns(List.of("TradeID"));
        assertThat(ConnectorValidator.validate(connector)).isEmpty();
    }

    // ---- transforms ------------------------------------------------------------------

    @Test
    void acceptsNamedTransforms() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.setTransforms(List.of("enrich", "redact"));
        assertThat(ConnectorValidator.validate(connector)).isEmpty();
    }

    @Test
    @DisplayName("a transform is stateless, so naming it twice is a slip either way")
    void rejectsBlankAndDuplicateTransformNames() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.setTransforms(List.of("enrich", "enrich", " "));
        List<String> errors = ConnectorValidator.validate(connector);
        assertThat(errors).anyMatch(error -> error.contains("duplicate transform 'enrich'"));
        assertThat(errors).anyMatch(error -> error.contains("transforms contains a blank entry"));
    }

    // ---- helpers ---------------------------------------------------------------------

    private static KafkaSourceProperties kafka(String topic, boolean compacted) {
        KafkaSourceProperties kafka = new KafkaSourceProperties();
        kafka.setBootstrapServers("localhost:9092");
        kafka.setTopic(topic);
        kafka.setCompacted(compacted);
        return kafka;
    }

    private static TcpSourceProperties tcp() {
        TcpSourceProperties tcp = new TcpSourceProperties();
        tcp.setHost("localhost");
        tcp.setPort(5001);
        return tcp;
    }

    @Test
    void rejectsDuplicateConnectorNames() {
        ConnectorsProperties properties = new ConnectorsProperties();
        properties.setConnectors(List.of(TestConnectors.fixOrders(), TestConnectors.fixOrders()));
        assertThat(ConnectorValidator.validate(properties))
                .anyMatch(error -> error.contains("duplicate connector name: orders-fix"));
    }
}
