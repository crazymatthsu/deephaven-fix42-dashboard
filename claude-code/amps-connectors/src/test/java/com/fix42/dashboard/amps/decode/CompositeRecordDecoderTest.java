package com.fix42.dashboard.amps.decode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.SourceFormat;
import com.fix42.dashboard.amps.source.AmpsRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Composite message types: part-indexed tags, doc 07 section 5.3. */
class CompositeRecordDecoderTest {

    private final RecordDecoder decoder =
            new RecordDecoderFactory(new ObjectMapper()).create(TestConnectors.compositeOrders());

    private static AmpsRecord record(String... parts) {
        return AmpsRecord.composite(List.of(parts), "SOW-1", AmpsRecord.Action.UPSERT);
    }

    @Test
    @DisplayName("each part decodes in its own format, under part-indexed tags")
    void decodesEachPartInItsOwnFormat() {
        Map<String, String> fields = decoder.decode(record(
                "{\"orderId\":\"O-1\",\"account\":\"ACC-9\"}",
                TestConnectors.delimited("54", "1", "38", "250")));

        assertThat(fields)
                .containsEntry("0.orderId", "O-1")
                .containsEntry("0.account", "ACC-9")
                .containsEntry("1.54", "1")
                .containsEntry("1.38", "250");
    }

    @Test
    @DisplayName("bare tags alias the merged namespace, the composite-global spelling")
    void bareTagsAliasTheMergedNamespace() {
        Map<String, String> fields = decoder.decode(record(
                "{\"orderId\":\"O-1\"}", TestConnectors.delimited("54", "2")));

        assertThat(fields).containsEntry("orderId", "O-1").containsEntry("54", "2");
    }

    @Test
    @DisplayName("when two parts carry the same tag, the first part keeps the bare alias")
    void firstPartWinsAContestedBareTag() {
        RecordDecoder twoJson = new CompositeRecordDecoder(List.of(
                new JsonRecordDecoder(new ObjectMapper()),
                new JsonRecordDecoder(new ObjectMapper())));

        Map<String, String> fields = twoJson.decode(record(
                "{\"id\":\"first\"}", "{\"id\":\"second\"}"));

        assertThat(fields)
                .containsEntry("0.id", "first")
                .containsEntry("1.id", "second")
                .containsEntry("id", "first");
    }

    @Test
    @DisplayName("a message with fewer parts than configured just lacks those fields")
    void fewerPartsThanConfiguredMeansAbsentFields() {
        Map<String, String> fields = decoder.decode(record("{\"orderId\":\"O-1\"}"));

        assertThat(fields).containsEntry("0.orderId", "O-1");
        assertThat(fields).doesNotContainKey("1.54");
    }

    @Test
    @DisplayName("parts beyond the configured list are ignored, like any unmapped field")
    void extraPartsAreIgnored() {
        Map<String, String> fields = decoder.decode(record(
                "{\"orderId\":\"O-1\"}",
                TestConnectors.delimited("54", "1"),
                "{\"unconfigured\":\"part\"}"));

        assertThat(fields).containsEntry("0.orderId", "O-1").containsEntry("1.54", "1");
        assertThat(fields.keySet()).noneMatch(tag -> tag.startsWith("2."));
        assertThat(fields).doesNotContainKey("unconfigured");
    }

    @Test
    @DisplayName("a malformed part names its index")
    void malformedPartNamesItsIndex() {
        assertThatThrownBy(() -> decoder.decode(record("not json",
                TestConnectors.delimited("54", "1"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("part 0");
    }

    @Test
    @DisplayName("a composite payload never arrives as one string")
    void refusesASingleStringPayload() {
        assertThatThrownBy(() -> decoder.decode("anything"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parts");
    }

    @Test
    @DisplayName("a record with no parts (an empty delete body) decodes to no fields")
    void noPartsDecodesToNothing() {
        assertThat(decoder.decode(AmpsRecord.composite(List.of(), "SOW-1",
                AmpsRecord.Action.DELETE))).isEmpty();
        assertThat(decoder.decode(AmpsRecord.delete(null, "SOW-1"))).isEmpty();
    }

    @Test
    @DisplayName("the factory refuses a nested COMPOSITE part")
    void refusesNestedComposite() {
        ConnectorProperties connector = TestConnectors.compositeOrders();
        connector.setCompositeParts(List.of(SourceFormat.COMPOSITE));
        assertThatThrownBy(() -> new RecordDecoderFactory(new ObjectMapper()).create(connector))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
