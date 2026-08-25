package com.fix42.dashboard.amps.decode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JsonRecordDecoderTest {

    private final JsonRecordDecoder decoder = new JsonRecordDecoder(new ObjectMapper());

    @Test
    void decodesScalarMembers() {
        Map<String, String> fields = decoder.decode(
                "{\"tradeId\":\"T-1\",\"quantity\":100,\"price\":185.5,\"busted\":false}");
        assertThat(fields)
                .containsEntry("tradeId", "T-1")
                .containsEntry("quantity", "100")
                .containsEntry("price", "185.5")
                .containsEntry("busted", "false");
    }

    @Test
    @DisplayName("nested members are addressable by dotted path")
    void flattensNestedObjects() {
        Map<String, String> fields = decoder.decode(
                "{\"tradeId\":\"T-1\",\"execution\":{\"venue\":\"XNAS\",\"broker\":\"B1\"}}");
        assertThat(fields)
                .containsEntry("execution.venue", "XNAS")
                .containsEntry("execution.broker", "B1");
    }

    @Test
    @DisplayName("a leaf name also resolves when it is unambiguous")
    void registersBareLeafNames() {
        assertThat(decoder.decode("{\"execution\":{\"venue\":\"XNAS\"}}"))
                .containsEntry("venue", "XNAS");
    }

    @Test
    @DisplayName("a shallower field keeps the bare name when a deeper one shares it")
    void shallowFieldWinsTheBareName() {
        Map<String, String> fields = decoder.decode(
                "{\"venue\":\"OUTER\",\"execution\":{\"venue\":\"INNER\"}}");
        assertThat(fields)
                .containsEntry("venue", "OUTER")
                .containsEntry("execution.venue", "INNER");
    }

    @Test
    @DisplayName("an explicit JSON null is present with no value -- a delta clearing the field")
    void decodesExplicitNullAsPresentAndEmpty() {
        Map<String, String> fields = decoder.decode("{\"venue\":null}");
        assertThat(fields).containsKey("venue");
        assertThat(fields.get("venue")).isEmpty();
    }

    @Test
    void keepsArraysAsJsonText() {
        assertThat(decoder.decode("{\"tags\":[\"a\",\"b\"]}"))
                .containsEntry("tags", "[\"a\",\"b\"]");
    }

    @Test
    void handlesEmptyAndNullPayloads() {
        assertThat(decoder.decode("")).isEmpty();
        assertThat(decoder.decode(null)).isEmpty();
    }

    @Test
    void rejectsMalformedJson() {
        assertThatThrownBy(() -> decoder.decode("{not json"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid JSON");
    }

    @Test
    void rejectsNonObjectPayloads() {
        assertThatThrownBy(() -> decoder.decode("[1,2,3]"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be an object");
    }
}
