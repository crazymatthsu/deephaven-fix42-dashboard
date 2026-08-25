package com.fix42.dashboard.amps.decode;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.amps.TestConnectors;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DelimitedRecordDecoderTest {

    private final DelimitedRecordDecoder decoder = new DelimitedRecordDecoder(TestConnectors.SOH);

    @Test
    void decodesFixTagValuePairs() {
        Map<String, String> fields = decoder.decode(
                TestConnectors.delimited("11", "C-1", "55", "AAPL", "38", "100"));
        assertThat(fields).containsExactly(
                Map.entry("11", "C-1"), Map.entry("55", "AAPL"), Map.entry("38", "100"));
    }

    @Test
    @DisplayName("NVFIX is the same shape with names instead of tag numbers")
    void decodesNvfixNameValuePairs() {
        assertThat(decoder.decode(TestConnectors.delimited("Account", "ACC-2", "Symbol", "MSFT")))
                .containsExactly(Map.entry("Account", "ACC-2"), Map.entry("Symbol", "MSFT"));
    }

    @Test
    @DisplayName("only the first = splits, so values may contain =")
    void splitsOnTheFirstEqualsOnly() {
        assertThat(decoder.decode(TestConnectors.delimited("58", "reason=too late")))
                .containsEntry("58", "reason=too late");
    }

    @Test
    void keepsEmptyValuesAsPresentButEmpty() {
        assertThat(decoder.decode(TestConnectors.delimited("11", "C-1", "58", "")))
                .containsEntry("58", "")
                .containsKey("58");
    }

    @Test
    void toleratesMissingTrailingSeparator() {
        assertThat(decoder.decode("11=C-1" + TestConnectors.SOH + "55=AAPL"))
                .containsExactly(Map.entry("11", "C-1"), Map.entry("55", "AAPL"));
    }

    @Test
    void skipsSegmentsWithoutASeparator() {
        assertThat(decoder.decode("garbage" + TestConnectors.SOH + "55=AAPL"))
                .containsExactly(Map.entry("55", "AAPL"));
    }

    @Test
    void lastValueWinsForARepeatedTag() {
        assertThat(decoder.decode(TestConnectors.delimited("55", "AAPL", "55", "MSFT")))
                .containsEntry("55", "MSFT");
    }

    @Test
    void handlesEmptyAndNullPayloads() {
        assertThat(decoder.decode("")).isEmpty();
        assertThat(decoder.decode(null)).isEmpty();
    }

    @Test
    void honoursANonDefaultSeparator() {
        assertThat(new DelimitedRecordDecoder('|').decode("11=C-1|55=AAPL|"))
                .containsExactly(Map.entry("11", "C-1"), Map.entry("55", "AAPL"));
    }
}
