package com.fix42.dashboard.dh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Source selection and AMPS configuration -- the Java mirror of
 * {@code deephaven-scripts/tests/test_ingest_source.py}.
 *
 * <p>Both are injectable-environment functions precisely so this is testable without a broker.
 */
class IngestConfigTest {

    @Test
    void defaultsToKafkaWhenUnset() {
        assertEquals(Ingest.SOURCE_KAFKA, Ingest.fixSource(Map.of()));
        assertEquals(Ingest.DEFAULT_BOOTSTRAP, Ingest.kafkaBootstrap(Map.of()));
        assertEquals(Ingest.DEFAULT_TOPIC, Ingest.kafkaTopic(Map.of()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"kafka", "KAFKA", " Kafka ", "amps", "AMPS", " amps "})
    void sourceIsCaseAndWhitespaceInsensitive(String value) {
        assertTrue(Ingest.SOURCES.contains(Ingest.fixSource(Map.of(Ingest.SOURCE_ENV, value))));
    }

    @Test
    @DisplayName("an unknown source is a startup error, not a silent fallback to kafka")
    void unknownSourceThrows() {
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class, () -> Ingest.fixSource(Map.of(Ingest.SOURCE_ENV, "rabbitmq")));
        assertTrue(thrown.getMessage().contains("rabbitmq"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains(Ingest.SOURCE_ENV), thrown.getMessage());
    }

    @Test
    void kafkaOverridesAreRead() {
        Map<String, String> env = Map.of(
                Ingest.BOOTSTRAP_ENV, "broker-1:9092,broker-2:9092", Ingest.TOPIC_ENV, "other.topic");
        assertEquals("broker-1:9092,broker-2:9092", Ingest.kafkaBootstrap(env));
        assertEquals("other.topic", Ingest.kafkaTopic(env));
        assertEquals(
                "kafka: broker-1:9092,broker-2:9092 topic=other.topic (seek to beginning)",
                Ingest.sourceDescription(env));
    }

    @Test
    void ampsSourceDescriptionUsesTheAmpsConfig() {
        Map<String, String> env = Map.of(
                Ingest.SOURCE_ENV, "amps",
                AmpsConfig.URI_ENV, "tcp://a:9007/amps/fix,tcp://b:9007/amps/fix",
                AmpsConfig.TOPIC_ENV, "fix.audit");
        assertEquals(
                "amps: tcp://a:9007/amps/fix,tcp://b:9007/amps/fix topic=fix.audit bookmark=epoch",
                Ingest.sourceDescription(env));
    }

    @Test
    void ampsConfigDefaults() {
        AmpsConfig config = AmpsConfig.fromEnv(Map.of());
        assertEquals(List.of(AmpsConfig.DEFAULT_URI), config.uris());
        assertEquals(AmpsConfig.DEFAULT_TOPIC, config.topic());
        assertNull(config.filter());
        assertEquals(AmpsConfig.DEFAULT_CLIENT_NAME, config.clientName());
        assertEquals(AmpsConfig.DEFAULT_BOOKMARK, config.bookmark());
        assertEquals(AmpsConfig.DEFAULT_MAX_PENDING, config.maxPending());
    }

    @Test
    @DisplayName("the AMPS topic falls back to FIX42_TOPIC so a deployment names it once")
    void ampsTopicFallsBackToTheSharedTopic() {
        assertEquals("shared.topic", AmpsConfig.fromEnv(Map.of(AmpsConfig.TOPIC_FALLBACK_ENV, "shared.topic")).topic());
        assertEquals(
                "amps.topic",
                AmpsConfig.fromEnv(Map.of(
                                AmpsConfig.TOPIC_FALLBACK_ENV, "shared.topic",
                                AmpsConfig.TOPIC_ENV, "amps.topic"))
                        .topic());
    }

    @Test
    @DisplayName("the AMPS default topic must not drift from the kafka one")
    void ampsDefaultTopicMirrorsTheKafkaOne() {
        assertEquals(Ingest.DEFAULT_TOPIC, AmpsConfig.DEFAULT_TOPIC);
    }

    @Test
    void uriListSplitsOnCommasAndWhitespace() {
        assertEquals(List.of("tcp://a:9007", "tcp://b:9007"), AmpsConfig.splitUris("tcp://a:9007,tcp://b:9007"));
        assertEquals(List.of("tcp://a:9007", "tcp://b:9007"), AmpsConfig.splitUris(" tcp://a:9007   tcp://b:9007 "));
        assertEquals(List.of(), AmpsConfig.splitUris("  ,, "));
    }

    @Test
    void blankFilterIsTreatedAsAbsent() {
        assertNull(AmpsConfig.fromEnv(Map.of(AmpsConfig.FILTER_ENV, "")).filter());
        assertEquals("/OrderID = 'X'", AmpsConfig.fromEnv(Map.of(AmpsConfig.FILTER_ENV, "/OrderID = 'X'")).filter());
    }

    @Test
    void maxPendingRejectsNonPositiveAndUnparseableValues() {
        assertEquals(AmpsConfig.DEFAULT_MAX_PENDING, AmpsConfig.positiveInt(null, AmpsConfig.DEFAULT_MAX_PENDING));
        assertEquals(AmpsConfig.DEFAULT_MAX_PENDING, AmpsConfig.positiveInt("0", AmpsConfig.DEFAULT_MAX_PENDING));
        assertEquals(AmpsConfig.DEFAULT_MAX_PENDING, AmpsConfig.positiveInt("-5", AmpsConfig.DEFAULT_MAX_PENDING));
        assertEquals(AmpsConfig.DEFAULT_MAX_PENDING, AmpsConfig.positiveInt("lots", AmpsConfig.DEFAULT_MAX_PENDING));
        assertEquals(1000, AmpsConfig.positiveInt(" 1000 ", AmpsConfig.DEFAULT_MAX_PENDING));
    }

    @ParameterizedTest
    @ValueSource(strings = {"epoch", "EPOCH", "beginning", " Epoch "})
    void bookmarkAliasesResolveToEpoch(String value) {
        assertEquals("0", AmpsConfig.resolveBookmark(value, "0", "0|1|", "recent"));
    }

    @Test
    void bookmarkAliasesResolveNowAndMostRecent() {
        assertEquals("0|1|", AmpsConfig.resolveBookmark("now", "0", "0|1|", "recent"));
        assertEquals("recent", AmpsConfig.resolveBookmark("most_recent", "0", "0|1|", "recent"));
        assertEquals("recent", AmpsConfig.resolveBookmark("most-recent", "0", "0|1|", "recent"));
        assertEquals("recent", AmpsConfig.resolveBookmark("recent", "0", "0|1|", "recent"));
    }

    @Test
    @DisplayName("anything not an alias is passed to AMPS verbatim as a literal bookmark")
    void literalBookmarksPassThrough() {
        assertEquals("3|1|", AmpsConfig.resolveBookmark("3|1|", "0", "0|1|", "recent"));
    }

    @Test
    void describeIsTheBannerLine() {
        AmpsConfig plain = new AmpsConfig(List.of("tcp://a:9007"), "t", null, "c", "epoch", 10);
        assertEquals("tcp://a:9007 topic=t bookmark=epoch", plain.describe());
        AmpsConfig filtered = new AmpsConfig(List.of("tcp://a:9007"), "t", "/Sym='X'", "c", "epoch", 10);
        assertEquals("tcp://a:9007 topic=t bookmark=epoch filter='/Sym='X''", filtered.describe());
    }
}
