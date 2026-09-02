package com.fix42.dashboard.gen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link AmpsFixPublisher} routing, counting and flushing, driven through its in-process sink
 * constructor — no broker, no socket. The live path is covered by the remote-URI e2e.
 */
class AmpsFixPublisherTest {

    /** One recorded {@code publish(topic, rawFix)} — the two things an AMPS message carries. */
    private record Sent(String topic, String rawFix) {}

    private final List<Sent> sent = new ArrayList<>();
    private final AtomicInteger flushes = new AtomicInteger();

    private AmpsFixPublisher publisher() {
        return new AmpsFixPublisher((topic, rawFix) -> sent.add(new Sent(topic, rawFix)),
                flushes::incrementAndGet);
    }

    @Test
    @DisplayName("each message reaches its own topic with the raw FIX unchanged")
    void routesRawFixPerTopic() {
        MultiOmsScenarioEngine.GeneratedBatch batch = MultiOmsTestFix.batch(42L, 3, 8, "all");

        try (AmpsFixPublisher publisher = publisher()) {
            for (MultiOmsScenarioEngine.EmittedMessage emitted : batch.messages()) {
                publisher.publish(emitted.topic(), emitted.chainKey(),
                        FixSerializer.serialize(emitted.message()));
            }
            assertEquals(batch.messages().size(), publisher.publishedCount());
        }

        assertEquals(batch.messages().size(), sent.size());
        for (int i = 0; i < sent.size(); i++) {
            MultiOmsScenarioEngine.EmittedMessage emitted = batch.messages().get(i);
            assertEquals(emitted.topic(), sent.get(i).topic(), "each message goes to its hub topic");
            assertEquals(MultiOmsTestFix.hub(emitted.oms()).topic(), sent.get(i).topic());
            assertEquals(FixSerializer.serialize(emitted.message()), sent.get(i).rawFix(),
                    "the payload is the raw SOH-delimited FIX, framing included");
        }
        assertEquals(Set.copyOf(MultiOmsTopology.topics()),
                sent.stream().map(Sent::topic).collect(Collectors.toSet()));
    }

    @Test
    @DisplayName("values stay raw SOH-delimited FIX with valid framing")
    void valuesAreRawFix() {
        ScenarioEngine.GeneratedBatch batch =
                new ScenarioEngine(3L).generate(6, ScenarioCatalog.ALL);

        try (AmpsFixPublisher publisher = publisher()) {
            batch.messages().forEach(m -> publisher.publish(
                    "fix42.messages", m.chainKey(), FixSerializer.serialize(m.message())));
        }

        for (Sent record : sent) {
            assertEquals("fix42.messages", record.topic());
            assertTrue(record.rawFix().startsWith("8=FIX.4.2" + FixTags.SOH));
            assertTrue(record.rawFix().indexOf(FixTags.PIPE) < 0, "values must not be pipe-rendered");
            assertTrue(TestFix.framingValid(record.rawFix()));
        }
    }

    @Test
    @DisplayName("the chain key is dropped: an AMPS message is (topic, payload) only")
    void chainKeyIsNotSent() {
        try (AmpsFixPublisher publisher = publisher()) {
            publisher.publish("fix42.oms-a", "A-0001", "8=FIX.4.2" + FixTags.SOH + "35=D" + FixTags.SOH);
        }
        assertEquals(1, sent.size());
        assertTrue(sent.get(0).rawFix().indexOf("A-0001") < 0,
                "the key must not be smuggled into the payload");
    }

    @Test
    @DisplayName("publishedCount counts every accepted message; flush() calls the flusher")
    void countsAndFlushes() {
        AmpsFixPublisher publisher = publisher();
        assertEquals(0, publisher.publishedCount());
        assertEquals(0, flushes.get());

        publisher.publish("fix42.oms-a", "A-0001", "raw-a");
        assertEquals(1, publisher.publishedCount());
        publisher.publish("fix42.oms-c", "C-0001", "raw-b");
        assertEquals(2, publisher.publishedCount());
        assertEquals(0, flushes.get(), "publishing must not flush per message");

        publisher.flush();
        assertEquals(1, flushes.get());
        publisher.flush();
        assertEquals(2, flushes.get());

        publisher.close();
        assertEquals(2, flushes.get(), "close() does not flush; the caller does");
        assertEquals(2, publisher.publishedCount());
        assertEquals(List.of("fix42.oms-a", "fix42.oms-c"), sent.stream().map(Sent::topic).toList());
        assertEquals(List.of("raw-a", "raw-b"), sent.stream().map(Sent::rawFix).toList());
    }

    @Test
    @DisplayName("wire order is preserved message for message (per-topic order is AMPS's job)")
    void wireOrderIsPreserved() {
        MultiOmsScenarioEngine.GeneratedBatch batch = MultiOmsTestFix.batch(7L, 2, 6, "all");

        try (AmpsFixPublisher publisher = publisher()) {
            batch.messages().forEach(m -> publisher.publish(
                    m.topic(), m.chainKey(), FixSerializer.serialize(m.message())));
        }

        assertEquals(batch.messages().stream().map(m -> FixSerializer.serialize(m.message())).toList(),
                sent.stream().map(Sent::rawFix).toList());
    }
}
