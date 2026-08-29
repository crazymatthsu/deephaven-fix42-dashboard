package com.fix42.dashboard.dh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The AMPS-to-update-graph hand-off buffer.
 *
 * <p>The behaviour under test is the deliberate one: on overflow {@link RawBuffer#offer} blocks
 * rather than dropping, because a dropped FIX message would break an amend chain silently while a
 * blocked reader thread is just TCP backpressure.
 */
class RawBufferTest {

    private static RawBuffer.Row row(String raw) {
        return new RawBuffer.Row(raw, "bm-" + raw, Instant.parse("2024-01-15T14:30:00Z"));
    }

    @Test
    void offerThenDrainRoundTripsInOrder() {
        RawBuffer buffer = new RawBuffer(10);
        assertTrue(buffer.offer(row("a")));
        assertTrue(buffer.offer(row("b")));
        assertEquals(2, buffer.pending());

        List<RawBuffer.Row> drained = buffer.drain();
        assertEquals(List.of("a", "b"), drained.stream().map(RawBuffer.Row::rawFix).toList());
        assertEquals(0, buffer.pending());
        assertEquals(2, buffer.offered());
        assertEquals(0, buffer.dropped());
    }

    @Test
    void drainOnAnEmptyBufferIsEmpty() {
        assertEquals(List.of(), new RawBuffer(10).drain());
    }

    @Test
    @DisplayName("a full buffer blocks the producer until a drain frees space")
    @Timeout(10)
    void offerBlocksWhenFullAndResumesAfterDrain() throws Exception {
        RawBuffer buffer = new RawBuffer(1);
        assertTrue(buffer.offer(row("first")));

        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean accepted = new AtomicBoolean();
        Thread producer = new Thread(() -> {
            started.countDown();
            accepted.set(buffer.offer(row("second")));
        });
        producer.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));

        // The producer is parked in offer(); nothing was dropped, and the wait was counted.
        producer.join(300);
        assertTrue(producer.isAlive(), "offer must block rather than drop when the buffer is full");
        assertEquals(0, buffer.dropped());

        buffer.drain();
        producer.join(5_000);
        assertFalse(producer.isAlive());
        assertTrue(accepted.get(), "the row must be accepted once space frees up");
        assertEquals(1, buffer.waits());
        assertEquals(0, buffer.dropped());
        assertEquals(List.of("second"), buffer.drain().stream().map(RawBuffer.Row::rawFix).toList());
    }

    @Test
    @Timeout(10)
    void closeReleasesABlockedProducerAndCountsTheLoss() throws Exception {
        RawBuffer buffer = new RawBuffer(1);
        buffer.offer(row("first"));

        AtomicBoolean accepted = new AtomicBoolean(true);
        Thread producer = new Thread(() -> accepted.set(buffer.offer(row("second"))));
        producer.start();
        Thread.sleep(100);

        buffer.close();
        producer.join(5_000);
        assertFalse(producer.isAlive());
        assertFalse(accepted.get(), "a closed buffer refuses the row");
        assertEquals(1, buffer.dropped());
        assertTrue(buffer.closed());
    }

    @Test
    void offerAfterCloseIsRefusedImmediately() {
        RawBuffer buffer = new RawBuffer(10);
        buffer.close();
        assertFalse(buffer.offer(row("a")));
        assertEquals(1, buffer.dropped());
        assertEquals(0, buffer.offered());
    }

    @Test
    @DisplayName("a bounded wait that expires drops the row rather than blocking forever")
    @Timeout(10)
    void boundedOfferTimesOut() {
        RawBuffer buffer = new RawBuffer(1);
        assertTrue(buffer.offer(row("first"), 50));
        assertFalse(buffer.offer(row("second"), 50));
        assertEquals(1, buffer.dropped());
        assertEquals(1, buffer.waits());
    }

    @Test
    void aZeroBoundStillHoldsOneRow() {
        RawBuffer buffer = new RawBuffer(0);
        assertTrue(buffer.offer(row("a"), 50));
        assertFalse(buffer.offer(row("b"), 50));
    }
}
