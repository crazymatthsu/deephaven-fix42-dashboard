package com.fix42.dashboard.connectors.s3;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.S3SourceProperties;
import com.fix42.dashboard.connectors.config.SourceFormat;
import com.fix42.dashboard.connectors.config.SourceProperties;
import com.fix42.dashboard.connectors.source.SourceRecord;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The S3 source against a fake object store -- no SDK client, no network, no MinIO.
 *
 * <p>Everything this source decides for itself sits above {@link S3ObjectStore}: what counts as
 * new (an unseen key, or a key whose ETag moved), how an object's bytes become records, and
 * what a failed listing or a failed read does to the polls that follow. The SDK's own behaviour
 * is AWS's to test, which is exactly why the seam exists.
 */
class S3RecordSourceTest {

    private static final String BUCKET = "trading-data";

    /** Fast enough that "the next poll" is not a wait; slow enough not to spin the CPU. */
    private static final Duration POLL = Duration.ofMillis(50);

    // ---- the fake store --------------------------------------------------------------

    /**
     * A bucket in a map. Keys sort themselves ({@code ConcurrentSkipListMap}), every write
     * mints a fresh ETag the way a rewritten object gets one, and failures are scheduled
     * per-operation so a test can say "this read fails once" without a mocking framework.
     */
    private static final class FakeStore implements S3ObjectStore {

        private final Map<String, String> bodies = new ConcurrentSkipListMap<>();
        private final Map<String, String> etags = new ConcurrentHashMap<>();
        private final Map<String, AtomicInteger> readFailures = new ConcurrentHashMap<>();
        private final AtomicInteger versions = new AtomicInteger();
        private final AtomicInteger listFailures = new AtomicInteger();
        private final AtomicInteger lists = new AtomicInteger();

        /** Write (or rewrite) an object; a rewrite mints a new ETag, as S3 would. */
        void put(String key, String body) {
            bodies.put(key, body);
            etags.put(key, "etag-" + versions.incrementAndGet());
        }

        /** Rewrite an object's bytes but leave its ETag alone -- a store that lies. */
        void putWithoutChangingEtag(String key, String body) {
            bodies.put(key, body);
        }

        void failNextListings(int count) {
            listFailures.set(count);
        }

        void failNextReadsOf(String key, int count) {
            readFailures.computeIfAbsent(key, k -> new AtomicInteger()).set(count);
        }

        @Override
        public List<ObjectRef> list() throws IOException {
            lists.incrementAndGet();
            if (listFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IOException("listing s3://" + BUCKET + " failed");
            }
            return bodies.keySet().stream()
                    .map(key -> new ObjectRef(key, etags.get(key)))
                    .toList();
        }

        @Override
        public InputStream read(String key) throws IOException {
            AtomicInteger failures = readFailures.get(key);
            if (failures != null && failures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                throw new IOException("reading s3://" + BUCKET + "/" + key + " failed");
            }
            String body = bodies.get(key);
            if (body == null) {
                throw new IOException("no such key: " + key);
            }
            return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    // ---- fixtures --------------------------------------------------------------------

    private static ConnectorProperties connector() {
        S3SourceProperties s3 = new S3SourceProperties();
        s3.setBucket(BUCKET);
        s3.setPrefix("trades/");
        s3.setPollInterval(POLL);
        s3.setReconnectDelay(Duration.ofMillis(50));

        SourceProperties source = new SourceProperties();
        source.setS3(s3);

        ConnectorProperties connector = new ConnectorProperties();
        connector.setName("trades");
        connector.setFormat(SourceFormat.JSON);
        connector.setSource(source);
        return connector;
    }

    private static S3RecordSource started(
            ConnectorProperties connector, FakeStore store, List<SourceRecord> received) {
        S3RecordSource source = new S3RecordSource(connector, store);
        source.start(received::add);
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(source::isConnected);
        return source;
    }

    private static void awaitRecords(List<SourceRecord> received, int count) {
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> received.size() >= count);
    }

    /** Let several more polls run, for the assertions about what does NOT arrive. */
    private static void severalMorePolls() throws InterruptedException {
        Thread.sleep(POLL.toMillis() * 6);
    }

    private static boolean pollThreadAlive() {
        return Thread.getAllStackTraces().keySet().stream()
                .anyMatch(thread -> thread.getName().equals("trades-s3") && thread.isAlive());
    }

    private static String ndjson(String... symbols) {
        StringBuilder body = new StringBuilder();
        for (String symbol : symbols) {
            body.append("{\"symbol\":\"").append(symbol).append("\"}").append('\n');
        }
        return body.toString();
    }

    // ---- the one rule: unseen key, or changed etag ------------------------------------

    @Test
    @DisplayName("each line of a new object under the prefix arrives as its own record")
    void prefixModeEmitsEveryLineOfANewObject() throws Exception {
        FakeStore store = new FakeStore();
        store.put("trades/2026-09-17.ndjson", ndjson("AAPL", "MSFT", "TSLA"));

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (S3RecordSource source = started(connector(), store, received)) {
            awaitRecords(received, 3);
            severalMorePolls();

            assertThat(received).extracting(SourceRecord::data).containsExactly(
                    "{\"symbol\":\"AAPL\"}",
                    "{\"symbol\":\"MSFT\"}",
                    "{\"symbol\":\"TSLA\"}");
            // Keyless upserts: an object's frames carry no key, and a key leaving a bucket
            // says nothing about the records already read out of it.
            assertThat(received).extracting(SourceRecord::action)
                    .containsOnly(SourceRecord.Action.UPSERT);
            assertThat(received).extracting(SourceRecord::key).containsOnlyNulls();
            // The trailing newline framed nothing: an empty frame is not a record.
            assertThat(received).hasSize(3);
        }
    }

    @Test
    @DisplayName("an unchanged etag emits nothing, however many polls run")
    void anUnchangedEtagIsNeverReEmitted() throws Exception {
        FakeStore store = new FakeStore();
        store.put("trades/2026-09-17.ndjson", ndjson("AAPL", "MSFT"));

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (S3RecordSource source = started(connector(), store, received)) {
            awaitRecords(received, 2);
            // Rewritten bytes behind an unchanged ETag are invisible on purpose: the ETag is
            // what the rule reads, and re-reading every object every poll is the thing this
            // source exists not to do.
            store.putWithoutChangingEtag("trades/2026-09-17.ndjson", ndjson("NVDA", "AMD"));
            severalMorePolls();

            assertThat(store.lists.get()).as("polls really did run").isGreaterThan(1);
            assertThat(received).hasSize(2);
        }
    }

    @Test
    @DisplayName("a rewritten object comes back whole, because every record in it is new")
    void aChangedEtagReEmitsTheWholeObject() throws Exception {
        FakeStore store = new FakeStore();
        store.put("trades/today.ndjson", ndjson("AAPL"));

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (S3RecordSource source = started(connector(), store, received)) {
            awaitRecords(received, 1);
            store.put("trades/today.ndjson", ndjson("AAPL", "MSFT"));
            awaitRecords(received, 3);
            severalMorePolls();

            assertThat(received).extracting(SourceRecord::data).containsExactly(
                    "{\"symbol\":\"AAPL\"}",
                    "{\"symbol\":\"AAPL\"}",
                    "{\"symbol\":\"MSFT\"}");
        }
    }

    @Test
    @DisplayName("a key that appears later is emitted once, and the earlier one is not re-read")
    void aNewKeyIsEmittedExactlyOnce() throws Exception {
        FakeStore store = new FakeStore();
        store.put("trades/2026-09-17.ndjson", ndjson("AAPL"));

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (S3RecordSource source = started(connector(), store, received)) {
            awaitRecords(received, 1);
            store.put("trades/2026-09-18.ndjson", ndjson("MSFT"));
            awaitRecords(received, 2);
            severalMorePolls();

            assertThat(received).extracting(SourceRecord::data)
                    .containsExactly("{\"symbol\":\"AAPL\"}", "{\"symbol\":\"MSFT\"}");
        }
    }

    // ---- framing -----------------------------------------------------------------------

    @Test
    @DisplayName("WHOLE framing makes each object one record, delimiters and all")
    void wholeFramingEmitsOneRecordPerObject() throws Exception {
        ConnectorProperties connector = connector();
        connector.getSource().getS3().setFraming(S3SourceProperties.Framing.WHOLE);

        FakeStore store = new FakeStore();
        String document = "{\"trades\":[{\"symbol\":\"AAPL\"},\n{\"symbol\":\"MSFT\"}]}";
        store.put("trades/batch-1.json", document);

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (S3RecordSource source = started(connector, store, received)) {
            awaitRecords(received, 1);
            store.put("trades/batch-2.json", document);
            awaitRecords(received, 2);
            severalMorePolls();

            // The embedded newline did not frame anything: WHOLE means the object.
            assertThat(received).extracting(SourceRecord::data)
                    .containsExactly(document, document);
        }
    }

    @Test
    @DisplayName("a multi-byte delimiter is matched as a byte sequence, not a lead byte")
    void aMultiByteDelimiterSplitsCorrectly() throws Exception {
        // U+00A7 encodes as C2 A7 in UTF-8; the payload carries U+00A9 (C2 A9), which shares
        // the lead byte. A split on the first byte alone would cut the copyright sign in half.
        String delimiter = "§";
        ConnectorProperties connector = connector();
        connector.getSource().getS3().setDelimiter(delimiter);

        FakeStore store = new FakeStore();
        store.put("trades/odd.txt",
                "{\"venue\":\"XNAS ©\"}" + delimiter + "{\"venue\":\"XLON\"}"
                        + delimiter + "{\"venue\":\"XETR\"}");

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (S3RecordSource source = started(connector, store, received)) {
            awaitRecords(received, 3);
            severalMorePolls();

            assertThat(received).extracting(SourceRecord::data).containsExactly(
                    "{\"venue\":\"XNAS ©\"}",
                    "{\"venue\":\"XLON\"}",
                    // The tail after the last delimiter is a whole record: an object ends
                    // where the file ends, unlike a socket's unterminated trailing bytes.
                    "{\"venue\":\"XETR\"}");
            assertThat(Charset.forName("UTF-8").encode(delimiter).remaining())
                    .as("the delimiter really is multi-byte").isGreaterThan(1);
        }
    }

    @Test
    @DisplayName("a record longer than one read is reassembled, not cut at the buffer")
    void aRecordSpanningSeveralReadsIsReassembled() throws Exception {
        // Comfortably past the 8 KiB read buffer, so the record's bytes arrive in pieces and
        // a delimiter scan that forgot the leftover would cut it into three.
        String wide = "{\"note\":\"" + "x".repeat(20_000) + "\"}";

        FakeStore store = new FakeStore();
        store.put("trades/wide.ndjson", wide + "\n" + "{\"note\":\"short\"}" + "\n");

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (S3RecordSource source = started(connector(), store, received)) {
            awaitRecords(received, 2);
            severalMorePolls();

            assertThat(received).extracting(SourceRecord::data)
                    .containsExactly(wide, "{\"note\":\"short\"}");
        }
    }

    // ---- failure and lifecycle -----------------------------------------------------------

    @Test
    @DisplayName("a store that throws once keeps polling, and does not re-read what it had")
    void aFailureIsRetriedWithoutLosingTheSeenObjects() throws Exception {
        FakeStore store = new FakeStore();
        store.put("trades/2026-09-17.ndjson", ndjson("AAPL", "MSFT"));

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (S3RecordSource source = started(connector(), store, received)) {
            awaitRecords(received, 2);

            // The next listing fails outright, and the read of the object that appears after
            // it fails once too -- so the poll dies halfway through, twice over.
            store.failNextListings(1);
            store.failNextReadsOf("trades/2026-09-18.ndjson", 1);
            store.put("trades/2026-09-18.ndjson", ndjson("TSLA"));

            awaitRecords(received, 3);
            severalMorePolls();

            // The new object arrived once the store recovered, and the one already read was
            // not re-emitted by any of the retries in between.
            assertThat(received).extracting(SourceRecord::data).containsExactly(
                    "{\"symbol\":\"AAPL\"}",
                    "{\"symbol\":\"MSFT\"}",
                    "{\"symbol\":\"TSLA\"}");
            assertThat(source.isConnected()).as("back in the poll loop").isTrue();
        }
    }

    @Test
    @DisplayName("close() cuts the poll interval short instead of waiting it out")
    void closeStopsThePollThreadMidInterval() throws Exception {
        ConnectorProperties connector = connector();
        // Long enough that a close waiting out the interval would be obvious.
        connector.getSource().getS3().setPollInterval(Duration.ofSeconds(30));

        FakeStore store = new FakeStore();
        store.put("trades/2026-09-17.ndjson", ndjson("AAPL"));

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        S3RecordSource source = started(connector, store, received);
        awaitRecords(received, 1);

        long start = System.nanoTime();
        source.close();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isLessThan(Duration.ofSeconds(5));
        assertThat(source.isConnected()).isFalse();
        assertThat(pollThreadAlive()).as("no connector thread is left behind").isFalse();
        // Idempotent, the way ConnectorManager calls it.
        source.close();
    }
}
