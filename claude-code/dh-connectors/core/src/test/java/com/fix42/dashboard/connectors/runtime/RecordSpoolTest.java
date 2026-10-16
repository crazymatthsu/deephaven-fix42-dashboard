package com.fix42.dashboard.connectors.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.connectors.config.SpoolProperties;
import com.fix42.dashboard.connectors.source.SourceRecord;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.util.unit.DataSize;

/**
 * The memory-first, disk-spilling queue (doc 07 section 8.1): order across the tiers, segment
 * files appearing and disappearing, the disk cap, and the lifecycle calls the connector makes.
 */
class RecordSpoolTest {

    @TempDir
    Path dir;

    private SpoolProperties settings(long memoryBytes, long segmentBytes, long diskBytes) {
        SpoolProperties properties = new SpoolProperties();
        properties.setMemoryLimit(DataSize.ofBytes(memoryBytes));
        properties.setSegmentSize(DataSize.ofBytes(segmentBytes));
        properties.setDiskLimit(DataSize.ofBytes(diskBytes));
        properties.setDirectory(dir.toString());
        return properties;
    }

    private RecordSpool spool(long memoryBytes, long segmentBytes, long diskBytes) throws IOException {
        return new RecordSpool("orders-fix", settings(memoryBytes, segmentBytes, diskBytes));
    }

    private static SpoolEntry entry(int index) {
        return SpoolEntry.of(SourceRecord.of("payload-" + index, "K-" + index));
    }

    private static int index(SpoolEntry entry) {
        return Integer.parseInt(entry.record().key().substring(2));
    }

    private static long segmentFiles(RecordSpool spool) throws IOException {
        if (!Files.isDirectory(spool.directory())) {
            return 0L;
        }
        try (Stream<Path> files = Files.list(spool.directory())) {
            return files.filter(file -> file.getFileName().toString().endsWith(".seg")).count();
        }
    }

    private static List<Integer> takeAll(RecordSpool spool, int count) throws InterruptedException {
        List<Integer> taken = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            taken.add(index(spool.take()));
        }
        return taken;
    }

    @Test
    @DisplayName("while the entries fit in memory, no file is touched")
    void staysInMemoryWhileItFits() throws Exception {
        try (RecordSpool spool = spool(1_000_000, 65_536, 1_000_000)) {
            for (int i = 0; i < 10; i++) {
                spool.put(entry(i));
            }
            assertThat(spool.isSpilling()).isFalse();
            assertThat(spool.memoryBytes()).isPositive();
            assertThat(spool.backlog()).isEqualTo(10);
            assertThat(segmentFiles(spool)).isZero();

            assertThat(takeAll(spool, 10)).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
            assertThat(spool.backlog()).isZero();
            assertThat(spool.memoryBytes()).isZero();
        }
    }

    @Test
    @DisplayName("memory full -> disk, in order, and back to memory once the disk has drained")
    void spillsToDiskWhenMemoryIsFullAndKeepsOrder() throws Exception {
        try (RecordSpool spool = spool(200, 65_536, 10_000_000)) {
            for (int i = 0; i < 1_000; i++) {
                spool.put(entry(i));
            }
            assertThat(spool.isSpilling()).isTrue();
            assertThat(spool.spilledEntries()).isEqualTo(998);
            assertThat(spool.spillEpisodes()).isEqualTo(1);
            assertThat(segmentFiles(spool)).isEqualTo(1);

            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < 1_000; i++) {
                expected.add(i);
            }
            assertThat(takeAll(spool, 1_000)).isEqualTo(expected);

            assertThat(spool.isSpilling()).as("the episode ends with the last spilled entry").isFalse();
            assertThat(spool.diskBytes()).isZero();
            assertThat(segmentFiles(spool)).isZero();

            spool.put(entry(1_000));
            assertThat(spool.isSpilling()).isFalse();
            assertThat(spool.memoryBytes()).isPositive();
            assertThat(index(spool.take())).isEqualTo(1_000);
        }
    }

    @Test
    @DisplayName("segments roll at segment-size and are deleted as they are consumed")
    void rollsSegmentsAndDeletesThemAsTheyAreConsumed() throws Exception {
        try (RecordSpool spool = spool(0, 1_024, 10_000_000)) {
            for (int i = 0; i < 200; i++) {
                spool.put(entry(i));
            }
            long created = spool.segmentsCreated();
            assertThat(created).isGreaterThanOrEqualTo(3);
            assertThat(segmentFiles(spool)).isEqualTo(created);

            assertThat(takeAll(spool, 150)).startsWith(0, 1, 2).endsWith(148, 149);
            assertThat(spool.segmentsDeleted()).isPositive();
            assertThat(segmentFiles(spool)).isLessThan(created);

            assertThat(takeAll(spool, 50)).startsWith(150).endsWith(199);
            assertThat(spool.segmentsDeleted()).isEqualTo(spool.segmentsCreated());
            assertThat(segmentFiles(spool)).isZero();
            assertThat(spool.backlog()).isZero();
        }
    }

    @Test
    @DisplayName("at disk-limit the producer blocks until the consumer makes room")
    void blocksTheProducerAtTheDiskLimit() throws Exception {
        try (RecordSpool spool = spool(0, 1_024, 1_024)) {
            int queued = 0;
            while (spool.diskBytes() < 1_024) {
                spool.put(entry(queued++));
            }
            int extra = queued;
            Thread producer = new Thread(() -> spool.put(entry(extra)), "producer");
            producer.start();
            producer.join(300);
            assertThat(producer.isAlive()).as("blocked at the cap").isTrue();

            assertThat(index(spool.take())).isZero();
            producer.join(5_000);
            assertThat(producer.isAlive()).as("released by the take").isFalse();

            assertThat(takeAll(spool, queued)).endsWith(extra);
        }
    }

    @Test
    @DisplayName("snapshot boundaries travel in order with the records, in memory and on disk")
    void carriesSnapshotBoundariesInOrder() throws Exception {
        for (long memoryBytes : new long[] {0, 1_000_000}) {
            try (RecordSpool spool = spool(memoryBytes, 65_536, 1_000_000)) {
                spool.put(entry(0));
                spool.put(SpoolEntry.snapshotBegin());
                spool.put(entry(1));
                spool.put(SpoolEntry.snapshotEnd());
                spool.put(entry(2));

                List<SpoolEntry.Kind> kinds = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    kinds.add(spool.take().kind());
                }
                assertThat(kinds).containsExactly(
                        SpoolEntry.Kind.RECORD, SpoolEntry.Kind.SNAPSHOT_BEGIN, SpoolEntry.Kind.RECORD,
                        SpoolEntry.Kind.SNAPSHOT_END, SpoolEntry.Kind.RECORD);
            }
        }
    }

    @Test
    @DisplayName("finish() lets take() return null once the backlog is gone, instead of waiting")
    void takeReturnsNullOnceFinishedAndDrained() throws Exception {
        try (RecordSpool spool = spool(0, 65_536, 1_000_000)) {
            spool.put(entry(0));
            spool.put(entry(1));
            spool.finish();
            assertThat(index(spool.take())).isZero();
            assertThat(index(spool.take())).isEqualTo(1);
            assertThat(spool.take()).isNull();

            spool.put(entry(2));
            assertThat(spool.backlog()).as("input after finish is dropped").isZero();
        }
    }

    @Test
    @DisplayName("close() releases a waiting consumer with null")
    void takeReturnsNullWhenClosedWhileWaiting() throws Exception {
        RecordSpool spool = spool(1_000_000, 65_536, 1_000_000);
        AtomicReference<SpoolEntry> result = new AtomicReference<>(entry(-1));
        Thread consumer = new Thread(() -> {
            try {
                result.set(spool.take());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "consumer");
        consumer.start();
        consumer.join(200);
        assertThat(consumer.isAlive()).isTrue();

        spool.close();
        consumer.join(5_000);
        assertThat(consumer.isAlive()).isFalse();
        assertThat(result.get()).isNull();
    }

    @Test
    @DisplayName("close() deletes the segment files; clear() discards and reports the backlog")
    void closeAndClearRemoveTheFiles() throws Exception {
        RecordSpool spool = spool(0, 65_536, 1_000_000);
        for (int i = 0; i < 5; i++) {
            spool.put(entry(i));
        }
        assertThat(segmentFiles(spool)).isEqualTo(1);

        assertThat(spool.clear()).isEqualTo(5);
        assertThat(spool.backlog()).isZero();
        assertThat(spool.isSpilling()).isFalse();
        assertThat(segmentFiles(spool)).isZero();

        spool.put(entry(6));
        assertThat(segmentFiles(spool)).as("usable again after clear").isEqualTo(1);
        spool.close();
        assertThat(segmentFiles(spool)).isZero();
        assertThat(spool.take()).isNull();
    }

    @Test
    @DisplayName("segments a crashed run left behind are removed when the spool opens")
    void removesLeftoverSegmentsOfAPreviousRun() throws Exception {
        Path connectorDir = RecordSpool.directoryFor(dir.toString(), "orders-fix");
        Files.createDirectories(connectorDir);
        Path leftover = connectorDir.resolve("segment-000042.seg");
        Files.writeString(leftover, "stale");

        try (RecordSpool spool = spool(0, 65_536, 1_000_000)) {
            assertThat(leftover).doesNotExist();
            assertThat(segmentFiles(spool)).isZero();
        }
    }

    @Test
    @DisplayName("every record shape survives the disk: composite parts, deletes, nulls, unicode")
    void roundTripsEveryRecordShape() throws Exception {
        List<SourceRecord> records = List.of(
                SourceRecord.of("11=C-1\u000155=AAPL", "SOW-1"),
                SourceRecord.of("{\"note\":\"café → ünïcode\"}"),
                SourceRecord.delete("", "SOW-1"),
                SourceRecord.delete("11=C-2", null),
                SourceRecord.composite(List.of("{\"a\":1}", "54=1\u0001", ""), "SOW-3", SourceRecord.Action.UPSERT),
                SourceRecord.composite(List.of(), "SOW-4", SourceRecord.Action.DELETE));
        try (RecordSpool spool = spool(0, 65_536, 1_000_000)) {
            records.forEach(record -> spool.put(SpoolEntry.of(record)));
            for (SourceRecord record : records) {
                assertThat(spool.take().record()).isEqualTo(record);
            }
        }
    }

    @Test
    @DisplayName("a record bigger than the I/O buffers goes through whole")
    void handlesRecordsLargerThanTheBuffers() throws Exception {
        String big = "x".repeat(600_000);
        try (RecordSpool spool = spool(0, 65_536, 10_000_000)) {
            spool.put(SpoolEntry.of(SourceRecord.of(big, "BIG")));
            spool.put(entry(1));
            assertThat(spool.take().record().data()).hasSize(600_000);
            assertThat(index(spool.take())).isEqualTo(1);
            assertThat(spool.isSpilling()).isFalse();
        }
    }

    @Test
    @DisplayName("the connector's directory is derived from the configured root, or the temp dir")
    void resolvesTheDirectory() {
        assertThat(RecordSpool.directoryFor("/var/spool/dh", "orders fix/1"))
                .isEqualTo(Path.of("/var/spool/dh", "orders_fix_1"));
        assertThat(RecordSpool.directoryFor(null, "orders-fix"))
                .isEqualTo(Path.of(System.getProperty("java.io.tmpdir"), "dh-connectors-spool", "orders-fix"));
    }

    @Test
    @DisplayName("a disk that cannot be written degrades to blocking on memory, never to dropping")
    void degradesToBlockingWhenTheDiskIsUnusable() throws Exception {
        try (RecordSpool spool = spool(200, 65_536, 1_000_000)) {
            // A plain file where the connector's directory was: no segment can be created.
            Files.delete(spool.directory());
            Files.writeString(spool.directory(), "not a directory");

            spool.put(entry(0));
            spool.put(entry(1));
            Thread producer = new Thread(() -> spool.put(entry(2)), "producer");
            producer.start();
            producer.join(300);
            assertThat(producer.isAlive()).as("no disk and memory full: blocked").isTrue();
            assertThat(spool.lostEntries()).isZero();
            assertThat(spool.isSpilling()).isFalse();

            assertThat(index(spool.take())).isZero();
            producer.join(5_000);
            assertThat(producer.isAlive()).as("released once memory had room").isFalse();
            assertThat(takeAll(spool, 2)).containsExactly(1, 2);
        }
    }

    @Test
    @DisplayName("an entry bigger than memory-limit still gets through when the disk is unusable")
    void letsAnOversizedEntryThroughWhenTheDiskIsUnusable() throws Exception {
        try (RecordSpool spool = spool(0, 65_536, 1_000_000)) {
            Files.delete(spool.directory());
            Files.writeString(spool.directory(), "not a directory");

            Thread producer = new Thread(() -> {
                spool.put(entry(0));
                spool.put(entry(1));
            }, "producer");
            producer.start();
            assertThat(index(spool.take())).isZero();
            assertThat(index(spool.take())).isEqualTo(1);
            producer.join(5_000);
            assertThat(producer.isAlive()).isFalse();
            assertThat(spool.lostEntries()).isZero();
        }
    }
}
