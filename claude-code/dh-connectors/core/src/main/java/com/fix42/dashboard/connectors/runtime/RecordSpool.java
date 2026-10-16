package com.fix42.dashboard.connectors.runtime;

import com.fix42.dashboard.connectors.config.SpoolProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The queue between a source's delivery thread and the pipeline: memory first, segment files
 * once memory is full, back to memory when the backlog has drained.
 *
 * <p>Why it exists: the AMPS client delivers a SOW replay as fast as the server can send it,
 * and a consumer that blocks on every Deephaven round trip stops reading its socket -- which is
 * precisely what the server's slow-client policy disconnects, after which the client reconnects
 * and the same replay starts over. Queueing here keeps the delivery thread reading while the
 * drain thread publishes at Deephaven's pace. {@code memory-limit} bounds the heap and
 * {@code disk-limit} bounds the files; the producer blocks only at the latter, so the
 * back-pressure the pipeline used to apply on every full batch is now the last resort.
 *
 * <p>Order is preserved across the tiers. Once a spill starts, every new entry goes to disk
 * behind the ones already there, and memory is used again only when the disk tier is empty;
 * mixing the tiers would let an older SOW record overtake a newer live update for its key.
 *
 * <p>A segment file is deleted as soon as every entry in it has been taken, so disk holds the
 * unread backlog plus at most the segment being written. The files are throwaway: no fsync,
 * nothing is read back after a restart (every start replays the feed, doc 07 section 6), and
 * leftovers of a crashed run are removed when the directory is opened again.
 *
 * <p>Thread-safety: any number of producers, one consumer. Every state change, the file I/O
 * included, happens under one lock. The I/O is page-cache speed, and the consumer does its
 * real work -- decoding and the Deephaven publish -- outside the lock.
 */
public final class RecordSpool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RecordSpool.class);

    /** Appends gather here before reaching the channel; the reader flushes it when it catches up. */
    private static final int WRITE_BUFFER_BYTES = 256 * 1024;
    private static final int READ_BUFFER_BYTES = 256 * 1024;
    /** Each entry on disk is a 4-byte length followed by {@link SpoolCodec}'s bytes. */
    private static final int FRAME_HEADER_BYTES = 4;
    private static final String SEGMENT_PREFIX = "segment-";
    private static final String SEGMENT_SUFFIX = ".seg";
    /** After a disk failure the spool blocks instead of spilling, and retries the disk after this long. */
    private static final Duration DISK_RETRY_INTERVAL = Duration.ofMinutes(1);

    private final String connectorName;
    private final Path directory;
    private final long memoryLimit;
    private final long segmentSize;
    private final long diskLimit;

    private final Object lock = new Object();
    private final ArrayDeque<SpoolEntry> memory = new ArrayDeque<>();
    /** Segments not yet fully consumed, oldest first; the last one is {@link #writeSegment} while spilling. */
    private final ArrayDeque<Segment> segments = new ArrayDeque<>();
    private final ByteBuffer writeBuffer = ByteBuffer.allocate(WRITE_BUFFER_BYTES);
    /** Kept in read mode: position is the next byte to parse, limit the end of what was fetched. */
    private ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_BYTES).limit(0);

    /** Non-null exactly while spilling, sealed when the disk failed and the episode is only draining. */
    private Segment writeSegment;
    private long memoryBytes;
    private long diskBytes;
    private long pending;
    private int bufferedEntries;
    private long nextSegmentId;
    private long diskRetryAtNanos;
    private boolean diskRetryPending;
    private boolean finished;
    private boolean closed;

    private long episodeEntries;
    private long episodeBytes;
    private long episodeSegments;
    private long episodeStartedNanos;

    private long spilledEntries;
    private long segmentsCreated;
    private long segmentsDeleted;
    private long lostEntries;
    private long spillEpisodes;

    private static final class Segment {
        final Path path;
        FileChannel writeChannel;
        FileChannel readChannel;
        /** Bytes appended, buffered or flushed. */
        long written;
        /** Bytes flushed to the file, which is also the file's length. */
        long committed;
        /** Bytes the reader has pulled into its buffer. */
        long fetched;
        /** The writer has moved on (or failed); delete once consumed. */
        boolean sealed;

        Segment(Path path, FileChannel writeChannel) {
            this.path = path;
            this.writeChannel = writeChannel;
        }
    }

    /**
     * Open the spool for one connector, creating its directory and removing any segment files a
     * previous run left behind.
     *
     * @throws IOException if the directory cannot be created
     */
    public RecordSpool(String connectorName, SpoolProperties properties) throws IOException {
        this.connectorName = connectorName;
        this.memoryLimit = Math.max(0L, properties.getMemoryLimit().toBytes());
        this.segmentSize = Math.max(1L, properties.getSegmentSize().toBytes());
        this.diskLimit = Math.max(this.segmentSize, properties.getDiskLimit().toBytes());
        this.directory = directoryFor(properties.getDirectory(), connectorName);
        Files.createDirectories(directory);
        int leftovers = deleteSegmentFiles();
        if (leftovers > 0) {
            log.info("[{}] removed {} leftover spool segment(s) of a previous run under {}",
                    connectorName, leftovers, directory);
        }
    }

    /** Where a connector's segments go: the configured directory, or a folder under the JVM's temp directory. */
    static Path directoryFor(String configured, String connectorName) {
        Path base = configured == null || configured.isBlank()
                ? Path.of(System.getProperty("java.io.tmpdir")).resolve("dh-connectors-spool")
                : Path.of(configured.trim());
        return base.resolve(connectorName.replaceAll("[^A-Za-z0-9._-]", "_"));
    }

    /** The directory this spool writes its segments to. */
    public Path directory() {
        return directory;
    }

    /**
     * Queue one entry.
     *
     * <p>Returns promptly unless the spilled backlog is at {@code disk-limit}, or the disk is
     * unusable and memory is full; then the caller blocks until the consumer makes room, which
     * is the back-pressure the pipeline applied before the spool existed. After
     * {@link #finish()} or {@link #close()} the entry is dropped: the connector is stopping, and
     * its next start replays the feed.
     */
    public void put(SpoolEntry entry) {
        long size = entry.sizeEstimate();
        synchronized (lock) {
            while (true) {
                if (closed || finished) {
                    return;
                }
                if (writeSegment == null) {
                    if (memoryBytes + size <= memoryLimit) {
                        memory.addLast(entry);
                        memoryBytes += size;
                        pending++;
                        lock.notifyAll();
                        return;
                    }
                    if (!beginSpillLocked()) {
                        if (memory.isEmpty()) {
                            // No disk, and nothing to wait for: an entry bigger than the memory
                            // limit still has to get through, one at a time.
                            memory.addLast(entry);
                            memoryBytes += size;
                            pending++;
                            lock.notifyAll();
                            return;
                        }
                        // No disk to spill to: block until memory has room, as before the spool.
                        if (!awaitRoomLocked()) {
                            return;
                        }
                        continue;
                    }
                }
                if (writeSegment.sealed) {
                    // The disk failed mid-episode. What is on disk still drains in order; this
                    // entry waits for that, then goes to memory.
                    if (!awaitRoomLocked()) {
                        return;
                    }
                    continue;
                }
                if (diskBytes >= diskLimit) {
                    if (!awaitRoomLocked()) {
                        return;
                    }
                    continue;
                }
                if (appendLocked(entry)) {
                    lock.notifyAll();
                    return;
                }
                // The append failed and sealed the segment; loop into the wait above.
            }
        }
    }

    /**
     * Take the next entry, waiting for one.
     *
     * @return the entry, or {@code null} once the spool is closed, or finished and drained
     */
    public SpoolEntry take() throws InterruptedException {
        synchronized (lock) {
            while (true) {
                if (closed) {
                    return null;
                }
                SpoolEntry entry = pollLocked();
                if (entry != null) {
                    return entry;
                }
                if (finished) {
                    return null;
                }
                lock.wait();
            }
        }
    }

    /** Take the next entry if there is one. */
    public SpoolEntry poll() {
        synchronized (lock) {
            return closed ? null : pollLocked();
        }
    }

    /**
     * No more input: {@link #take()} returns {@code null} once what is queued has been taken
     * instead of waiting, and a producer blocked for room is released.
     */
    public void finish() {
        synchronized (lock) {
            finished = true;
            lock.notifyAll();
        }
    }

    /**
     * Discard everything queued and delete the segment files.
     *
     * @return how many entries were discarded
     */
    public long clear() {
        synchronized (lock) {
            long discarded = pending;
            memory.clear();
            memoryBytes = 0L;
            discardDiskLocked();
            pending = 0L;
            lock.notifyAll();
            return discarded;
        }
    }

    /** Discard everything, delete the files, and release anyone waiting. Idempotent. */
    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            clear();
            try {
                Files.deleteIfExists(directory);
            } catch (IOException e) {
                // Something else is in the directory; leaving it is harmless.
                log.debug("[{}] spool directory {} not removed: {}", connectorName, directory, e.toString());
            }
            lock.notifyAll();
        }
    }

    /** Entries queued and not yet taken, across both tiers. */
    public long backlog() {
        synchronized (lock) {
            return pending;
        }
    }

    /** Whether there is nothing to take. */
    public boolean isEmpty() {
        return backlog() == 0L;
    }

    /** Whether the disk tier is in use. */
    public boolean isSpilling() {
        synchronized (lock) {
            return writeSegment != null;
        }
    }

    /** Approximate bytes of the entries held in memory. */
    public long memoryBytes() {
        synchronized (lock) {
            return memoryBytes;
        }
    }

    /** Bytes written to segment files and not yet taken. */
    public long diskBytes() {
        synchronized (lock) {
            return diskBytes;
        }
    }

    /** Entries that went through a segment file since the spool was opened. */
    public long spilledEntries() {
        synchronized (lock) {
            return spilledEntries;
        }
    }

    /** Spill episodes since the spool was opened: memory filled, disk used, disk drained. */
    public long spillEpisodes() {
        synchronized (lock) {
            return spillEpisodes;
        }
    }

    public long segmentsCreated() {
        synchronized (lock) {
            return segmentsCreated;
        }
    }

    public long segmentsDeleted() {
        synchronized (lock) {
            return segmentsDeleted;
        }
    }

    /** Entries lost to a disk failure or an interrupted producer; the next replay restores them. */
    public long lostEntries() {
        synchronized (lock) {
            return lostEntries;
        }
    }

    // ---- producer side -----------------------------------------------------------------

    /** Wait for the consumer to make room. {@code false} if interrupted, with the entry counted as lost. */
    private boolean awaitRoomLocked() {
        try {
            lock.wait(1_000L);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            lostEntries++;
            log.warn("[{}] interrupted while waiting for spool room; one record dropped", connectorName);
            return false;
        }
    }

    private boolean beginSpillLocked() {
        long now = System.nanoTime();
        if (diskRetryPending && now - diskRetryAtNanos < 0L) {
            return false;
        }
        diskRetryPending = false;
        try {
            writeSegment = openSegmentLocked();
        } catch (IOException e) {
            diskRetryPending = true;
            diskRetryAtNanos = now + DISK_RETRY_INTERVAL.toNanos();
            log.error("[{}] cannot spill to {}: {} -- the source blocks until memory drains, "
                    + "as it did before the spool", connectorName, directory, e.toString());
            return false;
        }
        spillEpisodes++;
        episodeEntries = 0L;
        episodeBytes = 0L;
        episodeSegments = 1L;
        episodeStartedNanos = now;
        log.info("[{}] memory queue full ({} record(s), ~{} bytes): spilling to {}",
                connectorName, memory.size(), memoryBytes, directory);
        return true;
    }

    private Segment openSegmentLocked() throws IOException {
        Path path = directory.resolve(String.format(Locale.ROOT, "%s%06d%s",
                SEGMENT_PREFIX, nextSegmentId++, SEGMENT_SUFFIX));
        FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        Segment segment = new Segment(path, channel);
        segments.addLast(segment);
        segmentsCreated++;
        return segment;
    }

    private boolean appendLocked(SpoolEntry entry) {
        byte[] payload = SpoolCodec.encode(entry);
        int frame = FRAME_HEADER_BYTES + payload.length;
        try {
            if (writeSegment.written >= segmentSize) {
                rollLocked();
            }
            Segment segment = writeSegment;
            if (writeBuffer.remaining() < frame) {
                flushWriteBufferLocked(segment);
            }
            if (writeBuffer.remaining() < frame) {
                // Bigger than the buffer: straight to the channel, behind what was just flushed.
                writeFully(segment.writeChannel, ByteBuffer.allocate(FRAME_HEADER_BYTES).putInt(payload.length).flip());
                writeFully(segment.writeChannel, ByteBuffer.wrap(payload));
                segment.committed += frame;
            } else {
                writeBuffer.putInt(payload.length).put(payload);
                bufferedEntries++;
            }
            segment.written += frame;
            diskBytes += frame;
            pending++;
            spilledEntries++;
            episodeEntries++;
            episodeBytes += frame;
            return true;
        } catch (IOException e) {
            failWriteLocked(e);
            return false;
        }
    }

    private void rollLocked() throws IOException {
        Segment current = writeSegment;
        flushWriteBufferLocked(current);
        current.writeChannel.close();
        current.writeChannel = null;
        current.sealed = true;
        writeSegment = openSegmentLocked();
        episodeSegments++;
    }

    private void flushWriteBufferLocked(Segment segment) throws IOException {
        if (writeBuffer.position() == 0) {
            return;
        }
        writeBuffer.flip();
        writeFully(segment.writeChannel, writeBuffer);
        writeBuffer.clear();
        segment.committed = segment.written;
        bufferedEntries = 0;
    }

    /**
     * The disk failed under the writer. What reached the file stays readable; what sat in the
     * write buffer is lost; the segment is sealed so the episode only drains from here, and
     * the producer blocks until it has, then uses memory again.
     */
    private void failWriteLocked(IOException e) {
        Segment segment = writeSegment;
        long unflushed = segment.written - segment.committed;
        int lost = bufferedEntries;
        lostEntries += lost;
        pending -= lost;
        diskBytes -= unflushed;
        episodeEntries -= lost;
        episodeBytes -= unflushed;
        segment.written = segment.committed;
        bufferedEntries = 0;
        writeBuffer.clear();
        if (segment.writeChannel != null) {
            try {
                segment.writeChannel.truncate(segment.committed);
            } catch (IOException truncateFailure) {
                log.debug("[{}] could not truncate {}: {}", connectorName, segment.path, truncateFailure.toString());
            }
            closeQuietly(segment.writeChannel);
            segment.writeChannel = null;
        }
        segment.sealed = true;
        diskRetryPending = true;
        diskRetryAtNanos = System.nanoTime() + DISK_RETRY_INTERVAL.toNanos();
        log.error("[{}] spool write to {} failed: {} -- {} buffered record(s) lost; the source "
                + "blocks until the spilled backlog drains", connectorName, segment.path, e.toString(), lost);
    }

    // ---- consumer side -----------------------------------------------------------------

    private SpoolEntry pollLocked() {
        SpoolEntry fromMemory = memory.pollFirst();
        if (fromMemory != null) {
            memoryBytes -= fromMemory.sizeEstimate();
            pending--;
            lock.notifyAll();
            return fromMemory;
        }
        if (writeSegment == null) {
            return null;
        }
        SpoolEntry fromDisk;
        try {
            fromDisk = readLocked();
        } catch (IOException e) {
            long onDisk = pending;
            lostEntries += onDisk;
            pending = 0L;
            log.error("[{}] spool read failed: {} -- {} spilled record(s) lost; the next replay restores them",
                    connectorName, e.toString(), onDisk);
            discardDiskLocked();
            lock.notifyAll();
            return null;
        }
        if (fromDisk != null) {
            pending--;
            trimLocked();
        }
        lock.notifyAll();
        return fromDisk;
    }

    /**
     * After a take from disk: a segment that is now fully consumed is deleted at once, and when
     * that leaves nothing on disk the episode ends -- so "deleted when consumed" and "back to
     * memory when drained" hold the moment the last entry is taken, not on the next poll.
     */
    private void trimLocked() {
        Segment segment = segments.peekFirst();
        if (segment == null || readBuffer.hasRemaining()
                || segment.fetched != segment.committed || segment.committed != segment.written) {
            return;
        }
        if (segment.sealed) {
            deleteSegmentLocked(segment);
            if (segments.isEmpty()) {
                // The writer had already failed and sealed its segment: nothing more can arrive here.
                endSpillLocked();
            }
            return;
        }
        // The segment being written, with nothing buffered behind what was just taken.
        endSpillLocked();
    }

    /** The next entry from the disk tier, or {@code null} when it is drained -- which ends the episode. */
    private SpoolEntry readLocked() throws IOException {
        while (true) {
            Segment segment = segments.peekFirst();
            if (segment == null) {
                endSpillLocked();
                return null;
            }
            if (readBuffer.remaining() < FRAME_HEADER_BYTES && !fillLocked(segment, FRAME_HEADER_BYTES)) {
                if (readBuffer.hasRemaining()) {
                    throw new IOException("truncated frame header in " + segment.path);
                }
                if (segment.sealed) {
                    deleteSegmentLocked(segment);
                    continue;
                }
                // The segment being written, and every byte of it consumed: the backlog is gone.
                endSpillLocked();
                return null;
            }
            int length = readBuffer.getInt(readBuffer.position());
            if (length < 0) {
                throw new IOException("corrupt frame length " + length + " in " + segment.path);
            }
            int frame = FRAME_HEADER_BYTES + length;
            if (readBuffer.remaining() < frame && !fillLocked(segment, frame)) {
                throw new IOException("truncated frame in " + segment.path);
            }
            readBuffer.getInt();
            SpoolEntry entry = SpoolCodec.decode(
                    readBuffer.array(), readBuffer.arrayOffset() + readBuffer.position(), length);
            readBuffer.position(readBuffer.position() + length);
            diskBytes -= frame;
            return entry;
        }
    }

    /**
     * Make at least {@code needed} unread bytes available in the read buffer.
     *
     * @return {@code false} if the segment does not hold that many yet
     */
    private boolean fillLocked(Segment segment, int needed) throws IOException {
        if (segment == writeSegment && !segment.sealed && segment.committed < segment.written) {
            flushWriteBufferLocked(segment);
        }
        long available = readBuffer.remaining() + (segment.committed - segment.fetched);
        if (available < needed) {
            return false;
        }
        if (readBuffer.capacity() < needed) {
            ByteBuffer bigger = ByteBuffer.allocate(Math.max(needed, readBuffer.capacity() * 2));
            bigger.put(readBuffer);
            readBuffer = bigger.flip();
        }
        if (segment.readChannel == null) {
            segment.readChannel = FileChannel.open(segment.path, StandardOpenOption.READ);
        }
        readBuffer.compact();
        try {
            while (readBuffer.position() < needed) {
                int want = (int) Math.min(readBuffer.remaining(), segment.committed - segment.fetched);
                int limit = readBuffer.limit();
                readBuffer.limit(readBuffer.position() + want);
                int read = segment.readChannel.read(readBuffer, segment.fetched);
                readBuffer.limit(limit);
                if (read <= 0) {
                    throw new IOException("unexpected end of " + segment.path);
                }
                segment.fetched += read;
            }
        } finally {
            readBuffer.flip();
        }
        return true;
    }

    private void deleteSegmentLocked(Segment segment) {
        segments.remove(segment);
        closeQuietly(segment.readChannel);
        segment.readChannel = null;
        closeQuietly(segment.writeChannel);
        segment.writeChannel = null;
        try {
            Files.deleteIfExists(segment.path);
            segmentsDeleted++;
        } catch (IOException e) {
            log.warn("[{}] could not delete spool segment {}: {}", connectorName, segment.path, e.toString());
        }
        readBuffer.clear().limit(0);
    }

    private void endSpillLocked() {
        Duration took = Duration.ofNanos(System.nanoTime() - episodeStartedNanos);
        log.info("[{}] spill drained: {} record(s), {} bytes over {} segment(s) in {} -- back to memory",
                connectorName, episodeEntries, episodeBytes, episodeSegments, took);
        discardDiskLocked();
    }

    /** Close and delete every segment and leave the disk tier empty. */
    private void discardDiskLocked() {
        Segment segment;
        while ((segment = segments.peekFirst()) != null) {
            deleteSegmentLocked(segment);
        }
        writeSegment = null;
        diskBytes = 0L;
        bufferedEntries = 0;
        writeBuffer.clear();
        readBuffer.clear().limit(0);
    }

    private int deleteSegmentFiles() throws IOException {
        int deleted = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(
                directory, SEGMENT_PREFIX + "*" + SEGMENT_SUFFIX)) {
            for (Path file : files) {
                Files.deleteIfExists(file);
                deleted++;
            }
        }
        return deleted;
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void closeQuietly(FileChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException e) {
            // Nothing to do with a channel that will not close; the file is deleted next.
        }
    }
}
