package com.fix42.dashboard.connectors.s3;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.S3SourceProperties;
import com.fix42.dashboard.connectors.source.RecordHandler;
import com.fix42.dashboard.connectors.source.RecordSource;
import com.fix42.dashboard.connectors.source.SourceRecord;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RecordSource} over objects in an S3 bucket.
 *
 * <p>An object store has no subscription, so this source polls a listing. There is no mode to
 * choose, because there is only <strong>one rule</strong>:
 *
 * <blockquote>every poll, list the objects in scope; any whose key has not been seen, or whose
 * ETag differs from the one seen last time, is read and its frames are emitted.</blockquote>
 *
 * <p>Both configurations fall out of that single rule rather than needing their own code path:
 *
 * <table border="1">
 *   <caption>What the rule means for each scope</caption>
 *   <tr><th>{@code source.s3}</th><th>each poll emits</th></tr>
 *   <tr><td>{@code key: trades/today.ndjson}</td>
 *       <td>nothing, until the object is rewritten -- a new ETag re-emits the
 *           <em>whole</em> object, because a rewritten file is a new version of every record
 *           in it</td></tr>
 *   <tr><td>{@code prefix: trades/}</td>
 *       <td>every object that has appeared under the prefix since the last poll, once, in key
 *           order</td></tr>
 * </table>
 *
 * <h2>The seen-map is memory-only, by design</h2>
 *
 * <p>Nothing about what has been read is persisted, so a restarted connector re-lists and
 * re-reads everything in scope. That is the framework's rehydration contract rather than a
 * gap: the Deephaven table is the state, {@code ConnectorManager} rebuilds it from nothing on
 * every restart (doc 07 section 6), and an object feed's replay is simply reading the objects
 * again.
 *
 * <h2>A frame is a stretch of bytes, and nothing more</h2>
 *
 * <p>{@code WHOLE} framing makes each object one record; {@code DELIMITED} splits its bytes on
 * the delimiter's byte sequence in the configured charset, so a multi-byte separator works and
 * empty frames are skipped. Unlike the TCP source -- whose approach to the split this reuses --
 * an object is finite, so the bytes after the last delimiter are a whole record rather than a
 * fragment waiting for more: a file with no trailing newline loses nothing.
 *
 * <p>{@code DELIMITED} reads the object a chunk at a time, so what it holds is one record plus
 * a buffer rather than the whole object -- which matters on a connector image sized for a JVM,
 * not for the largest file anyone might drop in the bucket. {@code WHOLE} necessarily holds the
 * object, because there the object <em>is</em> the record.
 *
 * <p>Every frame becomes a keyless {@code UPSERT}. This source never sets a key and never emits
 * a {@code DELETE}: an object carries no per-record key, and a key disappearing from a bucket
 * says nothing about the records that were read out of it. Hence {@code APPEND_ONLY} and
 * {@code RING} are its natural targets, and the validator refuses
 * {@code deephaven.source-key-column} outright.
 *
 * <p>{@link #start} returns as soon as the poll thread is running, rather than reaching the
 * bucket on the caller's thread the way {@code AmpsRecordSource} does. For a transport whose
 * normal state includes "the bucket is not reachable yet", a failed first listing is the same
 * event as a failed later one and both belong in the same backoff -- so {@link #isConnected()},
 * not a thrown {@code start}, is what reports it.
 */
public class S3RecordSource implements RecordSource {

    private static final Logger log = LoggerFactory.getLogger(S3RecordSource.class);

    /** How long {@link #close()} waits for the poll thread before giving up on it. */
    private static final long CLOSE_JOIN_MILLIS = 5_000;

    /** Read chunk; frames are assembled above it, so this only sets the read size. */
    private static final int READ_BUFFER_BYTES = 8192;

    private final ConnectorProperties connector;
    private final S3SourceProperties source;
    private final S3ObjectStore store;
    private final Charset charset;
    private final byte[] delimiter;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean storeClosed = new AtomicBoolean(false);

    /** Monitor the poll interval and the reconnect backoff wait on, so a close cuts them short. */
    private final Object idle = new Object();

    /**
     * Key to the ETag it was last read at. Touched only by the poll thread, and only ever added
     * to -- never cleared, never replaced -- so a poll that threw halfway through still knows
     * about everything it had already read, and the retry does not re-emit those objects.
     */
    private final Map<String, String> seen = new HashMap<>();

    private volatile Thread thread;

    public S3RecordSource(ConnectorProperties connector) {
        this(connector, new SdkS3ObjectStore(connector.getSource().getS3()));
    }

    /** The seam the tests use: any {@link S3ObjectStore}, no SDK and no network. */
    S3RecordSource(ConnectorProperties connector, S3ObjectStore store) {
        this.connector = connector;
        this.source = connector.getSource().getS3();
        this.store = store;
        // Resolved once, here: an unknown charset is a configuration error, and failing at
        // construction reports it against the connector instead of once per poll.
        this.charset = Charset.forName(source.getCharset());
        this.delimiter = source.getDelimiter().getBytes(charset);
    }

    @Override
    public void start(RecordHandler handler) {
        log.info("[{}] starting S3 source: s3://{}/{} ({} framing, poll every {})",
                connector.getName(), source.getBucket(), source.keyOrPrefix(),
                source.getFraming(), source.getPollInterval());
        Thread runner = new Thread(() -> run(handler), connector.getName() + "-s3");
        runner.setDaemon(true);
        this.thread = runner;
        runner.start();
    }

    /**
     * Poll until a listing or a read fails, back off, poll again. One iteration of the outer
     * loop is one healthy run of the feed.
     *
     * <p>There is no session to establish, so the first successful poll <em>is</em> the
     * connection: that is when {@link #isConnected()} starts answering true, and a failure
     * anywhere -- first listing or thousandth -- drops back into the same backoff.
     */
    private void run(RecordHandler handler) {
        try {
            while (!closed.get()) {
                try {
                    while (!closed.get()) {
                        poll(handler);
                        if (connected.compareAndSet(false, true)) {
                            log.info("[{}] reading s3://{}/{}", connector.getName(),
                                    source.getBucket(), source.keyOrPrefix());
                        }
                        if (!sleep(source.getPollInterval())) {
                            break;
                        }
                    }
                } catch (IOException | RuntimeException e) {
                    if (!closed.get()) {
                        log.warn("[{}] S3 source failed on s3://{}/{}", connector.getName(),
                                source.getBucket(), source.keyOrPrefix(), e);
                    }
                } finally {
                    connected.set(false);
                }
                if (!closed.get()) {
                    log.info("[{}] retrying s3://{}/{} in {}", connector.getName(),
                            source.getBucket(), source.keyOrPrefix(),
                            source.getReconnectDelay());
                    if (!sleep(source.getReconnectDelay())) {
                        break;
                    }
                }
            }
        } finally {
            connected.set(false);
            // The poll thread owns the store while it runs, so it is the thread that releases
            // it; close() closes it again afterwards, and the flag makes that a no-op.
            closeStore();
            log.info("[{}] S3 source stopped", connector.getName());
        }
    }

    /**
     * List once and emit whatever is new or changed.
     *
     * @param handler where the records go
     * @throws IOException when the listing or an object read fails, which ends this run
     */
    private void poll(RecordHandler handler) throws IOException {
        for (S3ObjectStore.ObjectRef ref : store.list()) {
            if (closed.get()) {
                return;
            }
            // containsKey rather than a null check on the value, so an object the store reports
            // without an ETag is read once and then left alone, instead of being re-emitted
            // every poll for as long as it sits in the bucket.
            if (seen.containsKey(ref.key()) && Objects.equals(seen.get(ref.key()), ref.etag())) {
                continue;
            }
            // Recorded only after the object has been read in full: a read that failed halfway
            // must be retried, not remembered as done.
            emit(ref.key(), handler);
            seen.put(ref.key(), ref.etag());
        }
    }

    /** Read one object and hand over its frames, in the order they sit in the object. */
    private void emit(String key, RecordHandler handler) throws IOException {
        log.debug("[{}] reading s3://{}/{}", connector.getName(), source.getBucket(), key);
        try (InputStream in = store.read(key)) {
            if (source.getFraming() == S3SourceProperties.Framing.WHOLE) {
                // The record IS the object, so there is nothing to stream it into.
                byte[] body = in.readAllBytes();
                emitFrame(body, 0, body.length, handler);
                return;
            }
            readDelimited(in, handler);
        }
    }

    /**
     * Split the object on the delimiter's byte sequence, a read at a time.
     *
     * <p>Bytes are accumulated rather than scanned per read, because a frame boundary is not a
     * read boundary: a record arrives split across two reads as readily as three records arrive
     * in one. What is held is therefore one record plus a chunk, not the object.
     */
    private void readDelimited(InputStream in, RecordHandler handler) throws IOException {
        ByteArrayOutputStream pending = new ByteArrayOutputStream();
        byte[] chunk = new byte[READ_BUFFER_BYTES];
        int read;
        while ((read = in.read(chunk)) >= 0) {
            if (closed.get()) {
                return;
            }
            pending.write(chunk, 0, read);
            byte[] buffered = pending.toByteArray();
            int start = 0;
            int index;
            while ((index = indexOf(buffered, start, delimiter)) >= 0) {
                emitFrame(buffered, start, index - start, handler);
                start = index + delimiter.length;
            }
            pending.reset();
            pending.write(buffered, start, buffered.length - start);
        }
        // The tail after the last delimiter. An object ends where the file ends, so this is a
        // record -- not a partial frame the way it would be on a socket.
        byte[] tail = pending.toByteArray();
        emitFrame(tail, 0, tail.length, handler);
    }

    /** First offset at or after {@code from} where {@code needle} appears, or {@code -1}. */
    private static int indexOf(byte[] haystack, int from, byte[] needle) {
        outer:
        for (int i = from; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    /** Hand one frame over, unless it is empty -- a trailing delimiter is not a record. */
    private void emitFrame(byte[] body, int offset, int length, RecordHandler handler) {
        if (length <= 0) {
            return;
        }
        try {
            handler.onRecord(SourceRecord.of(new String(body, offset, length, charset)));
        } catch (RuntimeException e) {
            log.error("[{}] failed to handle an S3 frame", connector.getName(), e);
        }
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public void close() {
        closed.set(true);
        connected.set(false);
        synchronized (idle) {
            idle.notifyAll();
        }
        Thread runner = this.thread;
        this.thread = null;
        if (runner != null && runner != Thread.currentThread()) {
            try {
                runner.join(CLOSE_JOIN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (runner.isAlive()) {
                // Still in an HTTP read with no monitor to ring. Closing the store under it is
                // what unblocks it, the way closing the socket unblocks the TCP source's read.
                log.warn("[{}] S3 poll thread did not stop within {}ms; closing the store "
                        + "under it", connector.getName(), CLOSE_JOIN_MILLIS);
            }
        }
        closeStore();
    }

    /** Idempotent: both {@link #close()} and the poll thread's exit call it. */
    private void closeStore() {
        if (!storeClosed.compareAndSet(false, true)) {
            return;
        }
        try {
            store.close();
        } catch (RuntimeException e) {
            log.debug("[{}] S3 store close failed", connector.getName(), e);
        }
    }

    /**
     * Wait out the poll interval or the reconnect backoff, returning early when
     * {@link #close()} rings the monitor.
     *
     * @param delay how long to wait
     * @return {@code false} if the source should stop instead of carrying on
     */
    private boolean sleep(Duration delay) {
        synchronized (idle) {
            if (closed.get()) {
                return false;
            }
            try {
                idle.wait(Math.max(1L, delay.toMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return !closed.get();
    }
}
