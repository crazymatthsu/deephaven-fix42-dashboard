package com.fix42.dashboard.connectors.tcp;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.TcpSourceProperties;
import com.fix42.dashboard.connectors.source.RecordHandler;
import com.fix42.dashboard.connectors.source.RecordSource;
import com.fix42.dashboard.connectors.source.SourceRecord;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RecordSource} over a raw TCP socket.
 *
 * <p>Reads its endpoint and framing from {@code source.tcp}; the wire format stays on the
 * connector, because the framing says where a message ends and the format says what is inside
 * it. Two framings cover the feeds that are worth dialling directly:
 *
 * <table border="1">
 *   <caption>Framing</caption>
 *   <tr><th>{@code framing}</th><th>on the wire</th></tr>
 *   <tr><td>{@code DELIMITED}</td>
 *       <td>messages separated by {@code delimiter}, in the configured charset -- a
 *           multi-byte separator is matched as a byte sequence, so CRLF works</td></tr>
 *   <tr><td>{@code LENGTH_PREFIXED}</td>
 *       <td>a 4-byte big-endian length, then that many payload bytes -- the same framing
 *           AMPS composite message parts use</td></tr>
 * </table>
 *
 * <h2>A socket has no state to replay</h2>
 *
 * <p>Every frame becomes an {@code UPSERT} {@link SourceRecord} with <strong>no key</strong>,
 * and this source never produces a {@code DELETE}: a raw feed carries no per-message key and
 * no notion of a record leaving it, so there is nothing a removal could address. That is why
 * {@code RING}, {@code APPEND_ONLY} and {@code BLINK} are its natural target tables -- a
 * {@code KEYED} table would need key columns from the payload, and the validator refuses
 * {@code deephaven.source-key-column} here outright.
 *
 * <p>It follows that a reconnect replays nothing. Where the AMPS source resubscribes into a
 * SOW replay and the Kafka source re-seeks, this one simply redials and picks up the live
 * stream: whatever the feed published while the socket was down is gone. A feed that must
 * survive a restart belongs behind a broker, not behind this source.
 *
 * <p>{@link #start} returns as soon as the reader thread is running, rather than dialling on
 * the caller's thread the way {@code AmpsRecordSource} does. A feed that is not listening yet
 * is the same event as one that hung up, and both belong in the same backoff -- so
 * {@link #isConnected()}, not a thrown {@code start}, is what reports the connection.
 */
public class TcpRecordSource implements RecordSource {

    private static final Logger log = LoggerFactory.getLogger(TcpRecordSource.class);

    /**
     * Sanity bound on a {@code LENGTH_PREFIXED} frame. A length this large means the stream
     * is misframed (a wrong endianness, a missed byte) rather than a genuinely huge message,
     * and allocating on it is how a misframe turns into an OutOfMemoryError.
     */
    private static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;

    /** Read chunk; frames are assembled above it, so this only sets the syscall size. */
    private static final int READ_BUFFER_BYTES = 8192;

    /** How long {@link #close()} waits for the reader thread before giving up on it. */
    private static final long CLOSE_JOIN_MILLIS = 5_000;

    private final ConnectorProperties connector;
    private final TcpSourceProperties source;
    private final Charset charset;
    private final byte[] delimiter;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);

    /** Monitor the reconnect backoff waits on, so {@link #close()} cuts it short. */
    private final Object backoff = new Object();

    private volatile Socket socket;
    private volatile Thread thread;

    public TcpRecordSource(ConnectorProperties connector) {
        this.connector = connector;
        this.source = connector.getSource().getTcp();
        // Resolved once, here: an unknown charset is a configuration error, and failing at
        // construction reports it against the connector instead of once per redial.
        this.charset = Charset.forName(source.getCharset());
        this.delimiter = source.getDelimiter().getBytes(charset);
    }

    @Override
    public void start(RecordHandler handler) {
        log.info("[{}] starting TCP source: {}:{} ({} framing, {})", connector.getName(),
                source.getHost(), source.getPort(), source.getFraming(), charset.name());
        Thread runner = new Thread(() -> run(handler), connector.getName() + "-tcp");
        runner.setDaemon(true);
        this.thread = runner;
        runner.start();
    }

    /**
     * Dial, read until the peer or {@link #close()} ends it, back off, dial again. One
     * iteration of the outer loop is one socket's lifetime.
     */
    private void run(RecordHandler handler) {
        while (!closed.get()) {
            try (Socket client = new Socket()) {
                this.socket = client;
                if (closed.get()) {
                    // close() publishes `closed` and then reads `socket`; this reads them the
                    // other way round, so between the two at least one of us sees the other.
                    // Without the check, a close landing in this window would leave a socket
                    // nobody can unblock.
                    break;
                }
                client.connect(new InetSocketAddress(source.getHost(), source.getPort()),
                        (int) source.getConnectTimeout().toMillis());
                connected.set(true);
                log.info("[{}] connected to {}:{}",
                        connector.getName(), source.getHost(), source.getPort());
                read(client.getInputStream(), handler);
                if (!closed.get()) {
                    log.warn("[{}] {}:{} closed the connection",
                            connector.getName(), source.getHost(), source.getPort());
                }
            } catch (IOException e) {
                if (!closed.get()) {
                    log.error("[{}] TCP source failed on {}:{}", connector.getName(),
                            source.getHost(), source.getPort(), e);
                }
            } finally {
                connected.set(false);
                this.socket = null;
            }
            if (!closed.get()) {
                log.info("[{}] redialling {}:{} in {} (a raw feed replays nothing)",
                        connector.getName(), source.getHost(), source.getPort(),
                        source.getReconnectDelay());
                if (!sleep(source.getReconnectDelay())) {
                    break;
                }
            }
        }
        connected.set(false);
        log.info("[{}] TCP source stopped", connector.getName());
    }

    private void read(InputStream in, RecordHandler handler) throws IOException {
        if (source.getFraming() == TcpSourceProperties.Framing.LENGTH_PREFIXED) {
            readLengthPrefixed(in, handler);
        } else {
            readDelimited(in, handler);
        }
    }

    /**
     * Split the stream on the delimiter's byte sequence.
     *
     * <p>Bytes are accumulated rather than scanned per read, because a frame boundary is not a
     * read boundary: a message arrives split across two reads as readily as three messages
     * arrive in one. The leftover after the last delimiter is carried into the next read.
     */
    private void readDelimited(InputStream in, RecordHandler handler) throws IOException {
        ByteArrayOutputStream pending = new ByteArrayOutputStream();
        byte[] chunk = new byte[READ_BUFFER_BYTES];
        while (!closed.get()) {
            int read = in.read(chunk);
            if (read < 0) {
                return;
            }
            pending.write(chunk, 0, read);
            byte[] buffered = pending.toByteArray();
            int start = 0;
            int index;
            while ((index = indexOf(buffered, start, delimiter)) >= 0) {
                emit(buffered, start, index - start, handler);
                start = index + delimiter.length;
            }
            pending.reset();
            pending.write(buffered, start, buffered.length - start);
        }
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

    /**
     * Read 4-byte-length-prefixed frames.
     *
     * <p>A length outside {@code 0..}{@value #MAX_FRAME_BYTES} is treated as a protocol error
     * rather than a big message: once the stream is misframed every following length is
     * garbage too, and the only recovery is a fresh connection.
     */
    private void readLengthPrefixed(InputStream in, RecordHandler handler) throws IOException {
        DataInputStream data = new DataInputStream(new BufferedInputStream(in));
        byte[] header = new byte[Integer.BYTES];
        while (!closed.get()) {
            try {
                data.readFully(header);
            } catch (EOFException e) {
                return;
            }
            int length = ByteBuffer.wrap(header).getInt();
            if (length < 0 || length > MAX_FRAME_BYTES) {
                throw new IOException("frame length " + length + " out of range (0.."
                        + MAX_FRAME_BYTES + "): the stream is misframed");
            }
            if (length == 0) {
                continue;
            }
            byte[] payload = new byte[length];
            data.readFully(payload);
            emit(payload, 0, length, handler);
        }
    }

    /** Hand one frame over, unless it is empty -- a trailing delimiter is not a message. */
    private void emit(byte[] buffer, int offset, int length, RecordHandler handler) {
        if (length <= 0) {
            return;
        }
        try {
            handler.onRecord(SourceRecord.of(new String(buffer, offset, length, charset)));
        } catch (RuntimeException e) {
            log.error("[{}] failed to handle TCP frame", connector.getName(), e);
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
        Socket open = this.socket;
        this.socket = null;
        if (open != null) {
            try {
                // The read blocks with no timeout; closing the socket under it is what
                // unblocks it.
                open.close();
            } catch (IOException e) {
                log.debug("[{}] TCP socket close failed", connector.getName(), e);
            }
        }
        synchronized (backoff) {
            backoff.notifyAll();
        }
        Thread runner = this.thread;
        this.thread = null;
        if (runner == null || runner == Thread.currentThread()) {
            return;
        }
        try {
            runner.join(CLOSE_JOIN_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        if (runner.isAlive()) {
            log.warn("[{}] TCP reader thread did not stop within {}ms",
                    connector.getName(), CLOSE_JOIN_MILLIS);
        }
    }

    /**
     * Wait out the reconnect backoff, returning early when {@link #close()} rings the monitor.
     *
     * @param delay the configured backoff
     * @return {@code false} if the source should stop instead of redialling
     */
    private boolean sleep(Duration delay) {
        synchronized (backoff) {
            if (closed.get()) {
                return false;
            }
            try {
                backoff.wait(Math.max(1L, delay.toMillis()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return !closed.get();
    }
}
