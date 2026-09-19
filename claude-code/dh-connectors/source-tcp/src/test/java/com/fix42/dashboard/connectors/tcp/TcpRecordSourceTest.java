package com.fix42.dashboard.connectors.tcp;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.SourceFormat;
import com.fix42.dashboard.connectors.config.SourceProperties;
import com.fix42.dashboard.connectors.config.TcpSourceProperties;
import com.fix42.dashboard.connectors.source.SourceRecord;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The TCP source against a loopback server started by the test itself.
 *
 * <p>Self-contained on purpose: a socket needs no broker, so the framing rules -- the part
 * that actually goes wrong -- can be exercised against real bytes on a real connection
 * instead of a mock stream.
 */
class TcpRecordSourceTest {

    /** One connection's worth of output. */
    @FunctionalInterface
    private interface Session {
        void serve(OutputStream out) throws IOException;
    }

    /**
     * A loopback server that serves each scripted session on its own connection and then
     * hangs up -- which is what gives the reconnect test something to reconnect to.
     */
    private static final class Feed implements AutoCloseable {

        private final ServerSocket server;
        private final AtomicInteger served = new AtomicInteger();

        Feed(Session... sessions) throws IOException {
            this.server = new ServerSocket(0, 4, InetAddress.getLoopbackAddress());
            Thread thread = new Thread(() -> {
                for (Session session : sessions) {
                    try (Socket client = server.accept()) {
                        OutputStream out = client.getOutputStream();
                        session.serve(out);
                        out.flush();
                        served.incrementAndGet();
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "loopback-feed");
            thread.setDaemon(true);
            thread.start();
        }

        int port() {
            return server.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    // ---- fixtures ------------------------------------------------------------------

    private static ConnectorProperties connector(
            int port, TcpSourceProperties.Framing framing, String delimiter) {
        TcpSourceProperties tcp = new TcpSourceProperties();
        tcp.setHost("127.0.0.1");
        tcp.setPort(port);
        tcp.setFraming(framing);
        tcp.setDelimiter(delimiter);
        tcp.setConnectTimeout(Duration.ofSeconds(2));
        tcp.setReconnectDelay(Duration.ofMillis(100));

        SourceProperties source = new SourceProperties();
        source.setTcp(tcp);

        ConnectorProperties connector = new ConnectorProperties();
        connector.setName("ticks-tcp");
        connector.setFormat(SourceFormat.JSON);
        connector.setSource(source);
        return connector;
    }

    private static ConnectorProperties delimited(int port) {
        return connector(port, TcpSourceProperties.Framing.DELIMITED, "\n");
    }

    private static void write(OutputStream out, String text) throws IOException {
        out.write(text.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    /** Frame each payload as a 4-byte big-endian length followed by its bytes. */
    private static byte[] lengthPrefixed(String... frames) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String frame : frames) {
            byte[] payload = frame.getBytes(StandardCharsets.UTF_8);
            out.writeBytes(ByteBuffer.allocate(Integer.BYTES)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt(payload.length)
                    .array());
            out.writeBytes(payload);
        }
        return out.toByteArray();
    }

    private static void awaitRecords(List<SourceRecord> received, int count) {
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> received.size() >= count);
    }

    // ---- delimited framing ----------------------------------------------------------

    @Test
    @DisplayName("delimited frames arrive as keyless upserts -- a raw feed has no state")
    void delimitedFramesBecomeKeylessUpserts() throws Exception {
        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (Feed feed = new Feed(out -> write(out, "{\"px\":1}\n{\"px\":2}\n"));
                TcpRecordSource source = new TcpRecordSource(delimited(feed.port()))) {
            source.start(received::add);
            awaitRecords(received, 2);

            assertThat(received).extracting(SourceRecord::data)
                    .containsExactly("{\"px\":1}", "{\"px\":2}");
            assertThat(received).extracting(SourceRecord::key).containsOnlyNulls();
            // No key and never a DELETE: the two things this transport cannot express.
            assertThat(received).extracting(SourceRecord::action)
                    .containsOnly(SourceRecord.Action.UPSERT);
        }
    }

    @Test
    @DisplayName("a frame split across two writes is reassembled, not delivered in halves")
    void aFrameSplitAcrossWritesIsReassembled() throws Exception {
        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        Session split = out -> {
            write(out, "{\"px\":");
            // Long enough that the reader really does return the partial frame first.
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            write(out, "42}\n");
        };
        try (Feed feed = new Feed(split);
                TcpRecordSource source = new TcpRecordSource(delimited(feed.port()))) {
            source.start(received::add);
            awaitRecords(received, 1);

            assertThat(received).extracting(SourceRecord::data).containsExactly("{\"px\":42}");
        }
    }

    @Test
    @DisplayName("a multi-byte delimiter (CRLF) is matched as a byte sequence")
    void aMultiByteDelimiterSplitsFrames() throws Exception {
        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        ConnectorProperties connector =
                connector(0, TcpSourceProperties.Framing.DELIMITED, "\r\n");
        try (Feed feed = new Feed(out -> write(out, "one\r\ntwo\r\nthree\r\n"))) {
            connector.getSource().getTcp().setPort(feed.port());
            try (TcpRecordSource source = new TcpRecordSource(connector)) {
                source.start(received::add);
                awaitRecords(received, 3);

                // Not split on the bare CR or LF, and not carrying either of them along.
                assertThat(received).extracting(SourceRecord::data)
                        .containsExactly("one", "two", "three");
            }
        }
    }

    @Test
    @DisplayName("an empty frame is skipped -- a trailing delimiter is not a message")
    void emptyFramesAreSkipped() throws Exception {
        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (Feed feed = new Feed(out -> write(out, "alpha\n\nbravo\n"));
                TcpRecordSource source = new TcpRecordSource(delimited(feed.port()))) {
            source.start(received::add);
            awaitRecords(received, 2);

            assertThat(received).extracting(SourceRecord::data).containsExactly("alpha", "bravo");
        }
    }

    // ---- length-prefixed framing -----------------------------------------------------

    @Test
    @DisplayName("length-prefixed frames are read by their 4-byte big-endian length")
    void lengthPrefixedFramesAreRead() throws Exception {
        byte[] wire = lengthPrefixed("hello", "{\"px\":7}");
        // The prefix really is big-endian: 5 lands in the LAST of the four bytes.
        assertThat(wire[0]).isEqualTo((byte) 0);
        assertThat(wire[1]).isEqualTo((byte) 0);
        assertThat(wire[2]).isEqualTo((byte) 0);
        assertThat(wire[3]).isEqualTo((byte) 5);

        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        ConnectorProperties connector =
                connector(0, TcpSourceProperties.Framing.LENGTH_PREFIXED, "\n");
        try (Feed feed = new Feed(out -> {
            out.write(wire);
            out.flush();
        })) {
            connector.getSource().getTcp().setPort(feed.port());
            try (TcpRecordSource source = new TcpRecordSource(connector)) {
                source.start(received::add);
                awaitRecords(received, 2);

                assertThat(received).extracting(SourceRecord::data)
                        .containsExactly("hello", "{\"px\":7}");
            }
        }
    }

    // ---- lifecycle --------------------------------------------------------------------

    @Test
    @DisplayName("a dropped connection is redialled and the feed resumes")
    void aDroppedConnectionIsRedialled() throws Exception {
        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        // Two sessions: the server hangs up after the first, which is the EOF the source
        // has to treat as "redial", not as "done".
        try (Feed feed = new Feed(
                out -> write(out, "before\n"),
                out -> write(out, "after\n"));
                TcpRecordSource source = new TcpRecordSource(delimited(feed.port()))) {
            source.start(received::add);
            awaitRecords(received, 2);

            assertThat(received).extracting(SourceRecord::data)
                    .containsExactly("before", "after");
        }
    }

    @Test
    @DisplayName("close() unblocks the read and stops the thread")
    void closeStopsTheReaderThread() throws Exception {
        List<SourceRecord> received = new CopyOnWriteArrayList<>();
        try (Feed feed = new Feed(out -> write(out, "alpha\n"))) {
            TcpRecordSource source = new TcpRecordSource(delimited(feed.port()));
            source.start(received::add);
            awaitRecords(received, 1);

            source.close();

            assertThat(source.isConnected()).isFalse();
            assertThat(Thread.getAllStackTraces().keySet())
                    .as("no connector thread is left behind")
                    .noneMatch(thread -> thread.getName().equals("ticks-tcp-tcp"));
            // Idempotent, the way ConnectorManager calls it.
            source.close();
        }
    }
}
