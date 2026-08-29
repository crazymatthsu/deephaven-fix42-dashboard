package com.fix42.dashboard.dh;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import io.deephaven.stream.TablePublisher;
import io.deephaven.util.SafeCloseable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * One AMPS bookmark subscription feeding one {@code fix_raw} blink table -- port of
 * {@code dh_app.amps_ingest.AmpsRawSource}.
 *
 * <p>The Kafka source replays {@code fix42.messages} from offset 0 on every start so a restart
 * rebuilds the identical cache (doc 03 section 3.3). This is the same contract against an AMPS
 * transaction log: a bookmark subscription from {@code EPOCH} replays the whole journal and then
 * cuts over to live messages on the same subscription.
 *
 * <p>Shape of the bridge:
 *
 * <pre>
 * AMPS client thread          RawBuffer          update-graph thread
 * ------------------          ---------          -------------------
 * invoke(message)  --offer--&gt; [rows]  --drain--&gt; onFlush -&gt; publisher.add(batch)
 * </pre>
 *
 * <p>{@link TablePublisher}'s flush callback fires once at the start of each update-graph cycle,
 * which is exactly the batching hook this needs: the AMPS reader thread only appends to a list, and
 * one table per cycle is built on the update-graph thread.
 *
 * <p>Three translations from the python client are easy to get wrong and are called out where they
 * happen: the reversed {@code bookmarkSubscribe} parameter order, the bookmark store's
 * {@code discard} living on the store rather than the client, and {@code HAClient}'s default
 * no-op bookmark store.
 */
public final class AmpsRawSource {

    /** Every live source; a collected {@code AmpsRawSource} takes its client, and the stream, with it. */
    private static final List<AmpsRawSource> ACTIVE = Collections.synchronizedList(new ArrayList<>());

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString(AmpsConfig.RAW_COLUMN),
            ColumnDefinition.ofString("AmpsBookmark"),
            ColumnDefinition.ofTime("IngestTs"));

    private final AmpsConfig config;
    private final RawBuffer buffer;
    private final Function<AmpsConfig, HAClient> clientFactory;
    private final Supplier<Instant> now;

    private volatile HAClient client;
    private CommandId subId;
    private ExecutionContext context;
    private Table table;
    private long published;
    private long failedBatches;

    /** Creates an unstarted source with the default client factory and clock. */
    public AmpsRawSource(AmpsConfig config) {
        this(config, AmpsRawSource::defaultClient, Instant::now);
    }

    /**
     * Creates an unstarted source.
     *
     * @param config settings
     * @param clientFactory builds the AMPS client (injected for tests)
     * @param now the ingest clock (injected for tests)
     */
    public AmpsRawSource(AmpsConfig config, Function<AmpsConfig, HAClient> clientFactory, Supplier<Instant> now) {
        this.config = config;
        this.buffer = new RawBuffer(config.maxPending());
        this.clientFactory = clientFactory;
        this.now = now;
    }

    /**
     * Builds the {@code fix_raw} blink table backed by an AMPS transaction-log replay.
     *
     * @param config settings
     * @return a blink table with {@code RawFix}, {@code AmpsBookmark} and {@code IngestTs}
     */
    public static Table build(AmpsConfig config) {
        AmpsRawSource source = new AmpsRawSource(config);
        Table table = source.start();
        ACTIVE.add(source);
        return table;
    }

    /** The most recently built source, or {@code null} when the AMPS path is not in use. */
    public static AmpsRawSource activeSource() {
        synchronized (ACTIVE) {
            return ACTIVE.isEmpty() ? null : ACTIVE.get(ACTIVE.size() - 1);
        }
    }

    public AmpsConfig config() {
        return config;
    }

    public RawBuffer buffer() {
        return buffer;
    }

    /** Rows handed to the publisher since start. */
    public long published() {
        return published;
    }

    /** Flush cycles that failed to publish (each logged with its stack trace). */
    public long failedBatches() {
        return failedBatches;
    }

    // ------------------------------------------------------------------ lifecycle

    /** Creates the blink table and its publisher (no AMPS connection yet). */
    public Table buildTable() {
        // Captured on the setup thread and re-entered inside the flush callback, which runs on the
        // update-graph thread and builds tables there (doc 04 section 1).
        this.context = ExecutionContext.getContext();
        TablePublisher publisher = TablePublisher.of(
                "fix_raw_amps",
                DEFINITION,
                this::onFlush,
                this::stop,
                context.getUpdateGraph(),
                2048);
        this.table = publisher.table();
        return table;
    }

    /**
     * Connects, subscribes from the configured bookmark, and returns the table.
     *
     * @return the blink table
     * @throws IllegalStateException if the source was already started
     */
    public Table start() {
        if (client != null) {
            throw new IllegalStateException("AmpsRawSource.start() called twice");
        }
        if (table == null) {
            buildTable();
        }

        HAClient created = clientFactory.apply(config);
        this.client = created;
        try {
            created.addConnectionStateListener(state -> System.out.println("[fix42] AMPS connection state: " + state));
            created.connectAndLogon();
            String bookmark = AmpsConfig.resolveBookmark(
                    config.bookmark(), Client.Bookmarks.EPOCH, Client.Bookmarks.NOW, Client.Bookmarks.MOST_RECENT);
            // PARAMETER ORDER IS NOT THE PYTHON ONE. Java is
            //   (handler, topic, filter, subId, bookmark, options, timeout)
            // where python is (handler, topic, bookmark, filter). filter and bookmark are both
            // String, so transposing them compiles cleanly and then subscribes from a bookmark the
            // server reads as a filter. Verified against the 5.3.4.1 bytecode: argument 3 reaches
            // Command.setFilter and argument 5 reaches Command.setBookmark.
            this.subId = created.bookmarkSubscribe(
                    this::onMessage,
                    config.topic(),
                    config.filter(),
                    CommandId.nextIdentifier(),
                    bookmark,
                    null,
                    0L);
        } catch (Exception startFailure) {
            this.client = null;
            safeClose(created);
            throw new IllegalStateException("AMPS subscribe failed: " + config.describe(), startFailure);
        }
        System.out.println("[fix42] AMPS subscribed: " + config.describe());
        return table;
    }

    /** Unsubscribes and closes the client (best effort; never throws). */
    public void stop() {
        HAClient current = client;
        CommandId currentSub = subId;
        client = null;
        subId = null;
        buffer.close();
        if (current == null) {
            return;
        }
        try {
            if (currentSub != null) {
                current.unsubscribe(currentSub);
            }
        } catch (Exception shutdownFailure) { // shutdown must never raise
            shutdownFailure.printStackTrace();
        }
        safeClose(current);
    }

    private static void safeClose(HAClient client) {
        try {
            client.close();
        } catch (RuntimeException shutdownFailure) { // shutdown must never raise
            shutdownFailure.printStackTrace();
        }
    }

    // ------------------------------------------------------------------ callbacks

    /**
     * AMPS client thread: buffers one message's payload.
     *
     * <p>Discards the bookmark once buffered. Without the discard, an HA reconnect replays from the
     * oldest undiscarded bookmark -- the whole journal, on every blip. Losing the buffer means the
     * process died, and a fresh process starts with an empty memory bookmark store and replays from
     * {@code EPOCH} anyway, so nothing is lost by discarding here.
     */
    private void onMessage(Message message) {
        try {
            // Copy out before anything else: AMPS reuses one Message instance backed by the socket
            // buffer, so retaining it (or its Fields) past this call reads whatever arrived next.
            String raw = message.getData();
            if (raw == null || raw.isEmpty()) {
                return;
            }
            String bookmark = message.getBookmark();
            buffer.offer(new RawBuffer.Row(raw, bookmark == null ? "" : bookmark, now.get()));
        } catch (RuntimeException handlerFailure) { // a handler exception would kill the subscription
            handlerFailure.printStackTrace();
        } finally {
            discard(message);
        }
    }

    /** Releases {@code message} from the local bookmark store. */
    private void discard(Message message) {
        HAClient current = client;
        if (current == null) {
            return;
        }
        try {
            // python has Client.discard(message); the java client keeps it on the store.
            current.getBookmarkStore().discard(message);
        } catch (Exception bookkeepingFailure) { // never fail the reader thread on bookkeeping
            bookkeepingFailure.printStackTrace();
        }
    }

    /**
     * Update-graph thread: publishes everything buffered since the last cycle.
     *
     * <p>This blocks the update cycle, so it does exactly one drain and one add, and never throws.
     */
    private void onFlush(TablePublisher publisher) {
        try {
            List<RawBuffer.Row> rows = buffer.drain();
            if (rows.isEmpty()) {
                return;
            }
            try (SafeCloseable ignored = context.open()) {
                publisher.add(buildBatch(rows));
            }
            published += rows.size();
        } catch (Exception flushFailure) { // a flush exception would stall the update graph
            failedBatches++;
            flushFailure.printStackTrace();
        }
    }

    /** Turns buffered rows into one static table matching {@link AmpsConfig#COLUMN_NAMES}. */
    private static Table buildBatch(List<RawBuffer.Row> rows) {
        String[] raw = new String[rows.size()];
        String[] bookmarks = new String[rows.size()];
        Instant[] ingestTs = new Instant[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            RawBuffer.Row row = rows.get(i);
            raw[i] = row.rawFix();
            bookmarks[i] = row.bookmark();
            ingestTs[i] = row.ingestTs();
        }
        return TableTools.newTable(
                DEFINITION,
                TableTools.stringCol(AmpsConfig.RAW_COLUMN, raw),
                TableTools.stringCol("AmpsBookmark", bookmarks),
                TableTools.instantCol("IngestTs", ingestTs));
    }

    /**
     * Builds an {@link HAClient} wired to {@code config}'s server list.
     *
     * <p>{@code HAClient.createMemoryBacked} rather than {@code new HAClient(name)}: the plain
     * constructor installs a <em>no-op</em> bookmark store, which would make the {@code EPOCH}
     * subscription unable to resume after a reconnect. The python client's default is already
     * memory-backed, so this is what keeps the two behaviours the same.
     */
    private static HAClient defaultClient(AmpsConfig config) {
        try {
            HAClient client = HAClient.createMemoryBacked(config.clientName());
            DefaultServerChooser chooser = new DefaultServerChooser();
            for (String uri : config.uris()) {
                chooser.add(uri);
            }
            client.setServerChooser(chooser);
            return client;
        } catch (Exception constructionFailure) {
            throw new IllegalStateException(
                    "could not build the AMPS client for " + config.describe(), constructionFailure);
        }
    }
}
