package com.fix42.dashboard.dh;

import com.fix42.dashboard.fixcache.ExecutionRow;
import com.fix42.dashboard.fixcache.MessageRow;
import com.fix42.dashboard.fixcache.OrderEventRow;
import com.fix42.dashboard.fixcache.OrderState;
import com.fix42.dashboard.fixcache.OrderStateMachine;
import com.fix42.dashboard.fixcache.Result;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.table.iterators.ColumnIterator;
import io.deephaven.util.SafeCloseable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The single stateful DAG node: FIX state-machine listener plus publishers.
 *
 * <p>Doc 03 sections 2.2/2.3, and the Java port of {@code dh_app.pipeline.Pipeline}. A
 * {@link TableUpdateListener} folds every {@code RawFix} value of the {@code fix_raw} blink table
 * through one {@link OrderStateMachine} and republishes the resulting normalized rows through five
 * {@link BlinkStream}s.
 *
 * <p>Invariants (doc 04 section 9 gotchas):
 *
 * <ol>
 *   <li>Strong references to the listener, publishers and blink tables are held by this instance,
 *       which the application keeps alive.
 *   <li>Table construction inside the listener happens under the execution context captured at
 *       construction time.
 *   <li>One batch per publisher per update cycle -- never one {@code add()} per row.
 *   <li>Nothing escapes the callback: per-message failures land in {@code ingest_errors} and are
 *       printed to the server log.
 * </ol>
 *
 * <p>This class contains <b>no FIX business logic</b>; it only adapts {@code fixcache} rows to
 * Deephaven columns.
 */
public final class Fix42Pipeline {

    /** The one column the fold reads; every other {@code fix_raw} column is source bookkeeping. */
    static final String RAW_COLUMN = "RawFix";

    private final OrderStateMachine machine;
    private final ExecutionContext context;
    private final Map<String, BlinkStream> streams = new LinkedHashMap<>();

    private TableUpdateListener listener;
    private Table source;
    private ColumnSource<String> rawColumn;
    // Written on the update-graph thread, read from a console or client thread (the shim exposes
    // them on the banner and for diagnostics), so publish the writes.
    private volatile long processed;
    private volatile long failed;

    /** Creates the publishers and captures the execution context, with a fresh state machine. */
    public Fix42Pipeline() {
        this(new OrderStateMachine());
    }

    /**
     * Creates the publishers and captures the execution context.
     *
     * @param machine the state machine driving the fold (injectable for tests)
     */
    public Fix42Pipeline(OrderStateMachine machine) {
        this.machine = machine;
        // Captured here (the setup thread) and opened inside the listener callback so tables can be
        // created on the update-graph thread. Doc 04 section 1.
        this.context = ExecutionContext.getContext();
        Schemas.ALL.forEach((name, definition) -> streams.put(name, new BlinkStream(name, definition)));
    }

    /** The single {@link OrderStateMachine} instance driving the fold. */
    public OrderStateMachine machine() {
        return machine;
    }

    /** Messages successfully folded into the cache. */
    public long processedCount() {
        return processed;
    }

    /** Messages routed to {@code ingest_errors}. */
    public long failedCount() {
        return failed;
    }

    /** The blink tables keyed by their doc 03 section 2.3 names. */
    public Map<String, Table> tables() {
        Map<String, Table> tables = new LinkedHashMap<>();
        streams.forEach((name, stream) -> tables.put(name, stream.table()));
        return Collections.unmodifiableMap(tables);
    }

    /**
     * Subscribes to {@code fixRaw} and begins publishing.
     *
     * @param fixRaw the blink table from {@link Ingest#buildFixRaw()}
     * @return the blink tables keyed by their doc 03 section 2.3 names
     * @throws IllegalStateException if the pipeline was already started
     */
    public Map<String, Table> start(Table fixRaw) {
        if (listener != null) {
            throw new IllegalStateException("Fix42Pipeline.start() called twice");
        }
        if (!fixRaw.isRefreshing()) {
            // Table.addUpdateListener silently no-ops on a static table, which would leave every
            // downstream node permanently empty with nothing in the log to explain it.
            throw new IllegalStateException(
                    "fix_raw is not refreshing -- ingest is misconfigured (is the source reachable?)");
        }
        this.source = fixRaw;
        this.rawColumn = fixRaw.getColumnSource(RAW_COLUMN, String.class);
        // retain=true is load-bearing: BaseTable holds its child listeners weakly, so a listener
        // the adapter does not retain is collected at the first GC and the stream stops silently.
        // The strong field reference below is the second half of the same guarantee.
        this.listener = new InstrumentedTableUpdateListenerAdapter("fix42-state-machine", fixRaw, true) {
            @Override
            public void onUpdate(TableUpdate update) {
                Fix42Pipeline.this.onUpdate(update);
            }
        };
        try (SafeCloseable ignored = fixRaw.getUpdateGraph().sharedLock().lockCloseable()) {
            fixRaw.addUpdateListener(listener);
        }
        return tables();
    }

    /**
     * Unsubscribes the listener (best effort; used by tests and reloads).
     *
     * <p>The listener was created with {@code retain=true}, so it also sits in
     * {@code InstrumentedTableUpdateListenerAdapter}'s static retention cache until it is destroyed;
     * removing the subscription does not evict it. The app wires exactly one pipeline per process
     * and memoizes it, so that is a bounded one-off rather than a leak -- but a caller that started
     * and stopped pipelines in a loop would accumulate them.
     */
    public void stop() {
        TableUpdateListener current = listener;
        listener = null;
        if (current == null || source == null) {
            return;
        }
        try (SafeCloseable ignored = source.getUpdateGraph().sharedLock().lockCloseable()) {
            source.removeUpdateListener(current);
        } catch (RuntimeException shutdownFailure) { // shutdown must never raise
            shutdownFailure.printStackTrace();
        }
    }

    // ------------------------------------------------------------------ the fold

    /**
     * Table-listener callback: folds added rows and publishes one batch per stream.
     *
     * <p>Runs on the update-graph thread; it is O(added rows) and never throws.
     */
    private void onUpdate(TableUpdate update) {
        try {
            // Borrowed from the update -- never close it. RowSet is SafeCloseable, so a
            // try-with-resources here would compile and corrupt the engine; only the iterator below
            // is ours to close.
            RowSet added = update.added();
            if (added == null || added.isEmpty()) {
                return;
            }
            try (SafeCloseable ignored = context.open();
                    ColumnIterator<String> raw = ChunkedColumnIterator.make(rawColumn, added)) {
                processBatch(raw);
            }
        } catch (Throwable listenerFailure) {
            // Throwable, not Exception: anything escaping onUpdate fails the listener and every
            // node downstream of it, for the life of the process. One poisonous message must cost
            // one message.
            listenerFailure.printStackTrace();
        }
    }

    /** Runs the state machine over the cycle's added rows and publishes the accumulated batches. */
    private void processBatch(ColumnIterator<String> rawValues) {
        List<Map<String, Object>> stateRows = new ArrayList<>();
        List<Map<String, Object>> executionRows = new ArrayList<>();
        List<Map<String, Object>> eventRows = new ArrayList<>();
        List<Map<String, Object>> messageRows = new ArrayList<>();
        List<Map<String, Object>> errorRows = new ArrayList<>();

        while (rawValues.hasNext()) {
            String raw = BlinkStream.asString(rawValues.next());
            if (raw.isEmpty()) {
                continue;
            }
            Result result;
            try {
                result = machine.process(raw);
            } catch (RuntimeException unexpected) { // process() is total; defensive
                errorRows.add(errorRow(raw, unexpected.getClass().getSimpleName() + ": " + unexpected.getMessage()));
                failed++;
                unexpected.printStackTrace();
                continue;
            }

            // python's `if error:` is falsy for "" as well as None. No code path produces an empty
            // error today, but matching the test keeps the two folds identical by construction
            // rather than by audit.
            if (result.error() != null && !result.error().isEmpty()) {
                errorRows.add(errorRow(raw, result.error()));
                failed++;
                continue;
            }

            collect(result, stateRows, executionRows, eventRows, messageRows);
            processed++;
        }

        publish(Names.ORDER_STATE_BLINK, stateRows, errorRows);
        publish(Names.EXECUTIONS_BLINK, executionRows, errorRows);
        publish(Names.ORDER_EVENTS_BLINK, eventRows, errorRows);
        publish(Names.FIX_MESSAGES_BLINK, messageRows, errorRows);
        // Errors last: the list may have grown while publishing the other four streams.
        publish(Names.INGEST_ERRORS, errorRows, null);
    }

    /** Appends a {@link Result}'s rows to the per-cycle accumulators. */
    private static void collect(
            Result result,
            List<Map<String, Object>> stateRows,
            List<Map<String, Object>> executionRows,
            List<Map<String, Object>> eventRows,
            List<Map<String, Object>> messageRows) {
        OrderState state = result.state();
        if (state != null) {
            stateRows.add(state.toRow());
        }
        for (ExecutionRow execution : result.executions()) {
            executionRows.add(execution.toRow());
        }
        for (OrderEventRow event : result.events()) {
            eventRows.add(event.toRow());
        }
        MessageRow message = result.message();
        if (message != null) {
            messageRows.add(message.toRow());
        }
    }

    /** Publishes one batch, diverting build/add failures to {@code errorSink}. */
    private void publish(String streamName, List<Map<String, Object>> rows, List<Map<String, Object>> errorSink) {
        if (rows.isEmpty()) {
            return;
        }
        try {
            streams.get(streamName).publish(rows);
        } catch (RuntimeException publishFailure) { // one bad batch must not stop ingest
            publishFailure.printStackTrace();
            String message = "publish to " + streamName + " failed: "
                    + publishFailure.getClass().getSimpleName() + ": " + publishFailure.getMessage();
            if (errorSink != null) {
                errorSink.add(errorRow("", message));
            } else {
                System.out.println("[fix42] " + message);
            }
        }
    }

    /** Builds one {@code ingest_errors} row (and echoes it to the server log). */
    private static Map<String, Object> errorRow(String raw, String error) {
        System.out.println("[fix42] ingest error: " + error + " | raw="
                + (raw.length() > 200 ? raw.substring(0, 200) : raw));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("RawFix", raw);
        row.put("Error", error);
        row.put("IngestTs", Instant.now());
        return row;
    }
}
