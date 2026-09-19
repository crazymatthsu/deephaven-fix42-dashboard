package com.fix42.dashboard.connectors.runtime;

import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.UpdateMode;
import com.fix42.dashboard.connectors.decode.RecordDecoder;
import com.fix42.dashboard.connectors.deephaven.DeephavenGateway;
import com.fix42.dashboard.connectors.mapping.DeltaRowMerger;
import com.fix42.dashboard.connectors.mapping.FieldMapper;
import com.fix42.dashboard.connectors.mapping.MappedRow;
import com.fix42.dashboard.connectors.mapping.RecordExploder;
import com.fix42.dashboard.connectors.mapping.TableSchema;
import com.fix42.dashboard.connectors.source.RecordSource;
import com.fix42.dashboard.connectors.source.SourceRecord;
import com.fix42.dashboard.connectors.transform.RecordTransform;
import java.time.Clock;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One upstream feed bridged into one Deephaven table.
 *
 * <p>The pipeline per message is: decode the payload for its format, fold the configured
 * transforms over the decoded tags, map the configured fields onto the target columns,
 * optionally merge the result over the last row for that key, then buffer it for the next
 * batch publish.
 *
 * <p>Not started in its constructor: {@link ConnectorManager} starts and stops connectors as the
 * Deephaven server comes and goes, and every start replays the feed from the beginning.
 */
public final class Connector implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Connector.class);

    private final ConnectorProperties properties;
    private final TableSchema schema;
    private final RecordDecoder decoder;
    private final FieldMapper mapper;
    private final RecordExploder exploder;
    private final TransformSupplier transformSupplier;
    private final DeltaRowMerger merger;
    private final RowBatcher batcher;
    private final DeephavenGateway gateway;
    private final SubscriberSupplier subscriberSupplier;
    private final Clock clock;

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong ignoredRemovals = new AtomicLong();

    private volatile RecordSource subscriber;
    private volatile List<RecordTransform> transforms = List.of();
    private volatile boolean started;

    /** Creates the connector's source; a seam so tests can inject a fake one. */
    @FunctionalInterface
    public interface SubscriberSupplier {
        RecordSource get(ConnectorProperties connector);
    }

    /**
     * Resolves the connector's {@code transforms:} names to beans.
     *
     * <p>Called from {@link #start()} rather than the constructor so that an unknown name
     * fails this connector the way an unreachable broker does -- logged, retried on the next
     * health check -- instead of refusing to build the application.
     */
    @FunctionalInterface
    public interface TransformSupplier {
        List<RecordTransform> get(ConnectorProperties connector);
    }

    public Connector(
            ConnectorProperties properties,
            RecordDecoder decoder,
            DeephavenGateway gateway,
            SubscriberSupplier subscriberSupplier,
            Clock clock) {
        this(properties, decoder, gateway, subscriberSupplier, connector -> List.of(), clock);
    }

    public Connector(
            ConnectorProperties properties,
            RecordDecoder decoder,
            DeephavenGateway gateway,
            SubscriberSupplier subscriberSupplier,
            TransformSupplier transformSupplier,
            Clock clock) {
        this.properties = properties;
        this.schema = TableSchema.of(properties);
        this.decoder = decoder;
        this.gateway = gateway;
        this.subscriberSupplier = subscriberSupplier;
        this.transformSupplier = transformSupplier;
        this.clock = clock;
        this.mapper = new FieldMapper(schema);
        this.exploder = properties.getExplode() == null
                ? null
                : new RecordExploder(properties.getExplode(), schema, mapper);
        this.merger = properties.getDeephaven().getPublishMode() == UpdateMode.DELTA
                ? new DeltaRowMerger(schema)
                : null;
        this.batcher = new RowBatcher(
                properties.getName(),
                schema,
                gateway,
                properties.getDeephaven().getMaxBatchRows(),
                properties.getDeephaven().getFlushInterval());
    }

    public String name() {
        return properties.getName();
    }

    public TableSchema schema() {
        return schema;
    }

    public boolean isStarted() {
        return started;
    }

    /**
     * Create the target table if needed, then subscribe.
     *
     * <p>Every start is a full rehydration: a SOW topic replays its state of the world, a journal
     * topic replays from its bookmark.
     *
     * @throws Exception if the table could not be created or the subscription not established
     */
    public synchronized void start() throws Exception {
        if (started) {
            return;
        }
        // Before the table: a connector naming a transform that does not exist must not get
        // as far as creating anything.
        this.transforms = List.copyOf(transformSupplier.get(properties));
        if (properties.getDeephaven().isCreateIfMissing()) {
            gateway.ensureTable(schema, properties.getName());
        }
        if (merger != null) {
            // The replay that follows is authoritative; anything remembered is from the last life.
            merger.clear();
        }
        if (exploder != null) {
            exploder.clear();
        }
        batcher.discard();
        batcher.start();

        RecordSource source = subscriberSupplier.get(properties);
        try {
            source.start(this::onRecord);
        } catch (Exception e) {
            source.close();
            batcher.close();
            throw e;
        }
        this.subscriber = source;
        this.started = true;
        log.info("[{}] started: {} {} -> {} table {} ({} columns, publish {})",
                properties.getName(),
                properties.getFormat(),
                properties.getSource().describe(),
                schema.tableType().name().toLowerCase(Locale.ROOT).replace('_', '-'),
                schema.tableName(),
                schema.size(),
                properties.getDeephaven().getPublishMode());
        if (properties.getSource().stateful() && !schema.keyed()) {
            // Legal, and sometimes what you want -- a live view of updates rather than of state
            // -- but a stateful feed's removals have nowhere to land, so say so rather than
            // dropping them in silence.
            log.warn("[{}] {} is a {} table, so removals from stateful source {} "
                            + "cannot be applied and will be ignored",
                    properties.getName(), schema.tableName(), schema.tableType(),
                    properties.getSource().describe());
        }
    }

    /** Unsubscribe, flush what is buffered, and release the subscriber. */
    public synchronized void stop() {
        if (!started) {
            return;
        }
        started = false;
        RecordSource current = subscriber;
        subscriber = null;
        if (current != null) {
            current.close();
        }
        try {
            batcher.flush();
        } catch (RuntimeException e) {
            log.debug("[{}] final flush failed", properties.getName(), e);
        }
        batcher.close();
        log.info("[{}] stopped after {} record(s), {} row(s) published, {} rejected, {} dropped",
                properties.getName(), received.get(), batcher.publishedRows(), rejected.get(),
                dropped.get());
    }

    /** Handle one source record. Never throws: a bad message must not kill the subscription. */
    void onRecord(SourceRecord record) {
        received.incrementAndGet();
        try {
            if (record.action() == SourceRecord.Action.DELETE && !schema.keyed()) {
                // Nothing to remove from: an append-only table forbids deletion and a blink or
                // ring table has already let the row go. Counted, not published, so
                // publishedRows keeps meaning "rows that reached Deephaven".
                ignoredRemovals.incrementAndGet();
                return;
            }
            Map<String, String> fields = record.action() == SourceRecord.Action.DELETE
                    ? decodeQuietly(record)
                    : decoder.decode(record);
            fields = transform(fields, record);
            if (fields == null) {
                // A transform said no. Not a failure, so it is counted apart from `rejected`:
                // the two answer different questions when a table looks emptier than expected.
                dropped.incrementAndGet();
                return;
            }
            List<MappedRow> rows = exploder != null
                    ? exploder.explode(record, fields, clock.instant())
                    : List.of(mapper.map(record, fields, clock.instant()));
            for (MappedRow mapped : rows) {
                MappedRow row = merger != null ? merger.merge(mapped) : mapped;
                if (row.rowKey() == null && schema.keyed()) {
                    rejected.incrementAndGet();
                    log.debug("[{}] dropping record with no key value", properties.getName());
                    continue;
                }
                batcher.submit(row);
            }
        } catch (RuntimeException e) {
            long count = rejected.incrementAndGet();
            if (count <= 10 || count % 1_000 == 0) {
                log.warn("[{}] rejected record #{}: {}", properties.getName(), count, e.getMessage());
            }
        }
    }

    /**
     * Fold the configured transforms over the decoded tags, stopping at the first that drops
     * the record. Deletes go through too: their key columns come out of the same namespace,
     * so a derived key column has to be derived for them as well.
     *
     * @return the fields to publish, or {@code null} when a transform dropped the record
     */
    private Map<String, String> transform(Map<String, String> fields, SourceRecord record) {
        Map<String, String> current = fields;
        for (RecordTransform transform : transforms) {
            current = transform.apply(record, current);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    /**
     * A delete may carry no body -- the source identifies the record by its own key. Decode
     * what is there so key columns carried in the payload still resolve, but never fail on an
     * empty one.
     */
    private Map<String, String> decodeQuietly(SourceRecord record) {
        try {
            return decoder.decode(record);
        } catch (RuntimeException e) {
            return Map.of();
        }
    }

    /** Records received from the source since construction. */
    public long receivedRecords() {
        return received.get();
    }

    /** Records that could not be decoded or mapped. */
    public long rejectedRecords() {
        return rejected.get();
    }

    /** Records a {@link RecordTransform} deliberately discarded. */
    public long droppedRecords() {
        return dropped.get();
    }

    /** Removals dropped because the target table has no keys to remove by. */
    public long ignoredRemovals() {
        return ignoredRemovals.get();
    }

    /** Rows successfully published to Deephaven. */
    public long publishedRows() {
        return batcher.publishedRows();
    }

    @Override
    public void close() {
        stop();
    }
}
