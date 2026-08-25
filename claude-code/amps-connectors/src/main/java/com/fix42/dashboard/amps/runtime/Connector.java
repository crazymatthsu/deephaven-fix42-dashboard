package com.fix42.dashboard.amps.runtime;

import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.UpdateMode;
import com.fix42.dashboard.amps.decode.RecordDecoder;
import com.fix42.dashboard.amps.deephaven.DeephavenGateway;
import com.fix42.dashboard.amps.mapping.DeltaRowMerger;
import com.fix42.dashboard.amps.mapping.FieldMapper;
import com.fix42.dashboard.amps.mapping.MappedRow;
import com.fix42.dashboard.amps.mapping.TableSchema;
import com.fix42.dashboard.amps.source.AmpsRecord;
import com.fix42.dashboard.amps.source.AmpsSubscriber;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One AMPS topic bridged into one Deephaven table.
 *
 * <p>The pipeline per message is: decode the payload for its format, map the configured fields
 * onto the target columns, optionally merge the result over the last row for that key, then
 * buffer it for the next batch publish.
 *
 * <p>Not started in its constructor: {@link ConnectorManager} starts and stops connectors as the
 * Deephaven server comes and goes, and every start replays from AMPS.
 */
public final class Connector implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Connector.class);

    private final ConnectorProperties properties;
    private final TableSchema schema;
    private final RecordDecoder decoder;
    private final FieldMapper mapper;
    private final DeltaRowMerger merger;
    private final RowBatcher batcher;
    private final DeephavenGateway gateway;
    private final SubscriberSupplier subscriberSupplier;
    private final Clock clock;

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();

    private volatile AmpsSubscriber subscriber;
    private volatile boolean started;

    /** Creates the connector's subscriber; a seam so tests can inject a fake source. */
    @FunctionalInterface
    public interface SubscriberSupplier {
        AmpsSubscriber get(ConnectorProperties connector);
    }

    public Connector(
            ConnectorProperties properties,
            RecordDecoder decoder,
            DeephavenGateway gateway,
            SubscriberSupplier subscriberSupplier,
            Clock clock) {
        this.properties = properties;
        this.schema = TableSchema.of(properties);
        this.decoder = decoder;
        this.gateway = gateway;
        this.subscriberSupplier = subscriberSupplier;
        this.clock = clock;
        this.mapper = new FieldMapper(schema);
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
        if (properties.getDeephaven().isCreateIfMissing()) {
            gateway.ensureTable(schema, properties.getName());
        }
        if (merger != null) {
            // The replay that follows is authoritative; anything remembered is from the last life.
            merger.clear();
        }
        batcher.discard();
        batcher.start();

        AmpsSubscriber source = subscriberSupplier.get(properties);
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
                properties.getSource().getTopic(),
                schema.keyed() ? "keyed" : "append-only",
                schema.tableName(),
                schema.size(),
                properties.getDeephaven().getPublishMode());
    }

    /** Unsubscribe, flush what is buffered, and release the subscriber. */
    public synchronized void stop() {
        if (!started) {
            return;
        }
        started = false;
        AmpsSubscriber current = subscriber;
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
        log.info("[{}] stopped after {} record(s), {} row(s) published, {} rejected",
                properties.getName(), received.get(), batcher.publishedRows(), rejected.get());
    }

    /** Handle one AMPS record. Never throws: a bad message must not kill the subscription. */
    void onRecord(AmpsRecord record) {
        received.incrementAndGet();
        try {
            Map<String, String> fields = record.action() == AmpsRecord.Action.DELETE
                    ? decodeQuietly(record)
                    : decoder.decode(record.data());
            MappedRow row = mapper.map(record, fields, clock.instant());
            if (merger != null) {
                row = merger.merge(row);
            }
            if (row.rowKey() == null && schema.keyed()) {
                rejected.incrementAndGet();
                log.debug("[{}] dropping record with no key value", properties.getName());
                return;
            }
            batcher.submit(row);
        } catch (RuntimeException e) {
            long count = rejected.incrementAndGet();
            if (count <= 10 || count % 1_000 == 0) {
                log.warn("[{}] rejected record #{}: {}", properties.getName(), count, e.getMessage());
            }
        }
    }

    /**
     * A delete may carry no body -- AMPS identifies the record by SOW key. Decode what is there
     * so key columns carried in the payload still resolve, but never fail on an empty one.
     */
    private Map<String, String> decodeQuietly(AmpsRecord record) {
        try {
            return decoder.decode(record.data());
        } catch (RuntimeException e) {
            return Map.of();
        }
    }

    /** Records received from AMPS since construction. */
    public long receivedRecords() {
        return received.get();
    }

    /** Records that could not be decoded or mapped. */
    public long rejectedRecords() {
        return rejected.get();
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
