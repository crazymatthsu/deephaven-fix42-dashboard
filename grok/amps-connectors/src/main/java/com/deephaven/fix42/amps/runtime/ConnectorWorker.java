package com.deephaven.fix42.amps.runtime;

import com.deephaven.fix42.amps.amps.AmpsClientAdapter;
import com.deephaven.fix42.amps.amps.AmpsInboundMessage;
import com.deephaven.fix42.amps.amps.SubscriptionSpec;
import com.deephaven.fix42.amps.amps.SubscriptionSpecFactory;
import com.deephaven.fix42.amps.config.AmpsProperties;
import com.deephaven.fix42.amps.config.ConnectorProperties;
import com.deephaven.fix42.amps.dh.DeephavenInputTableSink;
import io.deephaven.client.impl.FlightSession;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One configured AMPS→Deephaven mapping. Subscribe after Deephaven is up so SOW/journal
 * replay rehydrates the table.
 */
public final class ConnectorWorker implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ConnectorWorker.class);

    private final AmpsProperties amps;
    private final ConnectorProperties connector;
    private final ConnectorRuntime runtime;
    private final DeephavenInputTableSink sink;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private AmpsClientAdapter ampsClient;

    public ConnectorWorker(
            AmpsProperties amps,
            ConnectorProperties connector,
            ConnectorRuntime runtime,
            DeephavenInputTableSink sink) {
        this.amps = amps;
        this.connector = connector;
        this.runtime = runtime;
        this.sink = sink;
    }

    public static ConnectorWorker start(
            AmpsProperties amps,
            ConnectorProperties connector,
            ConnectorRuntimeFactory runtimes,
            FlightSession flight,
            BufferAllocator allocator,
            Duration publishTimeout)
            throws Exception {
        DeephavenInputTableSink sink =
                new DeephavenInputTableSink(flight, allocator, connector, publishTimeout);
        boolean created = sink.ensureTable();
        ConnectorRuntime runtime = runtimes.create(connector, sink);
        runtime.resetLocalState();
        ConnectorWorker worker = new ConnectorWorker(amps, connector, runtime, sink);
        worker.subscribe(created);
        return worker;
    }

    private void subscribe(boolean replayFromBeginning) throws Exception {
        SubscriptionSpec spec = SubscriptionSpecFactory.create(connector, replayFromBeginning);
        ampsClient = AmpsClientAdapter.connect(amps, connector);
        ampsClient.subscribe(spec, this::safeOnMessage);
        log.info("connector {} subscribed ({})", connector.getName(), spec);
    }

    private void safeOnMessage(AmpsInboundMessage message) {
        if (!running.get()) {
            return;
        }
        try {
            runtime.onMessage(message);
        } catch (RuntimeException e) {
            log.warn("connector {} dropped message: {}", connector.getName(), e.toString());
        }
    }

    @Override
    public void close() {
        running.set(false);
        if (ampsClient != null) {
            ampsClient.close();
        }
    }
}
