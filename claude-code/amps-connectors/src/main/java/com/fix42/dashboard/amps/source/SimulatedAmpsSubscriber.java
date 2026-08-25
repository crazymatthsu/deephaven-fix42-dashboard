package com.fix42.dashboard.amps.source;

import com.fix42.dashboard.amps.config.AmpsSourceProperties;
import com.fix42.dashboard.amps.config.ColumnType;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.FieldMapping;
import com.fix42.dashboard.amps.config.SourceFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AmpsSubscriber} that synthesises records in process, with no AMPS server.
 *
 * <p>Selected with {@code source.driver: SIMULATED}. It generates payloads in the connector's
 * configured {@link SourceFormat} using the connector's own field mappings, so the decoder,
 * mapper, batcher and publisher all run exactly as they would against a real server. That is
 * what lets the demo stack and the end-to-end test exercise the whole pipeline; AMPS itself is
 * commercial software with no public image to run alongside Kafka and Deephaven.
 *
 * <p>A SOW topic replays one record per simulated key first -- the analogue of the SOW replay
 * that rehydrates the table -- and only then starts emitting live updates.
 */
public class SimulatedAmpsSubscriber implements AmpsSubscriber {

    private static final Logger log = LoggerFactory.getLogger(SimulatedAmpsSubscriber.class);

    private final ConnectorProperties connector;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sequence = new AtomicLong();

    private ScheduledExecutorService scheduler;

    public SimulatedAmpsSubscriber(ConnectorProperties connector) {
        this.connector = connector;
    }

    @Override
    public void start(RecordHandler handler) {
        AmpsSourceProperties source = connector.getSource();
        running.set(true);

        if (source.isSow()) {
            for (int key = 0; key < source.getSimulatedKeys(); key++) {
                handler.onRecord(record(key));
            }
            log.info("[{}] simulated SOW replay: {} records", connector.getName(),
                    source.getSimulatedKeys());
        }

        long periodMicros = Math.max(1L, 1_000_000L / source.getSimulatedRate());
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "amps-sim-" + connector.getName());
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleAtFixedRate(() -> {
            if (!running.get()) {
                return;
            }
            try {
                int key = (int) (sequence.get() % source.getSimulatedKeys());
                handler.onRecord(record(key));
            } catch (RuntimeException e) {
                log.error("[{}] simulated record failed", connector.getName(), e);
            }
        }, periodMicros, periodMicros, TimeUnit.MICROSECONDS);

        log.info("[{}] simulated AMPS source started ({} rec/s across {} keys)",
                connector.getName(), source.getSimulatedRate(), source.getSimulatedKeys());
    }

    private AmpsRecord record(int key) {
        long tick = sequence.incrementAndGet();
        String sowKey = "SIM-" + key;
        return new AmpsRecord(encode(key, tick), sowKey, AmpsRecord.Action.UPSERT);
    }

    /** Render one synthetic record in the connector's wire format. */
    String encode(int key, long tick) {
        List<FieldMapping> fields = connector.getFields();
        char separator = connector.getSource().getFieldSeparator();
        if (connector.getFormat() == SourceFormat.JSON) {
            List<String> members = new ArrayList<>(fields.size());
            for (FieldMapping field : fields) {
                members.add(jsonMember(field, key, tick));
            }
            return "{" + String.join(",", members) + "}";
        }
        StringBuilder payload = new StringBuilder();
        for (FieldMapping field : fields) {
            payload.append(field.getTag()).append('=').append(value(field, key, tick))
                    .append(separator);
        }
        return payload.toString();
    }

    private String jsonMember(FieldMapping field, int key, long tick) {
        String value = value(field, key, tick);
        boolean quoted = field.getType() == ColumnType.STRING
                || field.getType() == ColumnType.CHAR
                || field.getType() == ColumnType.INSTANT;
        String rendered = quoted ? "\"" + value + "\"" : value;
        // Dotted mappings address nested objects; the simulator only produces flat records, so
        // it emits the leaf name and relies on the decoder's flat-key fallback.
        String name = field.getTag();
        int dot = name.lastIndexOf('.');
        return "\"" + (dot < 0 ? name : name.substring(dot + 1)) + "\":" + rendered;
    }

    private String value(FieldMapping field, int key, long tick) {
        return switch (field.getType()) {
            case STRING -> field.getColumn() + "-" + key;
            case CHAR -> String.valueOf((char) ('A' + (key % 26)));
            case BOOLEAN -> String.valueOf(tick % 2 == 0);
            case BYTE, SHORT, INT, LONG -> String.valueOf(100 + key * 10L + (tick % 50));
            case FLOAT, DOUBLE -> String.format(java.util.Locale.ROOT, "%.2f",
                    100.0 + key + (tick % 100) / 100.0);
            case INSTANT -> Instant.now().toString();
        };
    }

    @Override
    public boolean isConnected() {
        return running.get();
    }

    @Override
    public void close() {
        running.set(false);
        ScheduledExecutorService current = scheduler;
        scheduler = null;
        if (current != null) {
            current.shutdownNow();
        }
    }
}
