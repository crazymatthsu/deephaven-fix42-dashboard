package com.fix42.dashboard.amps.runtime;

import com.fix42.dashboard.amps.config.AmpsConnectorsProperties;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.ConnectorValidator;
import com.fix42.dashboard.amps.decode.RecordDecoderFactory;
import com.fix42.dashboard.amps.deephaven.DeephavenGateway;
import com.fix42.dashboard.amps.source.AmpsSubscriberFactory;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Owns every configured connector and keeps them aligned with the Deephaven server's lifecycle.
 *
 * <p>This is where "restarting Deephaven restarts the connectors" lives (doc 07 section 6).
 * {@link DeephavenGateway#refresh()} hands back a generation number that changes whenever the
 * server we are talking to is a different incarnation -- reconnected, or back up with an empty
 * python scope. A change stops every connector, and starting them again re-creates the tables
 * and replays each subscription from the start, so the tables are rehydrated rather than
 * resuming mid-stream against state that no longer exists.
 *
 * <p>Starting is retried per connector on every tick, so one connector whose AMPS server is
 * unreachable does not hold up the others, and recovers on its own when it comes back.
 */
@Component
public class ConnectorManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConnectorManager.class);

    private final AmpsConnectorsProperties properties;
    private final DeephavenGateway gateway;
    private final List<Connector> connectors = new ArrayList<>();

    private long lastGeneration;
    private boolean initialised;

    public ConnectorManager(
            AmpsConnectorsProperties properties,
            DeephavenGateway gateway,
            RecordDecoderFactory decoderFactory,
            AmpsSubscriberFactory subscriberFactory,
            Clock clock) {
        this.properties = properties;
        this.gateway = gateway;
        for (ConnectorProperties connector : properties.enabledConnectors()) {
            connectors.add(new Connector(
                    connector,
                    decoderFactory.create(connector),
                    gateway,
                    subscriberFactory::create,
                    clock));
        }
    }

    /** Fail fast on a configuration that would misbehave at runtime. */
    public void validate() {
        List<String> errors = ConnectorValidator.validate(properties);
        if (!errors.isEmpty()) {
            throw new IllegalStateException(
                    "invalid amps-connectors configuration:\n  - " + String.join("\n  - ", errors));
        }
    }

    /** The connectors this manager runs. */
    public List<Connector> connectors() {
        return List.copyOf(connectors);
    }

    /**
     * React to the Deephaven server's current generation.
     *
     * @param generation the value from {@link DeephavenGateway#refresh()}; {@code 0} means the
     *     server is unreachable
     */
    public synchronized void onGeneration(long generation) {
        if (generation == 0L) {
            if (lastGeneration != 0L || !initialised) {
                stopAll("Deephaven is unavailable");
            }
            lastGeneration = 0L;
            initialised = true;
            return;
        }
        if (generation != lastGeneration) {
            if (lastGeneration != 0L) {
                log.info("Deephaven generation {} -> {}: restarting {} connector(s) to rehydrate",
                        lastGeneration, generation, connectors.size());
            }
            stopAll("Deephaven generation changed");
            lastGeneration = generation;
        }
        initialised = true;
        startPending();
    }

    /** Start every connector that is not running yet; failures are retried on the next tick. */
    private void startPending() {
        for (Connector connector : connectors) {
            if (connector.isStarted()) {
                continue;
            }
            try {
                connector.start();
            } catch (Exception e) {
                log.warn("[{}] start failed, retrying on the next health check: {}",
                        connector.name(), e.getMessage());
            }
        }
    }

    private void stopAll(String reason) {
        boolean any = connectors.stream().anyMatch(Connector::isStarted);
        if (any) {
            log.info("Stopping {} connector(s): {}", connectors.size(), reason);
        }
        for (Connector connector : connectors) {
            try {
                connector.stop();
            } catch (RuntimeException e) {
                log.warn("[{}] stop failed", connector.name(), e);
            }
        }
    }

    /** One line per connector, for the periodic status log. */
    public String status() {
        StringBuilder text = new StringBuilder();
        for (Connector connector : connectors) {
            text.append(String.format(
                    "%n  %-24s %-11s %-22s received=%d published=%d rejected=%d",
                    connector.name(),
                    connector.isStarted() ? "RUNNING" : "STOPPED",
                    connector.schema().tableName(),
                    connector.receivedRecords(),
                    connector.publishedRows(),
                    connector.rejectedRecords()));
        }
        return text.toString();
    }

    @Override
    public synchronized void close() {
        stopAll("application shutdown");
        lastGeneration = 0L;
    }
}
