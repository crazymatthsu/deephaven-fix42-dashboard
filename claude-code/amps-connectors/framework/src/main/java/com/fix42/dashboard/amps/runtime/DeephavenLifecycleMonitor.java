package com.fix42.dashboard.amps.runtime;

import com.fix42.dashboard.amps.config.AmpsConnectorsProperties;
import com.fix42.dashboard.amps.deephaven.DeephavenGateway;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Drives the connector lifecycle from the Deephaven server's.
 *
 * <p>Polls the server on a fixed delay and hands the generation it reports to
 * {@link ConnectorManager}. That single call is what starts the connectors when Deephaven first
 * becomes reachable, stops them when it goes away, and restarts them -- re-creating the tables
 * and replaying AMPS -- when it comes back.
 *
 * <p>A {@link SmartLifecycle} rather than a {@code @PostConstruct} so the poll starts only once
 * the context is fully refreshed, and stops before beans are torn down.
 */
@Component
public class DeephavenLifecycleMonitor implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DeephavenLifecycleMonitor.class);

    /** Log the connector status line every this many ticks. */
    private static final int STATUS_LOG_EVERY_TICKS = 60;

    private final AmpsConnectorsProperties properties;
    private final DeephavenGateway gateway;
    private final ConnectorManager manager;

    private ScheduledExecutorService scheduler;
    private volatile boolean running;
    private int ticks;

    public DeephavenLifecycleMonitor(
            AmpsConnectorsProperties properties,
            DeephavenGateway gateway,
            ConnectorManager manager) {
        this.properties = properties;
        this.gateway = gateway;
        this.manager = manager;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        if (!properties.isEnabled()) {
            log.warn("amps.enabled=false -- no connectors will run");
            running = true;
            return;
        }
        manager.validate();
        if (manager.connectors().isEmpty()) {
            log.warn("no enabled connectors configured under amps.connectors");
        }

        long periodMillis = Math.max(500L, properties.getDeephaven().getHealthCheckInterval().toMillis());
        scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "dh-lifecycle");
            thread.setDaemon(true);
            return thread;
        });
        running = true;
        scheduler.scheduleWithFixedDelay(this::tick, 0L, periodMillis, TimeUnit.MILLISECONDS);
        log.info("Watching Deephaven at {} every {}ms for {} connector(s)",
                properties.getDeephaven().target(), periodMillis, manager.connectors().size());
    }

    private void tick() {
        try {
            manager.onGeneration(gateway.refresh());
        } catch (RuntimeException e) {
            log.warn("Deephaven lifecycle check failed: {}", e.getMessage());
        }
        if (++ticks % STATUS_LOG_EVERY_TICKS == 0) {
            log.info("amps-connectors status:{}", manager.status());
        }
    }

    @Override
    public synchronized void stop() {
        running = false;
        ScheduledExecutorService current = scheduler;
        scheduler = null;
        if (current != null) {
            current.shutdownNow();
        }
        manager.close();
        gateway.close();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // Start late and stop early relative to ordinary beans.
        return Integer.MAX_VALUE - 1_000;
    }
}
