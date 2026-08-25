package com.deephaven.fix42.amps.runtime;

import com.deephaven.fix42.amps.config.AmpsProperties;
import com.deephaven.fix42.amps.config.ConnectorConfigValidator;
import com.deephaven.fix42.amps.config.ConnectorProperties;
import com.deephaven.fix42.amps.config.DeephavenClientProperties;
import com.deephaven.fix42.amps.dh.DeephavenFlightSupport;
import io.deephaven.client.impl.FlightSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Starts with Deephaven: wait until the server is up, create tables, subscribe to AMPS
 * (SOW snapshot or journal-from-epoch) to rehydrate, then stream. If Deephaven drops,
 * tear down AMPS subscriptions, wait for Deephaven, and rehydrate again.
 */
@Component
@ConditionalOnProperty(name = "amps.auto-start", havingValue = "true", matchIfMissing = true)
public class ConnectorSupervisor implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ConnectorSupervisor.class);

    private final AmpsProperties amps;
    private final DeephavenClientProperties deephaven;
    private final ConnectorRuntimeFactory runtimes;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread loop;
    private final List<ConnectorWorker> workers = new ArrayList<>();

    public ConnectorSupervisor(
            AmpsProperties amps, DeephavenClientProperties deephaven, ConnectorRuntimeFactory runtimes) {
        this.amps = amps;
        this.deephaven = deephaven;
        this.runtimes = runtimes;
        this.loop = new Thread(this::runLoop, "amps-connectors-supervisor");
        this.loop.setDaemon(true);
        this.loop.start();
    }

    private void runLoop() {
        List<ConnectorProperties> connectors;
        try {
            connectors = ConnectorConfigValidator.enabled(amps);
        } catch (RuntimeException e) {
            log.error("invalid AMPS connector configuration: {}", e.getMessage());
            return;
        }
        if (connectors.isEmpty()) {
            log.warn("no enabled AMPS connectors; supervisor idle");
            return;
        }
        Duration backoff = amps.getReconnectBackoff() == null ? Duration.ofSeconds(2) : amps.getReconnectBackoff();
        Duration maxBackoff =
                amps.getReconnectMaxBackoff() == null ? Duration.ofSeconds(30) : amps.getReconnectMaxBackoff();
        Duration current = backoff;
        while (running.get()) {
            DeephavenFlightSupport support = null;
            FlightSession session = null;
            try {
                waitForDeephaven();
                if (!running.get()) {
                    return;
                }
                support = new DeephavenFlightSupport(deephaven);
                session = openSession(support);
                log.info("Deephaven session ready at {}:{}", deephaven.getHost(), deephaven.getPort());
                startWorkers(session, support, connectors);
                current = backoff;
                awaitSessionEnd(session);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("AMPS connectors cycle failed: {}", e.toString());
            } finally {
                stopWorkers();
                closeQuietly(session);
                if (support != null) {
                    support.close();
                }
            }
            if (!running.get()) {
                return;
            }
            log.info("Deephaven unavailable or session ended; retrying AMPS connectors in {}", current);
            try {
                sleep(current);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            long nextMs = Math.min(current.toMillis() * 2, maxBackoff.toMillis());
            current = Duration.ofMillis(Math.max(nextMs, backoff.toMillis()));
        }
    }

    private void waitForDeephaven() throws InterruptedException {
        Instant deadline = Instant.now().plus(deephaven.getWaitTimeout());
        Duration retry = deephaven.getRetryInterval() == null ? Duration.ofSeconds(2) : deephaven.getRetryInterval();
        Exception last = null;
        while (running.get() && Instant.now().isBefore(deadline)) {
            try (DeephavenFlightSupport probe = new DeephavenFlightSupport(deephaven);
                    FlightSession ignored = probe.openSession()) {
                log.info("Deephaven is up");
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception e) {
                last = e;
                log.info("waiting for Deephaven {}:{} ({})", deephaven.getHost(), deephaven.getPort(), e.toString());
                sleep(retry);
            }
        }
        throw new IllegalStateException(
                "Deephaven did not become ready within " + deephaven.getWaitTimeout(), last);
    }

    private FlightSession openSession(DeephavenFlightSupport support) {
        return support.openSession();
    }

    private void startWorkers(FlightSession session, DeephavenFlightSupport support, List<ConnectorProperties> connectors)
            throws Exception {
        synchronized (workers) {
            for (ConnectorProperties connector : connectors) {
                ConnectorWorker worker = ConnectorWorker.start(
                        amps,
                        connector,
                        runtimes,
                        session,
                        support.allocator(),
                        deephaven.getPublishTimeout());
                workers.add(worker);
            }
        }
    }

    private void awaitSessionEnd(FlightSession session) throws InterruptedException {
        while (running.get()) {
            try {
                session.list().iterator();
            } catch (Exception e) {
                log.warn("Deephaven session lost: {}", e.toString());
                return;
            }
            Thread.sleep(2_000);
        }
    }

    private void stopWorkers() {
        synchronized (workers) {
            for (ConnectorWorker worker : workers) {
                try {
                    worker.close();
                } catch (Exception e) {
                    log.debug("worker close: {}", e.toString());
                }
            }
            workers.clear();
        }
    }

    private static void closeQuietly(FlightSession session) {
        if (session == null) {
            return;
        }
        try {
            session.close();
        } catch (Exception e) {
            log.debug("flight session close: {}", e.toString());
        }
    }

    private void sleep(Duration d) throws InterruptedException {
        long ms = Math.max(100, d.toMillis());
        long end = System.currentTimeMillis() + ms;
        while (running.get() && System.currentTimeMillis() < end) {
            Thread.sleep(Math.min(200, end - System.currentTimeMillis()));
        }
    }

    @PreDestroy
    @Override
    public void close() {
        running.set(false);
        loop.interrupt();
        stopWorkers();
        try {
            loop.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
