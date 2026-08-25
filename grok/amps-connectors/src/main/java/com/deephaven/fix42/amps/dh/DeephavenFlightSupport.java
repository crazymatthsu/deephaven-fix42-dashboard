package com.deephaven.fix42.amps.dh;

import io.deephaven.client.impl.ClientConfig;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactoryConfig;
import io.deephaven.client.impl.SessionConfig;
import io.deephaven.uri.DeephavenTarget;
import io.grpc.ManagedChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.deephaven.fix42.amps.config.DeephavenClientProperties;

public final class DeephavenFlightSupport implements AutoCloseable {
    private final BufferAllocator allocator;
    private final ScheduledExecutorService scheduler;
    private final FlightSessionFactoryConfig.Factory factory;
    private final SessionConfig sessionConfig;

    public DeephavenFlightSupport(DeephavenClientProperties props) {
        this.allocator = new RootAllocator();
        this.scheduler = Executors.newScheduledThreadPool(4);
        DeephavenTarget target = DeephavenTarget.builder()
                .host(props.getHost())
                .port(props.getPort())
                .isSecure(props.isSecure())
                .build();
        ClientConfig clientConfig = ClientConfig.builder().target(target).build();
        this.factory = FlightSessionFactoryConfig.builder()
                .clientConfig(clientConfig)
                .scheduler(scheduler)
                .allocator(allocator)
                .build()
                .factory();
        SessionConfig.Builder session = SessionConfig.builder();
        if (props.getPsk() != null && !props.getPsk().isBlank()) {
            session.authenticationTypeAndValue(
                    "io.deephaven.authentication.psk.PskAuthenticationHandler " + props.getPsk());
        }
        this.sessionConfig = session.build();
    }

    public BufferAllocator allocator() {
        return allocator;
    }

    public FlightSession openSession() {
        return factory.newFlightSession(sessionConfig);
    }

    @Override
    public void close() {
        try {
            factory.managedChannel().shutdownNow();
            factory.managedChannel().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignored) {
            // channel already down
        }
        scheduler.shutdownNow();
        allocator.close();
    }

    public ManagedChannel channel() {
        return factory.managedChannel();
    }
}
