package com.fix42.dashboard.fixcache;

import java.time.Instant;
import java.util.function.Supplier;

/**
 * Deterministic ingest clock: every call advances by a fixed step.
 *
 * <p>Port of the {@code clock} fixture in {@code deephaven-scripts/tests/conftest.py}, with the same
 * start instant and step so both suites stamp identical timestamps.
 */
final class FakeClock implements Supplier<Instant> {

    static final Instant START = Instant.parse("2024-01-15T14:30:00Z");

    private Instant current;
    private final long stepMillis;

    FakeClock() {
        this(START, 1);
    }

    FakeClock(Instant start, long stepMillis) {
        this.current = start;
        this.stepMillis = stepMillis;
    }

    @Override
    public Instant get() {
        Instant value = current;
        current = current.plusMillis(stepMillis);
        return value;
    }
}
