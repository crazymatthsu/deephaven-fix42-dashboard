package com.fix42.dashboard.amps.source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** An {@link AmpsSubscriber} the tests drive by hand. */
public class FakeAmpsSubscriber implements AmpsSubscriber {

    private final AtomicInteger starts = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();
    private final List<AmpsRecord> replay = new ArrayList<>();

    private RecordHandler handler;
    private RuntimeException startFailure;
    private volatile boolean connected;

    /** Records delivered automatically on every {@link #start}, standing in for a SOW replay. */
    public FakeAmpsSubscriber withReplay(AmpsRecord... records) {
        replay.addAll(List.of(records));
        return this;
    }

    /** Make {@link #start} throw, as an unreachable AMPS server would. */
    public FakeAmpsSubscriber failingToStart(RuntimeException failure) {
        this.startFailure = failure;
        return this;
    }

    @Override
    public void start(RecordHandler handler) {
        if (startFailure != null) {
            throw startFailure;
        }
        this.handler = handler;
        this.connected = true;
        starts.incrementAndGet();
        replay.forEach(handler::onRecord);
    }

    /** Push a record as if AMPS had delivered it. */
    public void deliver(AmpsRecord record) {
        if (handler == null) {
            throw new IllegalStateException("not subscribed");
        }
        handler.onRecord(record);
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public void close() {
        connected = false;
        handler = null;
        closes.incrementAndGet();
    }

    /** How many times this subscriber was subscribed -- one per rehydration. */
    public int startCount() {
        return starts.get();
    }

    public int closeCount() {
        return closes.get();
    }
}
