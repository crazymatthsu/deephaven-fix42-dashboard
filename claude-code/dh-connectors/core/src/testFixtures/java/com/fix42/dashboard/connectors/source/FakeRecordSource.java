package com.fix42.dashboard.connectors.source;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** A {@link RecordSource} the tests drive by hand. */
public class FakeRecordSource implements RecordSource {

    private final AtomicInteger starts = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();
    private final List<SourceRecord> replay = new ArrayList<>();

    private RecordHandler handler;
    private RuntimeException startFailure;
    private volatile boolean connected;

    /** Records delivered automatically on every {@link #start}, standing in for a SOW replay. */
    public FakeRecordSource withReplay(SourceRecord... records) {
        replay.addAll(List.of(records));
        return this;
    }

    /** Make {@link #start} throw, as an unreachable broker would. */
    public FakeRecordSource failingToStart(RuntimeException failure) {
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

    /** Push a record as if the source had delivered it. */
    public void deliver(SourceRecord record) {
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
