package com.fix42.dashboard.amps.deephaven;

import com.fix42.dashboard.amps.mapping.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/** A {@link DeephavenGateway} that records what it was asked to do, for the runtime tests. */
public class RecordingDeephavenGateway implements DeephavenGateway {

    /** One recorded publish. */
    public record Publish(String table, String action, List<Object[]> rows) {
    }

    private final AtomicLong generation = new AtomicLong(1);
    private final List<String> createdTables = new CopyOnWriteArrayList<>();
    private final List<Publish> publishes = new CopyOnWriteArrayList<>();

    private volatile boolean available = true;
    private volatile RuntimeException failPublishWith;

    @Override
    public long refresh() {
        return available ? generation.get() : 0L;
    }

    @Override
    public long generation() {
        return available ? generation.get() : 0L;
    }

    @Override
    public boolean isAvailable() {
        return available;
    }

    @Override
    public void ensureTable(TableSchema schema, String connectorName) {
        if (!available) {
            throw new DeephavenUnavailableException("not connected");
        }
        createdTables.add(schema.tableName());
    }

    @Override
    public void addRows(TableSchema schema, List<Object[]> rows) {
        record("add", schema, rows);
    }

    @Override
    public void deleteRows(TableSchema schema, List<Object[]> rows) {
        record("delete", schema, rows);
    }

    private void record(String action, TableSchema schema, List<Object[]> rows) {
        if (failPublishWith != null) {
            throw failPublishWith;
        }
        if (!available) {
            throw new DeephavenUnavailableException("not connected");
        }
        publishes.add(new Publish(schema.tableName(), action, new ArrayList<>(rows)));
    }

    /** Simulate a Deephaven restart: a new incarnation with an empty python scope. */
    public long restart() {
        createdTables.clear();
        return generation.incrementAndGet();
    }

    /** Simulate the server going away or coming back. */
    public void setAvailable(boolean available) {
        this.available = available;
    }

    /** Make the next publishes fail. */
    public void failPublishWith(RuntimeException error) {
        this.failPublishWith = error;
    }

    public List<String> createdTables() {
        return List.copyOf(createdTables);
    }

    public List<Publish> publishes() {
        return List.copyOf(publishes);
    }

    /** Every row published to a table, flattened in publish order. */
    public List<Object[]> rowsFor(String table, String action) {
        List<Object[]> rows = new ArrayList<>();
        for (Publish publish : publishes) {
            if (publish.table().equals(table) && publish.action().equals(action)) {
                rows.addAll(publish.rows());
            }
        }
        return rows;
    }

    @Override
    public void close() {
        available = false;
    }
}
