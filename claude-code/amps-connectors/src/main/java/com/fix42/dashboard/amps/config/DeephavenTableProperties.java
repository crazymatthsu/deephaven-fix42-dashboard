package com.fix42.dashboard.amps.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * The Deephaven side of one connector: which table to publish into, and how.
 *
 * <p>{@link #getKeyColumns()} decides the table's kind. Non-empty creates a
 * <em>keyed</em> input table -- an add replaces the whole row for that key, which is what
 * makes a SOW topic's updates behave like updates. Empty creates an <em>append-only</em>
 * input table, the right shape for a journal topic.
 */
public class DeephavenTableProperties {

    /** Global name of the Deephaven table, as it appears in the IDE's Panels menu. */
    @NotBlank
    private String table;

    /**
     * {@code DELTA} merges each mapped row over the last row published for its key before
     * sending it, so a partial update does not blank the columns it omits.
     */
    @NotNull
    private UpdateMode publishMode = UpdateMode.FULL;

    /** Key columns of the keyed input table; empty means an append-only table. */
    @NotNull
    private List<String> keyColumns = new ArrayList<>();

    /**
     * Optional column populated with the AMPS SOW key of each message. Useful as the key
     * column when the record body carries no natural identifier.
     */
    private String sowKeyColumn;

    /** Optional column populated with the connector's ingest timestamp. */
    private String ingestTimestampColumn;

    /** Create the table on the server when the global does not exist yet. */
    private boolean createIfMissing = true;

    /** Publish once this many rows have accumulated, without waiting for the flush tick. */
    @Min(1)
    private int maxBatchRows = 5_000;

    /** Maximum time a buffered row waits before being published. */
    @NotNull
    private Duration flushInterval = Duration.ofMillis(250);

    /** Whether this table is keyed -- i.e. whether any key columns were configured. */
    public boolean isKeyed() {
        return keyColumns != null && !keyColumns.isEmpty();
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public UpdateMode getPublishMode() {
        return publishMode;
    }

    public void setPublishMode(UpdateMode publishMode) {
        this.publishMode = publishMode;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(List<String> keyColumns) {
        this.keyColumns = keyColumns == null ? new ArrayList<>() : keyColumns;
    }

    public String getSowKeyColumn() {
        return sowKeyColumn;
    }

    public void setSowKeyColumn(String sowKeyColumn) {
        this.sowKeyColumn = sowKeyColumn;
    }

    public String getIngestTimestampColumn() {
        return ingestTimestampColumn;
    }

    public void setIngestTimestampColumn(String ingestTimestampColumn) {
        this.ingestTimestampColumn = ingestTimestampColumn;
    }

    public boolean isCreateIfMissing() {
        return createIfMissing;
    }

    public void setCreateIfMissing(boolean createIfMissing) {
        this.createIfMissing = createIfMissing;
    }

    public int getMaxBatchRows() {
        return maxBatchRows;
    }

    public void setMaxBatchRows(int maxBatchRows) {
        this.maxBatchRows = maxBatchRows;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public void setFlushInterval(Duration flushInterval) {
        this.flushInterval = flushInterval;
    }
}
