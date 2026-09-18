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
 * <p>{@link #getTableType()} decides the table's kind -- keyed, append-only, blink or ring
 * ({@link DeephavenTableType}). Left unset it follows the topic: keyed for a SOW topic,
 * append-only for a journal topic, which is what this module did before the setting existed.
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

    /**
     * The kind of table to create. Unset means "whatever the topic implies": {@code KEYED} for
     * a SOW topic, {@code APPEND_ONLY} for a journal topic.
     */
    private DeephavenTableType tableType;

    /** Key columns of the keyed table. Required by, and only meaningful for, {@code KEYED}. */
    @NotNull
    private List<String> keyColumns = new ArrayList<>();

    /** Rows a {@code RING} table retains. Ignored by every other type. */
    @Min(1)
    private int ringCapacity = 100_000;

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

    /**
     * Whether key columns were configured.
     *
     * <p>Not the same question as "is the target keyed": that is
     * {@link #resolveTableType(boolean)}, and {@link ConnectorValidator} is what holds the two
     * in agreement.
     *
     * @return {@code true} when {@code key-columns} is non-empty
     */
    public boolean isKeyed() {
        return keyColumns != null && !keyColumns.isEmpty();
    }

    /**
     * The table type to create, applying the default when none was configured.
     *
     * @param sow whether the source is a SOW topic
     * @return the resolved type
     */
    public DeephavenTableType resolveTableType(boolean sow) {
        return DeephavenTableType.resolve(tableType, sow);
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

    public DeephavenTableType getTableType() {
        return tableType;
    }

    public void setTableType(DeephavenTableType tableType) {
        this.tableType = tableType;
    }

    public int getRingCapacity() {
        return ringCapacity;
    }

    public void setRingCapacity(int ringCapacity) {
        this.ringCapacity = ringCapacity;
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
