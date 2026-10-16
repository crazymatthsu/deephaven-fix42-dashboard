package com.fix42.dashboard.connectors.runtime;

import com.fix42.dashboard.connectors.source.SourceRecord;

/**
 * One element of a {@link RecordSpool}: a record, or a snapshot boundary travelling in order
 * with the records around it.
 *
 * <p>The boundaries are entries rather than flags so that "the replay ended" is applied
 * exactly after the last replayed record and before the first live one, whichever tier they
 * sit in -- a flag set on the delivery thread would be seen by the drain thread at some
 * unrelated point of the backlog.
 *
 * @param kind what the entry is
 * @param record the record, for {@link Kind#RECORD}; {@code null} for a boundary
 */
public record SpoolEntry(Kind kind, SourceRecord record) {

    /** What travels through the spool. */
    public enum Kind {
        /** A source record. */
        RECORD,
        /** The source is about to replay its state: {@code RecordHandler.onSnapshotBegin}. */
        SNAPSHOT_BEGIN,
        /** The replay is complete: {@code RecordHandler.onSnapshotEnd}. */
        SNAPSHOT_END
    }

    private static final SpoolEntry SNAPSHOT_BEGIN_ENTRY = new SpoolEntry(Kind.SNAPSHOT_BEGIN, null);
    private static final SpoolEntry SNAPSHOT_END_ENTRY = new SpoolEntry(Kind.SNAPSHOT_END, null);

    /** Approximate footprint of an entry that carries no record. */
    private static final long BOUNDARY_BYTES = 32L;
    /** Per-record overhead on top of its text: the record object and its strings' headers. */
    private static final long RECORD_OVERHEAD_BYTES = 64L;
    private static final long PART_OVERHEAD_BYTES = 32L;

    /** A record entry. */
    public static SpoolEntry of(SourceRecord record) {
        return new SpoolEntry(Kind.RECORD, record);
    }

    /** The boundary that opens a snapshot replay. */
    public static SpoolEntry snapshotBegin() {
        return SNAPSHOT_BEGIN_ENTRY;
    }

    /** The boundary that closes a snapshot replay. */
    public static SpoolEntry snapshotEnd() {
        return SNAPSHOT_END_ENTRY;
    }

    /**
     * Approximate size, counting characters as bytes. What {@code spool.memory-limit} is
     * measured in; close enough for a bound, and free to compute.
     */
    public long sizeEstimate() {
        if (record == null) {
            return BOUNDARY_BYTES;
        }
        long size = RECORD_OVERHEAD_BYTES;
        if (record.key() != null) {
            size += record.key().length();
        }
        if (record.data() != null) {
            size += record.data().length();
        }
        if (record.parts() != null) {
            for (String part : record.parts()) {
                size += PART_OVERHEAD_BYTES + part.length();
            }
        }
        return size;
    }
}
