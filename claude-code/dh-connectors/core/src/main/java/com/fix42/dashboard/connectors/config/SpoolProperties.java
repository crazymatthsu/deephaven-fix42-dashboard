package com.fix42.dashboard.connectors.config;

import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.util.unit.DataSize;

/**
 * The buffer between a source's delivery thread and the Deephaven publish path (doc 07
 * section 8.1).
 *
 * <p>Records queue in memory up to {@code memory-limit}; beyond it they spill to segment files
 * under {@code directory}, each deleted as soon as it has been consumed, until the backlog has
 * drained and the queue is back in memory. The spill is throwaway: nothing is ever read back
 * after a restart, because every start replays the feed (doc 07 section 6).
 */
public class SpoolProperties {

    /**
     * {@code false} hands every record to the pipeline on the source's own thread, which then
     * blocks on each full batch -- the behaviour before the spool existed, kept as a switch.
     */
    private boolean enabled = true;

    /**
     * Approximate bytes of records held in memory before spilling to disk; {@code 0} spills
     * every record. Characters count as bytes, so this is the payload size, not the heap it
     * occupies.
     */
    @NotNull
    private DataSize memoryLimit = DataSize.ofMegabytes(64);

    /** Size at which a segment file is rolled. Smaller segments give disk back sooner. */
    @NotNull
    private DataSize segmentSize = DataSize.ofMegabytes(64);

    /**
     * Cap on the spilled backlog across all segments. At the cap the source's delivery thread
     * blocks until the drain makes room -- the back-pressure the spool otherwise removes, kept
     * as the last resort. Size it to the largest snapshot a source replays.
     */
    @NotNull
    private DataSize diskLimit = DataSize.ofGigabytes(4);

    /**
     * Directory for the segment files, one subdirectory per connector. Unset means
     * {@code java.io.tmpdir}. Needs real disk: a tmpfs would spend memory again.
     */
    private String directory;

    /**
     * How long {@code stop()} keeps draining the backlog into Deephaven before discarding what
     * is left and deleting the segment files. Zero discards at once. The next start replays
     * the feed, so a discarded backlog is a gap of seconds, not lost state.
     */
    @NotNull
    private Duration stopTimeout = Duration.ofSeconds(10);

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public DataSize getMemoryLimit() {
        return memoryLimit;
    }

    public void setMemoryLimit(DataSize memoryLimit) {
        this.memoryLimit = memoryLimit;
    }

    public DataSize getSegmentSize() {
        return segmentSize;
    }

    public void setSegmentSize(DataSize segmentSize) {
        this.segmentSize = segmentSize;
    }

    public DataSize getDiskLimit() {
        return diskLimit;
    }

    public void setDiskLimit(DataSize diskLimit) {
        this.diskLimit = diskLimit;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public Duration getStopTimeout() {
        return stopTimeout;
    }

    public void setStopTimeout(Duration stopTimeout) {
        this.stopTimeout = stopTimeout;
    }
}
