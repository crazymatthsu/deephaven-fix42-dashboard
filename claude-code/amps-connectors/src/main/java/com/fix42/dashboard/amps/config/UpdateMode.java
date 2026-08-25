package com.fix42.dashboard.amps.config;

/**
 * Whether an update carries a whole record or only the fields that changed.
 *
 * <p>Used for two independent knobs (doc 07 section 4):
 *
 * <ul>
 *   <li>{@code source.subscription-mode} -- {@code DELTA} issues an AMPS
 *       {@code delta_subscribe} / {@code sow_and_delta_subscribe}, so AMPS sends only the
 *       changed fields of each record.
 *   <li>{@code deephaven.publish-mode} -- {@code DELTA} merges each mapped row over the
 *       last row published for that key before handing it to Deephaven.
 * </ul>
 *
 * <p>The second is what makes the first safe: a Deephaven keyed input table replaces the
 * <em>whole</em> row for a key on add, so publishing a partial row in {@code FULL} mode
 * would null out every column the delta did not mention.
 */
public enum UpdateMode {

    /** Every update carries the complete record. */
    FULL,

    /** Updates carry only changed fields. */
    DELTA
}
