package com.fix42.dashboard.connectors.transform;

import com.fix42.dashboard.connectors.source.SourceRecord;
import java.util.Map;

/**
 * A per-record rewrite of the decoded tag namespace, named by a connector's
 * {@code transforms:} list.
 *
 * <p>Applied between decode and explode/map, so it works in the same {@code tag -> value}
 * space the configuration addresses: a tag a transform derives is mappable by the ordinary
 * {@code fields:} entries and goes through the same allowlist, coercion and validation as one
 * the payload carried. That is the point of the seam -- an enrichment costs a bean plus a
 * mapping, not a fork of the pipeline.
 *
 * <p>The contract:
 *
 * <ul>
 *   <li>Implementations must be <strong>stateless</strong>: one instance serves every
 *       connector that names it, on the source's delivery thread, and every restart replays
 *       the feed from the beginning.
 *   <li>A {@code null} return <strong>drops</strong> the record -- the filtering spelling.
 *       The connector counts it as dropped rather than rejected: it is a decision, not a
 *       failure.
 *   <li>Transforms also see {@link SourceRecord.Action#DELETE} records, because a delete's
 *       key columns are extracted from its fields the same way an upsert's are. A transform
 *       that derives a key column has to derive it for deletes too, or the removal cannot be
 *       addressed.
 *   <li>The map passed in may be immutable; return a new map rather than mutating it.
 * </ul>
 */
@FunctionalInterface
public interface RecordTransform {

    /**
     * @param record the record the fields were decoded from, for its action and source key
     * @param fields the decoded {@code tag -> value} pairs
     * @return the fields to carry on with, or {@code null} to drop the record
     */
    Map<String, String> apply(SourceRecord record, Map<String, String> fields);
}
