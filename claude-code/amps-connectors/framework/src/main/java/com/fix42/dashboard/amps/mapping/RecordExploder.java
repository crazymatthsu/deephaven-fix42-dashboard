package com.fix42.dashboard.amps.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fix42.dashboard.amps.config.ColumnType;
import com.fix42.dashboard.amps.config.ExplodeProperties;
import com.fix42.dashboard.amps.config.FieldMapping;
import com.fix42.dashboard.amps.decode.JsonRecordDecoder;
import com.fix42.dashboard.amps.source.AmpsRecord;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns one decoded record into one {@link MappedRow} per member of an object-valued field
 * (doc 07 section 5.4).
 *
 * <p>This is what makes a map with <em>dynamic</em> keys tabular. A record shaped
 * {@code {"key": "portfolio-1", "value": {"AAPL": {...}, "MSFT": {...}}}} cannot be mapped by
 * a static column list -- the member names are data -- so each member becomes a row: the
 * connector's ordinary fields repeat on every row, the member's name fills the configured
 * key column, and the explode mappings resolve inside the member's value.
 *
 * <p>Mechanically each member is mapped by the ordinary {@link FieldMapper} over an augmented
 * copy of the decoded fields: the member name and the member's flattened value are registered
 * under synthetic tags ({@link #KEY_TAG} and {@link #fieldTag}) that the explode columns --
 * and nothing else -- read from. That routes every member row through the exact machinery a
 * plain row gets: {@code decode}/{@code values} rewrites, {@code default-value}, presence
 * flags, key building.
 *
 * <p>On a keyed target the exploder remembers which members each record last published, so a
 * member missing from a republished record deletes its row, and a record leaving the SOW
 * (out-of-focus, {@code sow_delete}) deletes all of its rows. Records are identified by their
 * AMPS SOW key when the topic supplies one, else by the target's non-explode key columns.
 *
 * <p>Not thread safe: each connector owns one exploder, driven from its own message thread.
 */
public final class RecordExploder {

    /**
     * Synthetic tag the member's name is registered under. The NUL prefix keeps the synthetic
     * namespace disjoint from anything a real payload could reasonably carry.
     */
    public static final String KEY_TAG = "\u0000explode.key";

    private static final String FIELD_TAG_PREFIX = "\u0000explode.field.";

    /** The synthetic tag a member-relative mapping's value is registered under. */
    public static String fieldTag(String tag) {
        return FIELD_TAG_PREFIX + tag;
    }

    /**
     * The columns an explode configuration adds to the schema: the key column, then the
     * member-relative fields, each reading from its synthetic tag.
     *
     * @param explode the configuration
     * @return the resolved columns, in configuration order
     */
    public static List<ColumnSpec> columns(ExplodeProperties explode) {
        List<ColumnSpec> columns = new ArrayList<>();
        columns.add(new ColumnSpec(explode.getKeyColumn().trim(), ColumnType.STRING,
                ColumnSpec.Origin.FIELD, KEY_TAG, Map.of(), null));
        for (FieldMapping field : explode.getFields()) {
            columns.add(ColumnSpec.field(field, fieldTag(field.getTag())));
        }
        return columns;
    }

    private final ExplodeProperties explode;
    private final TableSchema schema;
    private final FieldMapper mapper;
    // Structural access to the exploded object; deliberately default-configured, since it only
    // ever reads trees the topic's decoder already accepted as JSON.
    private final ObjectMapper json = new ObjectMapper();

    /** Members last published per record identity; maintained only for a keyed target. */
    private final Map<String, List<String>> membersByRecord = new HashMap<>();

    /** Indexes of the key columns the record itself supplies (all keys minus explode's). */
    private final int[] recordKeyIndexes;

    public RecordExploder(ExplodeProperties explode, TableSchema schema, FieldMapper mapper) {
        this.explode = explode;
        this.schema = schema;
        this.mapper = mapper;
        List<String> explodeColumns = columns(explode).stream().map(ColumnSpec::name).toList();
        this.recordKeyIndexes = schema.keyColumns().stream()
                .filter(key -> !explodeColumns.contains(key))
                .mapToInt(schema::indexOf)
                .toArray();
    }

    /**
     * Explode one record into member rows.
     *
     * @param record the record as delivered
     * @param fields its decoded payload
     * @param ingestTime the timestamp for an ingest-timestamp column
     * @return the rows to publish: upserts for the members carried, deletes for members (or
     *     the whole record) that went away; possibly empty
     * @throws IllegalArgumentException if the exploded field is not a JSON object, or a
     *     member value does not coerce to its column types
     */
    public List<MappedRow> explode(AmpsRecord record, Map<String, String> fields, Instant ingestTime) {
        if (record.action() == AmpsRecord.Action.DELETE) {
            return deletes(record, fields, ingestTime);
        }

        String text = fields.get(explode.getTag());
        if (text == null) {
            // The payload did not carry the field at all: nothing changed, exactly as an
            // unmapped field means elsewhere. (A FULL subscription always carries it.)
            return List.of();
        }
        List<String> members = new ArrayList<>();
        List<MappedRow> rows = new ArrayList<>();
        if (!text.isEmpty()) {
            JsonNode object = parseObject(text);
            for (Map.Entry<String, JsonNode> member : object.properties()) {
                members.add(member.getKey());
                rows.add(mapper.map(record, augment(fields, member.getKey(), member.getValue()),
                        ingestTime));
            }
        }
        // An empty text is an explicit clear (JSON null decodes to ""): zero members, so
        // every previously published member row is deleted below.

        for (String vanished : replaceTracked(record, fields, ingestTime, members)) {
            rows.add(deleteRow(record, fields, vanished, ingestTime));
        }
        return rows;
    }

    /** Drop all remembered state -- used when a connector restarts and replays from scratch. */
    public void clear() {
        membersByRecord.clear();
    }

    /** Number of records whose members are currently remembered. */
    public int trackedRecords() {
        return membersByRecord.size();
    }

    private List<MappedRow> deletes(AmpsRecord record, Map<String, String> fields, Instant ingestTime) {
        List<String> members = null;
        String identity = recordIdentity(record, fields, ingestTime);
        if (identity != null) {
            members = membersByRecord.remove(identity);
        }
        if (members == null) {
            // Nothing tracked (a restart, or an identity the topic never upserted through us):
            // fall back to the members the delete's own payload names, when it carries one --
            // an out-of-focus message usually reports the record's last content.
            members = membersOf(fields);
        }
        List<MappedRow> rows = new ArrayList<>(members.size());
        for (String member : members) {
            rows.add(deleteRow(record, fields, member, ingestTime));
        }
        return rows;
    }

    /**
     * Remember {@code members} for this record and report which previously published members
     * it no longer carries. Tracking only exists on a keyed target -- nothing can be deleted
     * from anything else, so there is nothing worth remembering.
     */
    private List<String> replaceTracked(
            AmpsRecord record, Map<String, String> fields, Instant ingestTime, List<String> members) {
        if (!schema.keyed()) {
            return List.of();
        }
        String identity = recordIdentity(record, fields, ingestTime);
        if (identity == null) {
            return List.of();
        }
        List<String> previous = membersByRecord.put(identity, List.copyOf(members));
        if (previous == null) {
            return List.of();
        }
        List<String> vanished = new ArrayList<>();
        for (String member : previous) {
            if (!members.contains(member)) {
                vanished.add(member);
            }
        }
        return vanished;
    }

    /**
     * What identifies a record across its republications: the AMPS SOW key when the topic
     * supplies one, else the record-level key column values.
     */
    private String recordIdentity(AmpsRecord record, Map<String, String> fields, Instant ingestTime) {
        if (record.sowKey() != null) {
            return "sow:" + record.sowKey();
        }
        if (recordKeyIndexes.length == 0) {
            return null;
        }
        Object[] values = mapper.map(record.withAction(AmpsRecord.Action.UPSERT), fields, ingestTime)
                .values();
        StringBuilder identity = new StringBuilder("key:");
        for (int index : recordKeyIndexes) {
            if (values[index] == null) {
                return null;
            }
            identity.append(values[index]).append('\u001F');
        }
        return identity.toString();
    }

    private MappedRow deleteRow(
            AmpsRecord record, Map<String, String> fields, String member, Instant ingestTime) {
        Map<String, String> augmented = new LinkedHashMap<>(fields);
        augmented.put(KEY_TAG, member);
        return mapper.map(record.withAction(AmpsRecord.Action.DELETE), augmented, ingestTime);
    }

    /** The decoded fields plus one member's name and flattened value, under the synthetic tags. */
    private Map<String, String> augment(Map<String, String> fields, String name, JsonNode value) {
        Map<String, String> augmented = new LinkedHashMap<>(fields);
        augmented.put(KEY_TAG, name);
        if (value.isObject()) {
            augmented.put(fieldTag("."), value.toString());
            for (Map.Entry<String, String> field : JsonRecordDecoder.flatten(value).entrySet()) {
                augmented.put(fieldTag(field.getKey()), field.getValue());
            }
        } else if (value.isArray()) {
            augmented.put(fieldTag("."), value.toString());
        } else if (value.isNull()) {
            augmented.put(fieldTag("."), "");
        } else {
            augmented.put(fieldTag("."), value.asText());
        }
        return augmented;
    }

    /** The member names the payload itself carries, or empty when it has none to name. */
    private List<String> membersOf(Map<String, String> fields) {
        String text = fields.get(explode.getTag());
        if (text == null || text.isEmpty()) {
            return List.of();
        }
        try {
            List<String> members = new ArrayList<>();
            for (Map.Entry<String, JsonNode> member : parseObject(text).properties()) {
                members.add(member.getKey());
            }
            return members;
        } catch (IllegalArgumentException e) {
            return List.of();
        }
    }

    private JsonNode parseObject(String text) {
        try {
            JsonNode node = json.readTree(text);
            if (node == null || !node.isObject()) {
                throw new IllegalArgumentException("explode tag '" + explode.getTag()
                        + "' is not a JSON object: " + abbreviate(text));
            }
            return node;
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalArgumentException("explode tag '" + explode.getTag()
                    + "' is not valid JSON: " + abbreviate(text), e);
        }
    }

    private static String abbreviate(String text) {
        return text.length() <= 80 ? text : text.substring(0, 77) + "...";
    }
}
