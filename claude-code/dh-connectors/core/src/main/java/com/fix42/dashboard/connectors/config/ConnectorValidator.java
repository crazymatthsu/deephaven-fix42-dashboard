package com.fix42.dashboard.connectors.config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Cross-field configuration rules that bean validation annotations cannot express.
 *
 * <p>These are the combinations that would otherwise fail late and confusingly -- or worse,
 * succeed while quietly corrupting the target table (a delta subscription published in
 * {@code FULL} mode blanks every column the delta omitted). Checked once at startup so a bad
 * {@code application.yml} stops the application with a readable list instead of a stack trace.
 *
 * <p>A rule that only one transport can satisfy names that transport in its message: with
 * three source kinds sharing one connector model, "requires a SOW topic" is only actionable
 * once you know which block was supposed to provide one.
 */
public final class ConnectorValidator {

    /**
     * Table and column names have to be plain identifiers: the table name is interpolated into
     * the python that creates it, and Deephaven column names are java identifiers.
     */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private ConnectorValidator() {
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /**
     * Validate every connector plus the rules that span connectors.
     *
     * @param properties the bound configuration
     * @return human-readable problems, empty when the configuration is sound
     */
    public static List<String> validate(ConnectorsProperties properties) {
        List<String> errors = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (ConnectorProperties connector : properties.getConnectors()) {
            if (connector.getName() != null && !names.add(connector.getName())) {
                errors.add("duplicate connector name: " + connector.getName());
            }
            errors.addAll(validate(connector));
        }
        return errors;
    }

    /**
     * Validate a single connector.
     *
     * @param connector the connector to check
     * @return human-readable problems, each already prefixed with the connector name
     */
    public static List<String> validate(ConnectorProperties connector) {
        List<String> errors = new ArrayList<>();
        String id = "connector '" + connector.getName() + "': ";
        SourceProperties source = connector.getSource();
        DeephavenTableProperties target = connector.getDeephaven();

        if (target.getTable() != null && !IDENTIFIER.matcher(target.getTable()).matches()) {
            errors.add(id + "deephaven.table '" + target.getTable()
                    + "' is not a valid identifier");
        }

        errors.addAll(validateSource(id, connector));

        if (connector.getFormat() == SourceFormat.COMPOSITE) {
            if (connector.getCompositeParts().isEmpty()) {
                errors.add(id + "format=COMPOSITE requires composite-parts: the constituent "
                        + "format of each part, in the order the server's type declares them");
            }
            if (connector.getCompositeParts().contains(SourceFormat.COMPOSITE)) {
                errors.add(id + "composite-parts cannot themselves be COMPOSITE");
            }
        } else if (!connector.getCompositeParts().isEmpty()) {
            errors.add(id + "composite-parts is only meaningful for format=COMPOSITE");
        }

        Set<String> tags = new HashSet<>();
        Set<String> columns = new LinkedHashSet<>();
        for (FieldMapping field : connector.getFields()) {
            if (field.getTag() != null && !tags.add(field.getTag())) {
                errors.add(id + "duplicate source tag '" + field.getTag() + "'");
            }
            if (field.getColumn() != null && !columns.add(field.getColumn())) {
                errors.add(id + "duplicate deephaven column '" + field.getColumn() + "'");
            }
            if (field.getColumn() != null && !IDENTIFIER.matcher(field.getColumn()).matches()) {
                errors.add(id + "column '" + field.getColumn() + "' is not a valid identifier");
            }
            if (connector.getFormat() == SourceFormat.FIX
                    && field.getTag() != null
                    && !field.getTag().chars().allMatch(Character::isDigit)) {
                errors.add(id + "FIX tag '" + field.getTag() + "' is not a tag number");
            }
            if (connector.getFormat() == SourceFormat.COMPOSITE && field.getTag() != null) {
                errors.addAll(validateCompositeTag(id, field.getTag(),
                        connector.getCompositeParts()));
            }
            errors.addAll(validateValueShaping(id, field, target.getKeyColumns()));
        }

        errors.addAll(validateExplode(connector, columns));

        String sourceKeyColumn = target.getSourceKeyColumn();
        if (sourceKeyColumn != null && !sourceKeyColumn.isBlank()) {
            if (!columns.add(sourceKeyColumn)) {
                errors.add(id + "source-key-column '" + sourceKeyColumn
                        + "' collides with a mapped column");
            }
            // A socket delivers bytes, nothing beside them: there is no per-message key to
            // publish, so the column could only ever be null.
            if (source.getTcp() != null) {
                errors.add(id + "deephaven.source-key-column is not available for source.tcp: "
                        + "a TCP stream carries no per-message key -- key on payload fields "
                        + "instead");
            }
            // Kafka's tombstone carries a key and no value, so a compacted topic can only
            // express a removal if the key column IS the message key.
            if (source.getKafka() != null && source.getKafka().isCompacted()
                    && target.isKeyed()
                    && !target.getKeyColumns().contains(sourceKeyColumn)) {
                errors.add(id + "source.kafka.compacted=true with a keyed table requires "
                        + "deephaven.key-columns to include source-key-column '"
                        + sourceKeyColumn + "': a tombstone carries no payload to rebuild the "
                        + "key from");
            }
        }
        String ingestColumn = target.getIngestTimestampColumn();
        if (ingestColumn != null && !ingestColumn.isBlank() && !columns.add(ingestColumn)) {
            errors.add(id + "ingest-timestamp-column '" + ingestColumn
                    + "' collides with a mapped column");
        }

        for (String key : target.getKeyColumns()) {
            if (!columns.contains(key)) {
                errors.add(id + "key column '" + key + "' is not one of the mapped columns "
                        + columns);
            }
        }

        // KEYED and key-columns imply each other: keys with nothing to key, or a keyed table
        // with no keys, are both configurations with no meaning.
        DeephavenTableType type = target.resolveTableType(source.stateful());
        String because = target.getTableType() == null
                ? " (a " + (source.stateful() ? "stateful" : "streaming") + " source defaults "
                        + "deephaven.table-type to " + type + ")"
                : "";
        if (type.keyed() && !target.isKeyed()) {
            errors.add(id + "deephaven.table-type=KEYED requires deephaven.key-columns" + because);
        }
        if (!type.keyed() && target.isKeyed()) {
            errors.add(id + "deephaven.key-columns is only meaningful for table-type=KEYED, "
                    + "but this connector resolves to " + type + because);
        }
        // Merging a partial row requires somewhere to merge it into.
        if (target.getPublishMode() == UpdateMode.DELTA && !type.keyed()) {
            errors.add(id + "deephaven.publish-mode=DELTA requires a keyed table");
        }
        // A blink table can only be written through the TablePublisher that created it, and the
        // only thing that creates one is the bootstrap. Turning the bootstrap off leaves nothing
        // able to publish, ever -- so say so now rather than on the first batch.
        if (type.publisherBacked() && !target.isCreateIfMissing()) {
            errors.add(id + "deephaven.table-type=" + type
                    + " requires deephaven.create-if-missing, because the only way into a blink "
                    + "table is the TablePublisher the bootstrap creates");
        }
        if (source.subscriptionMode() == UpdateMode.DELTA && !source.stateful()) {
            errors.add(id + "source.amps.subscription-mode=DELTA requires a SOW topic");
        }
        // The destructive combination: AMPS sends only changed fields, and we would publish
        // them as a whole row, nulling every column this particular delta did not mention.
        if (source.subscriptionMode() == UpdateMode.DELTA
                && target.getPublishMode() == UpdateMode.FULL) {
            errors.add(id + "source.amps.subscription-mode=DELTA needs "
                    + "deephaven.publish-mode=DELTA, otherwise fields absent from a delta would "
                    + "overwrite stored values with null");
        }
        if (connector.getFields().isEmpty()) {
            errors.add(id + "no fields configured, so nothing would ever be published");
        }
        errors.addAll(validateTransforms(id, connector));
        return errors;
    }

    /**
     * Which transport a connector speaks, and which features that transport can back.
     *
     * <p>Exactly one block is the rule, {@code SIMULATED} included: the block is what the
     * simulator stands in for, so dropping it under the demo profile would make the demo
     * validate configurations the real deployment would reject. The feature rules name the
     * block they need, because "requires AMPS" is only actionable if you can see which line
     * asked for it.
     */
    private static List<String> validateSource(String id, ConnectorProperties connector) {
        List<String> errors = new ArrayList<>();
        SourceProperties source = connector.getSource();
        List<String> blocks = source.configuredBlocks();
        if (blocks.isEmpty()) {
            errors.add(id + "source needs exactly one of amps/kafka/tcp, and has none");
            return errors;
        }
        if (blocks.size() > 1) {
            errors.add(id + "source configures " + blocks + ", but a connector feeds one table "
                    + "from one source: keep exactly one of amps/kafka/tcp");
            return errors;
        }

        AmpsSourceProperties amps = source.getAmps();
        if (connector.getFormat() == SourceFormat.COMPOSITE) {
            if (amps == null) {
                errors.add(id + "format=COMPOSITE requires source.amps: composite message "
                        + "types are an AMPS feature, not a wire format other sources frame");
            } else if (isBlank(amps.getMessageType()) && isBlank(amps.getUri())) {
                errors.add(id + "format=COMPOSITE requires source.amps.message-type (or a full "
                        + "source.amps.uri): 'composite' is not an AMPS message type name -- "
                        + "name the composite type the server config registers");
            }
        }
        if (amps == null && !connector.getCompositeParts().isEmpty()) {
            errors.add(id + "composite-parts requires source.amps");
        }
        // There is deliberately no "subscription-mode=DELTA requires amps" rule: the setting
        // lives on the amps block, so a connector without one cannot ask for deltas at all,
        // and SourceProperties.subscriptionMode() answers FULL for every other transport.
        return errors;
    }

    /**
     * The {@code transforms:} list. Names are resolved to beans at connector start, which is
     * where an unknown one is reported; these are the two mistakes worth catching earlier
     * because neither can ever be right.
     */
    private static List<String> validateTransforms(String id, ConnectorProperties connector) {
        List<String> errors = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (String name : connector.getTransforms()) {
            if (isBlank(name)) {
                errors.add(id + "transforms contains a blank entry");
            } else if (!seen.add(name)) {
                errors.add(id + "duplicate transform '" + name + "': a transform is stateless, "
                        + "so applying it twice either does nothing or is a copy-paste slip");
            }
        }
        return errors;
    }

    /**
     * The rules for one part-indexed composite tag: the part it names must exist, and a tag
     * into a FIX part must be a tag number. An unprefixed tag is legal -- it reads from the
     * decoder's merged bare-tag namespace, the natural spelling for a {@code composite-global}
     * topic.
     */
    private static List<String> validateCompositeTag(
            String id, String tag, List<SourceFormat> parts) {
        List<String> errors = new ArrayList<>();
        int dot = tag.indexOf('.');
        if (dot <= 0 || !tag.substring(0, dot).chars().allMatch(Character::isDigit)) {
            return errors;
        }
        int part = Integer.parseInt(tag.substring(0, dot));
        String rest = tag.substring(dot + 1);
        if (part >= parts.size()) {
            errors.add(id + "tag '" + tag + "' references part " + part
                    + ", but composite-parts declares only " + parts.size());
            return errors;
        }
        if (parts.get(part) == SourceFormat.FIX && !rest.chars().allMatch(Character::isDigit)) {
            errors.add(id + "tag '" + tag + "': part " + part + " is FIX, so '" + rest
                    + "' is not a tag number");
        }
        return errors;
    }

    /**
     * The {@code explode} rules (doc 07 section 5.4). Also registers the explode columns in
     * {@code columns}, so the collision and key-column checks that follow see them.
     */
    private static List<String> validateExplode(
            ConnectorProperties connector, Set<String> columns) {
        ExplodeProperties explode = connector.getExplode();
        if (explode == null) {
            return List.of();
        }
        List<String> errors = new ArrayList<>();
        String id = "connector '" + connector.getName() + "': ";
        SourceProperties source = connector.getSource();
        DeephavenTableProperties target = connector.getDeephaven();

        if (connector.getFormat() != SourceFormat.JSON
                && connector.getFormat() != SourceFormat.COMPOSITE) {
            errors.add(id + "explode requires a JSON object to enumerate, so format must be "
                    + "JSON (or COMPOSITE with the exploded tag in a JSON part), not "
                    + connector.getFormat());
        }
        if (connector.getFormat() == SourceFormat.COMPOSITE && explode.getTag() != null) {
            errors.addAll(validateCompositeTag(id, explode.getTag(),
                    connector.getCompositeParts()));
            int dot = explode.getTag().indexOf('.');
            if (dot > 0 && explode.getTag().substring(0, dot).chars().allMatch(Character::isDigit)) {
                int part = Integer.parseInt(explode.getTag().substring(0, dot));
                if (part < connector.getCompositeParts().size()
                        && connector.getCompositeParts().get(part) != SourceFormat.JSON) {
                    errors.add(id + "explode.tag '" + explode.getTag() + "' must sit in a JSON "
                            + "part, but part " + part + " is "
                            + connector.getCompositeParts().get(part));
                }
            }
        }
        // Member enumeration needs the whole record: a delta that omitted the exploded field
        // would read as "no members" and delete every row.
        if (source.subscriptionMode() == UpdateMode.DELTA) {
            errors.add(id + "explode requires source.amps.subscription-mode=FULL: a delta may "
                    + "omit the exploded field, which is indistinguishable from it emptying");
        }

        String keyColumn = explode.getKeyColumn();
        if (keyColumn != null) {
            if (!IDENTIFIER.matcher(keyColumn).matches()) {
                errors.add(id + "explode.key-column '" + keyColumn
                        + "' is not a valid identifier");
            }
            if (!columns.add(keyColumn)) {
                errors.add(id + "explode.key-column '" + keyColumn
                        + "' collides with a mapped column");
            }
            DeephavenTableType type = target.resolveTableType(source.stateful());
            if (type.keyed() && !target.getKeyColumns().contains(keyColumn)) {
                errors.add(id + "deephaven.key-columns must include explode.key-column '"
                        + keyColumn + "': member rows share every other key value, so without "
                        + "it each record's members collapse onto one row");
            }
        }

        Set<String> tags = new HashSet<>();
        for (FieldMapping field : explode.getFields()) {
            if (field.getTag() != null && !tags.add(field.getTag())) {
                errors.add(id + "duplicate explode tag '" + field.getTag() + "'");
            }
            if (field.getColumn() != null && !columns.add(field.getColumn())) {
                errors.add(id + "duplicate deephaven column '" + field.getColumn() + "'");
            }
            if (field.getColumn() != null && !IDENTIFIER.matcher(field.getColumn()).matches()) {
                errors.add(id + "column '" + field.getColumn() + "' is not a valid identifier");
            }
            errors.addAll(validateValueShaping(id, field, target.getKeyColumns()));
        }
        return errors;
    }

    /**
     * The {@code decode} / {@code values} / {@code default-value} rules for one field mapping.
     *
     * <p>The {@code default-value} coercion is checked here so a bad one joins the readable
     * list, but {@link com.fix42.dashboard.connectors.mapping.ColumnSpec#field} coerces it too and
     * will refuse the same configuration on its own -- whichever runs first names the column.
     */
    private static List<String> validateValueShaping(
            String id, FieldMapping field, List<String> keyColumns) {
        List<String> errors = new ArrayList<>();
        String column = field.getColumn();

        // A built-in table always yields a name; nothing but a string column can hold one.
        // An inline `values` map is not restricted, because rewriting "Y" to "1" for an INT
        // column is a perfectly sensible normalisation.
        if (field.getDecode() != null && field.getType() != ColumnType.STRING) {
            errors.add(id + "column '" + column + "': decode=" + field.getDecode()
                    + " publishes a name, so type must be STRING, not " + field.getType());
        }
        if (field.getDefaultValue() != null) {
            // A defaulted key column would give every record that omits the key the *same*
            // key, collapsing them onto one row -- the exact failure TableSchema.rowKey
            // returns null to prevent.
            if (keyColumns.contains(column)) {
                errors.add(id + "column '" + column + "' is a key column, so it must not set "
                        + "default-value: every record missing the key would share the default "
                        + "and collapse onto one row");
            }
            try {
                field.getType().coerce(field.getDefaultValue());
            } catch (IllegalArgumentException e) {
                errors.add(id + "column '" + column + "': default-value " + e.getMessage());
            }
        }
        return errors;
    }
}
