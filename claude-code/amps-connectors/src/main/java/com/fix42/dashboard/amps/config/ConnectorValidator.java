package com.fix42.dashboard.amps.config;

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
 */
public final class ConnectorValidator {

    /**
     * Table and column names have to be plain identifiers: the table name is interpolated into
     * the python that creates it, and Deephaven column names are java identifiers.
     */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private ConnectorValidator() {
    }

    /**
     * Validate every connector plus the rules that span connectors.
     *
     * @param properties the bound configuration
     * @return human-readable problems, empty when the configuration is sound
     */
    public static List<String> validate(AmpsConnectorsProperties properties) {
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
        AmpsSourceProperties source = connector.getSource();
        DeephavenTableProperties target = connector.getDeephaven();

        if (target.getTable() != null && !IDENTIFIER.matcher(target.getTable()).matches()) {
            errors.add(id + "deephaven.table '" + target.getTable()
                    + "' is not a valid identifier");
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
        }

        String sowKeyColumn = target.getSowKeyColumn();
        if (sowKeyColumn != null && !sowKeyColumn.isBlank() && !columns.add(sowKeyColumn)) {
            errors.add(id + "sow-key-column '" + sowKeyColumn + "' collides with a mapped column");
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
        DeephavenTableType type = target.resolveTableType(source.isSow());
        String because = target.getTableType() == null
                ? " (source.sow=" + source.isSow() + " defaults deephaven.table-type to " + type + ")"
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
        if (source.getSubscriptionMode() == UpdateMode.DELTA && !source.isSow()) {
            errors.add(id + "source.subscription-mode=DELTA requires a SOW topic");
        }
        // The destructive combination: AMPS sends only changed fields, and we would publish
        // them as a whole row, nulling every column this particular delta did not mention.
        if (source.getSubscriptionMode() == UpdateMode.DELTA
                && target.getPublishMode() == UpdateMode.FULL) {
            errors.add(id + "source.subscription-mode=DELTA needs deephaven.publish-mode=DELTA, "
                    + "otherwise fields absent from a delta would overwrite stored values with null");
        }
        if (connector.getFields().isEmpty()) {
            errors.add(id + "no fields configured, so nothing would ever be published");
        }
        return errors;
    }
}
