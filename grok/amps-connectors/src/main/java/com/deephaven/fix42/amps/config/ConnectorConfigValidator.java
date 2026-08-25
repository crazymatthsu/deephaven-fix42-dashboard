package com.deephaven.fix42.amps.config;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public final class ConnectorConfigValidator {
    private static final Pattern COLUMN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private ConnectorConfigValidator() {}

    public static List<ConnectorProperties> enabled(AmpsProperties amps) {
        List<ConnectorProperties> enabled = amps.getConnectors().stream()
                .filter(ConnectorProperties::isEnabled)
                .toList();
        validateAll(amps, enabled);
        return enabled;
    }

    public static void validateAll(AmpsProperties amps, List<ConnectorProperties> connectors) {
        Set<String> names = new HashSet<>();
        Set<String> tables = new HashSet<>();
        for (ConnectorProperties c : connectors) {
            validateOne(amps, c);
            if (!names.add(c.getName())) {
                throw new ConnectorConfigException("duplicate connector name: " + c.getName());
            }
            if (!tables.add(c.getTableName())) {
                throw new ConnectorConfigException("duplicate Deephaven table name: " + c.getTableName());
            }
        }
    }

    public static void validateOne(AmpsProperties amps, ConnectorProperties c) {
        if (c.getName() == null || c.getName().isBlank()) {
            throw new ConnectorConfigException("connector name is required");
        }
        if (c.getTopic() == null || c.getTopic().isBlank()) {
            throw new ConnectorConfigException(c.getName() + ": topic is required");
        }
        if (c.getTableName() == null || c.getTableName().isBlank()) {
            throw new ConnectorConfigException(c.getName() + ": table-name is required");
        }
        if (!COLUMN.matcher(c.getTableName()).matches()) {
            throw new ConnectorConfigException(
                    c.getName() + ": table-name is not a valid Deephaven identifier: " + c.getTableName());
        }
        if (c.getFields() == null || c.getFields().isEmpty()) {
            throw new ConnectorConfigException(c.getName() + ": at least one field mapping is required");
        }
        String host = resolvedHost(amps, c);
        int port = resolvedPort(amps, c);
        if ((c.getUri() == null || c.getUri().isBlank()) && (host.isBlank() || port <= 0)) {
            throw new ConnectorConfigException(
                    c.getName() + ": AMPS host and port (or uri) are required");
        }
        if (c.getTopicKind() == TopicKind.SOW && (c.getKeyColumns() == null || c.getKeyColumns().isEmpty())) {
            throw new ConnectorConfigException(
                    c.getName() + ": SOW topics require key-columns for the Deephaven keyed table");
        }
        Set<String> columns = new HashSet<>();
        Set<String> sources = new HashSet<>();
        for (FieldMappingProperties f : c.getFields()) {
            if (f.getSource() == null || f.getSource().isBlank()) {
                throw new ConnectorConfigException(c.getName() + ": field source is required");
            }
            if (f.getColumn() == null || f.getColumn().isBlank()) {
                throw new ConnectorConfigException(c.getName() + ": field column is required");
            }
            if (!COLUMN.matcher(f.getColumn()).matches()) {
                throw new ConnectorConfigException(
                        c.getName() + ": column is not a valid Deephaven identifier: " + f.getColumn());
            }
            if (f.getType() == null) {
                throw new ConnectorConfigException(c.getName() + ": field type is required for " + f.getColumn());
            }
            if (!columns.add(f.getColumn())) {
                throw new ConnectorConfigException(c.getName() + ": duplicate column " + f.getColumn());
            }
            if (!sources.add(f.getSource())) {
                throw new ConnectorConfigException(c.getName() + ": duplicate source " + f.getSource());
            }
            if (c.getDataFormat() == DataFormat.FIX) {
                try {
                    Integer.parseInt(f.getSource().trim());
                } catch (NumberFormatException e) {
                    throw new ConnectorConfigException(
                            c.getName() + ": FIX source must be a numeric tag, got " + f.getSource());
                }
            }
        }
        if (c.getKeyColumns() != null) {
            for (String key : c.getKeyColumns()) {
                if (!columns.contains(key)) {
                    throw new ConnectorConfigException(
                            c.getName() + ": key-column " + key + " is not in the field mapping");
                }
            }
        }
        if (c.getBatchSize() <= 0) {
            throw new ConnectorConfigException(c.getName() + ": batch-size must be positive");
        }
    }

    public static String resolvedHost(AmpsProperties amps, ConnectorProperties c) {
        if (c.getHost() != null && !c.getHost().isBlank()) {
            return c.getHost().trim();
        }
        return amps.getDefaultHost() == null ? "" : amps.getDefaultHost().trim();
    }

    public static int resolvedPort(AmpsProperties amps, ConnectorProperties c) {
        if (c.getPort() > 0) {
            return c.getPort();
        }
        return amps.getDefaultPort();
    }

    public static String resolvedUri(AmpsProperties amps, ConnectorProperties c) {
        if (c.getUri() != null && !c.getUri().isBlank()) {
            return c.getUri().trim();
        }
        String user = c.getUsername() == null ? "" : c.getUsername().trim();
        String pass = c.getPassword() == null ? "" : c.getPassword();
        String auth = "";
        if (!user.isBlank()) {
            auth = encodeUriUser(user);
            if (!pass.isBlank()) {
                auth = auth + ":" + encodeUriUser(pass);
            }
            auth = auth + "@";
        }
        return "tcp://" + auth + resolvedHost(amps, c) + ":" + resolvedPort(amps, c) + "/amps/"
                + c.resolvedMessageType();
    }

    private static String encodeUriUser(String value) {
        return java.net.URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
    }
}
