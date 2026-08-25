package com.deephaven.fix42.amps.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

public class ConnectorProperties {
    @NotBlank
    private String name;

    private boolean enabled = true;

    /** AMPS host. Falls back to {@code amps.default-host} when blank. */
    private String host = "";

    /** AMPS port. Falls back to {@code amps.default-port} when 0. */
    private int port;

    private String username = "";
    private String password = "";
    private String clientName = "";

    /**
     * Optional full AMPS URI. When set, host/port/message-type are ignored for connect.
     * Example: {@code tcp://amps:9007/amps/json}
     */
    private String uri = "";

    @NotBlank
    private String topic;

    @NotNull
    private TopicKind topicKind = TopicKind.SOW;

    @NotNull
    private DataFormat dataFormat = DataFormat.JSON;

    /**
     * AMPS protocol message type in the URI path ({@code /amps/{messageType}}).
     * Defaults to the data-format name when blank.
     */
    private String messageType = "";

    @NotNull
    private UpdateMode subscriberMode = UpdateMode.FULL;

    @NotNull
    private UpdateMode publisherMode = UpdateMode.FULL;

    @NotBlank
    private String tableName;

    private List<String> keyColumns = new ArrayList<>();

    private String filter = "";

    private int batchSize = 50;

    @Valid
    @NotEmpty
    private List<FieldMappingProperties> fields = new ArrayList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public TopicKind getTopicKind() {
        return topicKind;
    }

    public void setTopicKind(TopicKind topicKind) {
        this.topicKind = topicKind;
    }

    public DataFormat getDataFormat() {
        return dataFormat;
    }

    public void setDataFormat(DataFormat dataFormat) {
        this.dataFormat = dataFormat;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public UpdateMode getSubscriberMode() {
        return subscriberMode;
    }

    public void setSubscriberMode(UpdateMode subscriberMode) {
        this.subscriberMode = subscriberMode;
    }

    public UpdateMode getPublisherMode() {
        return publisherMode;
    }

    public void setPublisherMode(UpdateMode publisherMode) {
        this.publisherMode = publisherMode;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(List<String> keyColumns) {
        this.keyColumns = keyColumns == null ? new ArrayList<>() : keyColumns;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public List<FieldMappingProperties> getFields() {
        return fields;
    }

    public void setFields(List<FieldMappingProperties> fields) {
        this.fields = fields == null ? new ArrayList<>() : fields;
    }

    public String resolvedMessageType() {
        if (messageType != null && !messageType.isBlank()) {
            return messageType.trim();
        }
        return dataFormat.name().toLowerCase();
    }

    public String resolvedClientName() {
        if (clientName != null && !clientName.isBlank()) {
            return clientName.trim();
        }
        return "dh-amps-" + name;
    }
}
