package com.fix42.dashboard.amps.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

/**
 * One AMPS topic bridged into one Deephaven table.
 *
 * <p>An {@code amps-connectors} application runs one or more of these, each with its own
 * AMPS connection, subscription and target table (doc 07 section 2).
 */
public class ConnectorProperties {

    /** Connector name; used in logs, as the default AMPS client name, and must be unique. */
    @NotBlank
    private String name;

    /** Set {@code false} to keep a connector configured but not started. */
    private boolean enabled = true;

    /** Wire format of the AMPS payload. */
    @NotNull
    private SourceFormat format = SourceFormat.FIX;

    @Valid
    @NotNull
    private AmpsSourceProperties source = new AmpsSourceProperties();

    @Valid
    @NotNull
    private DeephavenTableProperties deephaven = new DeephavenTableProperties();

    /** Source-field -> column mappings. Fields not listed here are never published. */
    @Valid
    @NotEmpty
    private List<FieldMapping> fields = new ArrayList<>();

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

    public SourceFormat getFormat() {
        return format;
    }

    public void setFormat(SourceFormat format) {
        this.format = format;
    }

    public AmpsSourceProperties getSource() {
        return source;
    }

    public void setSource(AmpsSourceProperties source) {
        this.source = source;
    }

    public DeephavenTableProperties getDeephaven() {
        return deephaven;
    }

    public void setDeephaven(DeephavenTableProperties deephaven) {
        this.deephaven = deephaven;
    }

    public List<FieldMapping> getFields() {
        return fields;
    }

    public void setFields(List<FieldMapping> fields) {
        this.fields = fields == null ? new ArrayList<>() : fields;
    }

    @Override
    public String toString() {
        return "connector '" + name + "' (" + format + " " + source.getTopic() + " -> "
                + deephaven.getTable() + ")";
    }
}
