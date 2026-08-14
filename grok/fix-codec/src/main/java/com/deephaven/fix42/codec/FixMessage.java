package com.deephaven.fix42.codec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Ordered, lossless tag/value list. Unknown tags are kept. */
public final class FixMessage {
    private final List<FixField> fields;
    private final String raw;

    public FixMessage(List<FixField> fields, String raw) {
        this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
        this.raw = raw == null ? "" : raw;
    }

    public List<FixField> fields() {
        return fields;
    }

    public String raw() {
        return raw;
    }

    public Optional<String> get(int tag) {
        for (FixField field : fields) {
            if (field.tag() == tag) {
                return Optional.of(field.value());
            }
        }
        return Optional.empty();
    }

    public String getOrEmpty(int tag) {
        return get(tag).orElse("");
    }

    public String msgType() {
        return getOrEmpty(Tags.MSG_TYPE);
    }

    public boolean has(int tag) {
        return get(tag).isPresent();
    }
}
