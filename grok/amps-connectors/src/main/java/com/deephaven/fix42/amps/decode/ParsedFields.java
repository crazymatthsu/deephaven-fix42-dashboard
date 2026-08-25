package com.deephaven.fix42.amps.decode;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Source-tag values present in one AMPS payload. Unconfigured sources are never stored. */
public final class ParsedFields {
    private final Map<String, String> values;

    public ParsedFields(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    public static ParsedFields empty() {
        return new ParsedFields(Map.of());
    }

    public boolean isPresent(String source) {
        return values.containsKey(source);
    }

    public Optional<String> get(String source) {
        if (!values.containsKey(source)) {
            return Optional.empty();
        }
        return Optional.ofNullable(values.get(source));
    }

    public Set<String> sources() {
        return values.keySet();
    }

    public Map<String, String> asMap() {
        return values;
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ParsedFields that)) {
            return false;
        }
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }
}
