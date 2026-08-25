package com.deephaven.fix42.amps.map;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** One Deephaven-bound row. Only configured columns appear. */
public final class MappedRow {
    private final Map<String, Object> values;
    private final Set<String> present;

    public MappedRow(Map<String, Object> values, Set<String> present) {
        this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        this.present = Collections.unmodifiableSet(present);
    }

    public static MappedRow empty() {
        return new MappedRow(Map.of(), Set.of());
    }

    public boolean isPresent(String column) {
        return present.contains(column);
    }

    public Object get(String column) {
        return values.get(column);
    }

    public Map<String, Object> values() {
        return values;
    }

    public Set<String> present() {
        return present;
    }

    public boolean isEmpty() {
        return present.isEmpty();
    }

    public MappedRow copy() {
        return new MappedRow(values, present);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MappedRow that)) {
            return false;
        }
        return values.equals(that.values) && present.equals(that.present);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, present);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
