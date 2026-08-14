package com.deephaven.fix42.codec;

import java.util.Objects;

public final class FixField {
    private final int tag;
    private final String value;

    public FixField(int tag, String value) {
        this.tag = tag;
        this.value = value == null ? "" : value;
    }

    public int tag() {
        return tag;
    }

    public String value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FixField that)) {
            return false;
        }
        return tag == that.tag && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tag, value);
    }

    @Override
    public String toString() {
        return tag + "=" + value;
    }
}
