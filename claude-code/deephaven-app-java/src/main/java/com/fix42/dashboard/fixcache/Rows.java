package com.fix42.dashboard.fixcache;

/** Shared rendering helpers for the published row types. */
final class Rows {

    private Rows() {}

    /**
     * Renders an enum member as its readable name, {@code ""} when unset.
     *
     * <p>Port of {@code fix42cache.model._name}.
     */
    static String name(Enum<?> value) {
        return value != null ? value.name() : "";
    }
}
