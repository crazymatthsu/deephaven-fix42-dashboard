package com.deephaven.fix42.oms;

/** Factory used from Deephaven Python via jpy. */
public final class OmsCaches {
    private OmsCaches() {}

    public static InMemoryOmsCache createDefault() {
        return new InMemoryOmsCache(CacheConfig.defaults());
    }

    public static InMemoryOmsCache createTesting() {
        return new InMemoryOmsCache(CacheConfig.testing());
    }
}
