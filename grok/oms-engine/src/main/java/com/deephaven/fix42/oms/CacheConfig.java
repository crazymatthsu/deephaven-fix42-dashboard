package com.deephaven.fix42.oms;

import com.deephaven.fix42.codec.ParserConfig;
import com.deephaven.fix42.codec.Tags;

public final class CacheConfig {
    private final int historyLimit;
    private final boolean applyStaleExecReports;
    private final int parentOrderIdTag;
    private final int parentClOrdIdTag;
    private final ParserConfig parserConfig;

    public CacheConfig(
            int historyLimit,
            boolean applyStaleExecReports,
            int parentOrderIdTag,
            int parentClOrdIdTag,
            ParserConfig parserConfig) {
        this.historyLimit = historyLimit;
        this.applyStaleExecReports = applyStaleExecReports;
        this.parentOrderIdTag = parentOrderIdTag;
        this.parentClOrdIdTag = parentClOrdIdTag;
        this.parserConfig = parserConfig;
    }

    public static CacheConfig defaults() {
        return new CacheConfig(32, false, Tags.PARENT_ORDER_ID, Tags.PARENT_CL_ORD_ID, ParserConfig.defaults());
    }

    public static CacheConfig testing() {
        return new CacheConfig(32, false, Tags.PARENT_ORDER_ID, Tags.PARENT_CL_ORD_ID, ParserConfig.lenient());
    }

    public int historyLimit() {
        return historyLimit;
    }

    public boolean applyStaleExecReports() {
        return applyStaleExecReports;
    }

    public int parentOrderIdTag() {
        return parentOrderIdTag;
    }

    public int parentClOrdIdTag() {
        return parentClOrdIdTag;
    }

    public ParserConfig parserConfig() {
        return parserConfig;
    }
}
