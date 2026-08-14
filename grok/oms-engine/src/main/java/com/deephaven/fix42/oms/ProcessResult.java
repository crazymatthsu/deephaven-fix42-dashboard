package com.deephaven.fix42.oms;

public final class ProcessResult {
    private final String orderKey;
    private final String previousOrderKey;
    private final OrderState state;
    private final boolean created;
    private final boolean applied;
    private final boolean tombstone;
    private final String rawFix;
    private final String msgType;

    public ProcessResult(
            String orderKey,
            String previousOrderKey,
            OrderState state,
            boolean created,
            boolean applied,
            boolean tombstone,
            String rawFix,
            String msgType) {
        this.orderKey = orderKey == null ? "" : orderKey;
        this.previousOrderKey = previousOrderKey == null ? "" : previousOrderKey;
        this.state = state;
        this.created = created;
        this.applied = applied;
        this.tombstone = tombstone;
        this.rawFix = rawFix == null ? "" : rawFix;
        this.msgType = msgType == null ? "" : msgType;
    }

    public String getOrderKey() {
        return orderKey;
    }

    public String getPreviousOrderKey() {
        return previousOrderKey;
    }

    public OrderState getState() {
        return state;
    }

    public boolean isCreated() {
        return created;
    }

    public boolean isApplied() {
        return applied;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    public String getRawFix() {
        return rawFix;
    }

    public String getMsgType() {
        return msgType;
    }
}
