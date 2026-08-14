package com.deephaven.fix42.oms;

public final class ChildRollup {
    private final int childCount;
    private final double orderQty;
    private final double cumQty;
    private final double leavesQty;

    public ChildRollup(int childCount, double orderQty, double cumQty, double leavesQty) {
        this.childCount = childCount;
        this.orderQty = orderQty;
        this.cumQty = cumQty;
        this.leavesQty = leavesQty;
    }

    public int getChildCount() {
        return childCount;
    }

    public double getOrderQty() {
        return orderQty;
    }

    public double getCumQty() {
        return cumQty;
    }

    public double getLeavesQty() {
        return leavesQty;
    }
}
