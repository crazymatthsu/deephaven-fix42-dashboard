package com.fix42.dashboard.gen;

/**
 * The final blotter row one hub-order must produce once its whole tape has been consumed.
 *
 * <p>Exported as JSON by {@code --multi-oms --emit-expected} and asserted by the multi-OMS e2e
 * against {@code orders_recon}, keyed by ({@code Oms}, {@code ClOrdID}). The key set and the column
 * names are frozen by {@code docs/09-multi-oms-blotter.md} §8; the enum-ish values use the readable
 * names of {@code docs/01-fix42-messages-and-state-machine.md} §4.
 *
 * <p>{@link #breakKind} is the generator's own copy of the app's per-edge taxonomy (doc 09 §5.4) —
 * the point of the export is that the two are computed independently and must agree.
 *
 * @param oms            hub name, e.g. {@code OMS-B-child}
 * @param clOrdID        the {@code D}'s ClOrdID; equals the cache's {@code OrderKey} (no {@code D}
 *                       carries {@code 37}, and nothing in this mode rotates the ClOrdID)
 * @param orderID        tag 37 assigned by the hub on its first {@code 8}
 * @param extOrdID       the link tag's value ({@code ""} on a root order)
 * @param globalKey      {@code Oms + "|" + ClOrdID}
 * @param rootGlobalKey  the family's root {@code GlobalKey}; a {@code DANGLING} order is its own root
 * @param scenario       catalog name that produced the family
 * @param ordStatus      terminal {@code OrdStatus} name, e.g. {@code FILLED}
 * @param cumQty         terminal {@code CumQty}
 * @param leavesQty      terminal {@code LeavesQty}
 * @param avgPx          terminal {@code AvgPx}
 * @param linkState      {@code ROOT} / {@code LINKED} / {@code DANGLING}
 * @param breakKind      {@code NONE} / {@code UNROUTED} / {@code QTY_BREAK} / {@code NOTIONAL_BREAK}
 *                       / {@code DANGLING}
 */
public record ExpectedOmsOrder(
        String oms,
        String clOrdID,
        String orderID,
        String extOrdID,
        String globalKey,
        String rootGlobalKey,
        String scenario,
        String ordStatus,
        double cumQty,
        double leavesQty,
        double avgPx,
        String linkState,
        String breakKind) {

    /** Default {@code MULTIOMS_QTY_TOL} (doc 09 §3). */
    public static final double QTY_TOL = 1e-6;

    /** Default {@code MULTIOMS_NOTIONAL_TOL} (doc 09 §3). */
    public static final double NOTIONAL_TOL = 0.01;

    public static final String LINK_ROOT = "ROOT";
    public static final String LINK_LINKED = "LINKED";
    public static final String LINK_DANGLING = "DANGLING";

    public static final String BREAK_NONE = "NONE";
    public static final String BREAK_UNROUTED = "UNROUTED";
    public static final String BREAK_QTY = "QTY_BREAK";
    public static final String BREAK_NOTIONAL = "NOTIONAL_BREAK";
    public static final String BREAK_DANGLING = "DANGLING";

    /**
     * Classifies one edge exactly as {@code orders_recon} does (doc 09 §5.4): an unresolved link
     * wins outright, then quantity, then notional, then the amber unrouted remainder. Leaf orders
     * and parents whose direct children roll up cleanly are {@code NONE}.
     *
     * @param linkState      this order's {@code LinkState}
     * @param hasChildren    whether any order links to this one
     * @param deltaCumQty    own {@code CumQty} minus the sum over direct children
     * @param deltaNotional  own {@code AvgPx * CumQty} minus the sum over direct children
     * @param deltaLeavesQty own {@code LeavesQty} minus the sum over direct children
     */
    public static String breakKind(String linkState, boolean hasChildren,
            double deltaCumQty, double deltaNotional, double deltaLeavesQty) {
        if (LINK_DANGLING.equals(linkState)) {
            return BREAK_DANGLING;
        }
        if (!hasChildren) {
            return BREAK_NONE;
        }
        if (Math.abs(deltaCumQty) > QTY_TOL) {
            return BREAK_QTY;
        }
        if (Math.abs(deltaNotional) > NOTIONAL_TOL) {
            return BREAK_NOTIONAL;
        }
        if (Math.abs(deltaLeavesQty) > QTY_TOL) {
            return BREAK_UNROUTED;
        }
        return BREAK_NONE;
    }

    /** Renders one JSON object using the blotter's column names. */
    public String toJson() {
        return "{"
                + "\"Oms\":" + ExpectedChainState.jsonString(oms) + ","
                + "\"ClOrdID\":" + ExpectedChainState.jsonString(clOrdID) + ","
                + "\"OrderID\":" + ExpectedChainState.jsonString(orderID) + ","
                + "\"ExtOrdID\":" + ExpectedChainState.jsonString(extOrdID) + ","
                + "\"GlobalKey\":" + ExpectedChainState.jsonString(globalKey) + ","
                + "\"RootGlobalKey\":" + ExpectedChainState.jsonString(rootGlobalKey) + ","
                + "\"Scenario\":" + ExpectedChainState.jsonString(scenario) + ","
                + "\"OrdStatus\":" + ExpectedChainState.jsonString(ordStatus) + ","
                + "\"CumQty\":" + Double.toString(cumQty) + ","
                + "\"LeavesQty\":" + Double.toString(leavesQty) + ","
                + "\"AvgPx\":" + Double.toString(avgPx) + ","
                + "\"LinkState\":" + ExpectedChainState.jsonString(linkState) + ","
                + "\"BreakKind\":" + ExpectedChainState.jsonString(breakKind)
                + "}";
    }
}
