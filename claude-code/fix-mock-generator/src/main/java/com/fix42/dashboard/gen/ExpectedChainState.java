package com.fix42.dashboard.gen;

/**
 * The final per-chain state the Deephaven cache must converge to once the whole scripted sequence
 * has been consumed.
 *
 * <p>Exported as JSON by {@code --emit-expected} and asserted by the integration test against
 * {@code order_state_latest}. Enum-ish values use the readable names of
 * {@code docs/01-fix42-messages-and-state-machine.md} §4.
 *
 * @param chainKey   Kafka record key / venue OrderID of the chain
 * @param orderId    tag 37 assigned by the mock venue (equal to {@code chainKey})
 * @param scenario   catalog name that produced this chain
 * @param ordStatus  terminal {@code OrdStatus} name (e.g. {@code FILLED})
 * @param cumQty     terminal {@code CumQty}
 * @param leavesQty  terminal {@code LeavesQty}
 * @param clOrdID    current {@code ClOrdID} (rotates only on a replace confirm, {@code 150=5})
 */
public record ExpectedChainState(
        String chainKey,
        String orderId,
        String scenario,
        String ordStatus,
        double cumQty,
        double leavesQty,
        String clOrdID) {

    /** Renders one JSON object using the Deephaven column names. */
    public String toJson() {
        return "{"
                + "\"ChainKey\":" + jsonString(chainKey) + ","
                + "\"OrderID\":" + jsonString(orderId) + ","
                + "\"Scenario\":" + jsonString(scenario) + ","
                + "\"OrdStatus\":" + jsonString(ordStatus) + ","
                + "\"CumQty\":" + Double.toString(cumQty) + ","
                + "\"LeavesQty\":" + Double.toString(leavesQty) + ","
                + "\"ClOrdID\":" + jsonString(clOrdID)
                + "}";
    }

    static String jsonString(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
