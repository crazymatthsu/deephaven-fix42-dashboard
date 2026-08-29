package com.fix42.dashboard.gen;

import java.util.List;

/**
 * The default 4-hub drop-copy topology frozen by {@code docs/09-multi-oms-blotter.md} §3.
 *
 * <pre>
 * OMS-A  --&gt;  OMS-B-parent  --&gt;  OMS-B-child (1..n per parent)  --&gt;  OMS-C (1 per child)
 *              16666=A.ClOrdID     16667=B-parent.ClOrdID            16668=B-child.ClOrdID
 * </pre>
 *
 * <p>The generator only ever emits this hard-coded topology: the blotter app reads the same shape
 * from {@code MULTIOMS_HUBS}, whose default is exactly the table below, so a run of
 * {@code --multi-oms} lands on the topics that app consumes without any configuration.
 *
 * <p>{@code OMS-B-parent} and {@code OMS-B-child} are two hubs on purpose — parent/child inside
 * OMS-B is the same edge shape as a cross-hub route, so one link tag mechanism handles both.
 */
public final class MultiOmsTopology {

    private MultiOmsTopology() {}

    /**
     * One hub of the topology.
     *
     * @param name     hub name; the {@code Oms} column and the tape's {@code 49 SenderCompID}
     * @param topic    Kafka topic carrying this hub's drop-copy tape
     * @param upstream name of the hub this one routes from ({@code ""} for a root)
     * @param linkTag  tag on this hub's {@code D} holding the upstream order's ClOrdID ({@code 0} for a root)
     * @param depth    distance from the root hub; the root is {@code 0}
     */
    public record Hub(String name, String topic, String upstream, int linkTag, int depth) {

        /** A root hub has no upstream and its orders carry no link tag. */
        public boolean isRoot() {
            return upstream.isEmpty();
        }
    }

    public static final Hub OMS_A =
            new Hub("OMS-A", "fix42.oms-a", "", 0, 0);

    public static final Hub OMS_B_PARENT =
            new Hub("OMS-B-parent", "fix42.oms-b-parent", OMS_A.name(), FixTags.EXT_ORDER_ID_A_TO_B, 1);

    public static final Hub OMS_B_CHILD =
            new Hub("OMS-B-child", "fix42.oms-b-child", OMS_B_PARENT.name(),
                    FixTags.EXT_ORDER_ID_B_PARENT_TO_CHILD, 2);

    public static final Hub OMS_C =
            new Hub("OMS-C", "fix42.oms-c", OMS_B_CHILD.name(), FixTags.EXT_ORDER_ID_C_TO_B_CHILD, 3);

    /** Every hub, upstream first. */
    public static final List<Hub> HUBS = List.of(OMS_A, OMS_B_PARENT, OMS_B_CHILD, OMS_C);

    /** Every hub topic, in {@link #HUBS} order. */
    public static List<String> topics() {
        return HUBS.stream().map(Hub::topic).toList();
    }

    /**
     * The blotter's cross-hub row identity ({@code docs/09-multi-oms-blotter.md} §4): the hub name
     * and the chain's {@code OrderKey}, which for these tapes is always the {@code D}'s ClOrdID
     * because no {@code D} carries {@code 37 OrderID}.
     */
    public static String globalKey(String oms, String orderKey) {
        return oms + "|" + orderKey;
    }
}
