package com.fix42.dashboard.gen;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Test-only helpers for reading a multi-OMS batch back the way the blotter app would. */
final class MultiOmsTestFix {

    private MultiOmsTestFix() {}

    static final Instant BASE = Instant.parse("2025-08-14T12:00:00Z");

    static MultiOmsScenarioEngine engine(long seed, int children) {
        return new MultiOmsScenarioEngine(seed, children, BASE);
    }

    static MultiOmsScenarioEngine.GeneratedBatch batch(long seed, int children, int families, String selector) {
        return engine(seed, children).generate(families, selector);
    }

    static MultiOmsScenarioEngine.GeneratedBatch batch(
            MultiOmsScenarioCatalog scenario, long seed, int children, int families) {
        return batch(seed, children, families, scenario.cliName());
    }

    /** Every message of the batch, parsed, in wire order. */
    static List<Map<Integer, String>> parsed(MultiOmsScenarioEngine.GeneratedBatch batch) {
        return batch.messages().stream().map(MultiOmsTestFix::parse).toList();
    }

    static Map<Integer, String> parse(MultiOmsScenarioEngine.EmittedMessage emitted) {
        return TestFix.parse(FixSerializer.serialize(emitted.message()));
    }

    /**
     * The batch grouped into one tape per hub order, keyed by {@code GlobalKey} — the same identity
     * the blotter derives, because no {@code D} carries {@code 37} and nothing rotates a ClOrdID.
     */
    static Map<String, List<Map<Integer, String>>> byOrder(MultiOmsScenarioEngine.GeneratedBatch batch) {
        Map<String, List<Map<Integer, String>>> tapes = new LinkedHashMap<>();
        for (MultiOmsScenarioEngine.EmittedMessage emitted : batch.messages()) {
            tapes.computeIfAbsent(MultiOmsTopology.globalKey(emitted.oms(), emitted.chainKey()),
                    k -> new ArrayList<>()).add(parse(emitted));
        }
        return tapes;
    }

    static MultiOmsTopology.Hub hub(String name) {
        return MultiOmsTopology.HUBS.stream()
                .filter(h -> h.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("unknown hub " + name));
    }

    static boolean isD(Map<Integer, String> m) {
        return FixTags.MSG_NEW_ORDER_SINGLE.equals(m.get(FixTags.MSG_TYPE));
    }

    static boolean isExec(Map<Integer, String> m) {
        return FixTags.MSG_EXECUTION_REPORT.equals(m.get(FixTags.MSG_TYPE));
    }

    static boolean isFill(Map<Integer, String> m) {
        return isExec(m)
                && (FixTags.EXEC_TYPE_PARTIAL_FILL.equals(m.get(FixTags.EXEC_TYPE))
                        || FixTags.EXEC_TYPE_FILL.equals(m.get(FixTags.EXEC_TYPE)));
    }

    static double num(Map<Integer, String> m, int tag) {
        return Double.parseDouble(m.get(tag));
    }

    /** Renders a whole batch as keyed wire strings, for determinism comparisons. */
    static List<String> rendered(MultiOmsScenarioEngine.GeneratedBatch batch) {
        return batch.messages().stream()
                .map(m -> m.topic() + " " + m.chainKey() + " " + FixSerializer.serialize(m.message()))
                .toList();
    }

    // ---------------------------------------------------------------- independent recon

    /** One blotter row as rebuilt from the wire alone, with no reference to the expected export. */
    record ReconRow(
            String oms,
            String clOrdID,
            String orderID,
            String extOrdID,
            String globalKey,
            String parentGlobalKey,
            String rootGlobalKey,
            String linkState,
            String ordStatus,
            double orderQty,
            double cumQty,
            double leavesQty,
            double avgPx,
            String breakKind) {}

    /**
     * Replays a batch through the app's own algorithm (doc 09 §5.2–§5.4): fold each tape to its
     * last snapshot, index every id each hub published, resolve link values against the upstream
     * hub, walk to the root, then roll each parent up against its <em>direct</em> children.
     *
     * <p>Deliberately independent of {@link MultiOmsScenarioEngine}'s own book-keeping — the point
     * of the expected export is that both arrive at the same answer.
     */
    static Recon recon(MultiOmsScenarioEngine.GeneratedBatch batch) {
        Map<String, Map<Integer, String>> newOrder = new LinkedHashMap<>();
        Map<String, Map<Integer, String>> lastExec = new LinkedHashMap<>();
        Map<String, String> omsOf = new LinkedHashMap<>();
        Map<String, String> idIndex = new LinkedHashMap<>();

        for (MultiOmsScenarioEngine.EmittedMessage emitted : batch.messages()) {
            String globalKey = MultiOmsTopology.globalKey(emitted.oms(), emitted.chainKey());
            Map<Integer, String> m = parse(emitted);
            omsOf.put(globalKey, emitted.oms());
            idIndex.put(emitted.oms() + "|" + m.get(FixTags.CL_ORD_ID), globalKey);
            if (m.containsKey(FixTags.ORDER_ID)) {
                idIndex.put(emitted.oms() + "|" + m.get(FixTags.ORDER_ID), globalKey);
            }
            if (isD(m)) {
                newOrder.putIfAbsent(globalKey, m);
            } else {
                lastExec.put(globalKey, m);
            }
        }

        Map<String, String> parentOf = new LinkedHashMap<>();
        Map<String, ReconRow> rows = new LinkedHashMap<>();
        for (Map.Entry<String, Map<Integer, String>> entry : newOrder.entrySet()) {
            String globalKey = entry.getKey();
            Map<Integer, String> order = entry.getValue();
            MultiOmsTopology.Hub hub = hub(omsOf.get(globalKey));
            String extOrdID = hub.isRoot() ? "" : order.getOrDefault(hub.linkTag(), "");
            String parent = extOrdID.isEmpty() ? null : idIndex.get(hub.upstream() + "|" + extOrdID);
            if (parent != null) {
                parentOf.put(globalKey, parent);
            }
            String linkState = extOrdID.isEmpty()
                    ? (hub.isRoot() ? ExpectedOmsOrder.LINK_ROOT : "NO_LINK")
                    : (parent != null ? ExpectedOmsOrder.LINK_LINKED : ExpectedOmsOrder.LINK_DANGLING);

            Map<Integer, String> last = lastExec.get(globalKey);
            double orderQty = num(order, FixTags.ORDER_QTY);
            rows.put(globalKey, new ReconRow(
                    hub.name(),
                    order.get(FixTags.CL_ORD_ID),
                    last == null ? "" : last.get(FixTags.ORDER_ID),
                    extOrdID,
                    globalKey,
                    parent,
                    globalKey,   // filled in below
                    linkState,
                    FixTags.ordStatusName(last == null ? FixTags.ORD_STATUS_PENDING_NEW
                            : last.get(FixTags.ORD_STATUS)),
                    orderQty,
                    last == null ? 0 : num(last, FixTags.CUM_QTY),
                    last == null ? orderQty : num(last, FixTags.LEAVES_QTY),
                    last == null ? 0 : num(last, FixTags.AVG_PX),
                    ExpectedOmsOrder.BREAK_NONE));   // filled in below
        }

        Map<String, List<ReconRow>> childrenOf = new LinkedHashMap<>();
        rows.values().stream()
                .filter(row -> row.parentGlobalKey() != null)
                .forEach(row -> childrenOf.computeIfAbsent(row.parentGlobalKey(), k -> new ArrayList<>())
                        .add(row));

        Map<String, ReconRow> resolved = new LinkedHashMap<>();
        for (ReconRow row : rows.values()) {
            String root = row.globalKey();
            for (int hop = 0; hop < MultiOmsTopology.HUBS.size() && parentOf.containsKey(root); hop++) {
                root = parentOf.get(root);
            }
            List<ReconRow> children = childrenOf.getOrDefault(row.globalKey(), List.of());
            double childCum = children.stream().mapToDouble(ReconRow::cumQty).sum();
            double childLeaves = children.stream().mapToDouble(ReconRow::leavesQty).sum();
            double childNotional = children.stream().mapToDouble(c -> c.avgPx() * c.cumQty()).sum();
            String breakKind = ExpectedOmsOrder.breakKind(row.linkState(), !children.isEmpty(),
                    row.cumQty() - childCum,
                    row.avgPx() * row.cumQty() - childNotional,
                    row.leavesQty() - childLeaves);
            resolved.put(row.globalKey(), new ReconRow(row.oms(), row.clOrdID(), row.orderID(),
                    row.extOrdID(), row.globalKey(), row.parentGlobalKey(), root, row.linkState(),
                    row.ordStatus(), row.orderQty(), row.cumQty(), row.leavesQty(), row.avgPx(),
                    breakKind));
        }
        return new Recon(resolved, childrenOf);
    }

    /** The whole batch reconciled independently of the generator's export. */
    record Recon(Map<String, ReconRow> rows, Map<String, List<ReconRow>> childrenByParent) {

        ReconRow row(String globalKey) {
            ReconRow row = rows.get(globalKey);
            if (row == null) {
                throw new AssertionError("no row for " + globalKey);
            }
            return row;
        }

        List<ReconRow> childrenOf(String globalKey) {
            return childrenByParent.getOrDefault(globalKey, List.of());
        }

        List<ReconRow> ofOms(String oms) {
            return rows.values().stream().filter(row -> row.oms().equals(oms)).toList();
        }
    }
}
