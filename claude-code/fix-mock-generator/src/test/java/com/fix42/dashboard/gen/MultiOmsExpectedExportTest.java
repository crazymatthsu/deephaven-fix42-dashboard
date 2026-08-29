package com.fix42.dashboard.gen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The {@code --emit-expected} contract of {@code docs/09-multi-oms-blotter.md} §8: one row per
 * hub-order, and per-scenario the exact break the blotter's §5.4 taxonomy must report.
 *
 * <p>{@link #exportMatchesAnIndependentRecon} is the load-bearing case — it rebuilds every row from
 * the emitted messages alone and requires the generator's own edge math to agree.
 */
class MultiOmsExpectedExportTest {

    private static final int CHILDREN = 3;
    private static final int FAMILIES = 8;

    private static String parentOms() {
        return MultiOmsTopology.OMS_B_PARENT.name();
    }

    private static Set<String> keysWithBreak(MultiOmsScenarioEngine.GeneratedBatch batch, String breakKind) {
        return batch.expectedOrders().stream()
                .filter(order -> order.breakKind().equals(breakKind))
                .map(ExpectedOmsOrder::globalKey)
                .collect(Collectors.toSet());
    }

    // ---------------------------------------------------------------- shape

    @Test
    @DisplayName("one row per hub-order, GlobalKey = Oms|ClOrdID, and ClOrdID is the D's")
    void oneRowPerHubOrder() {
        MultiOmsScenarioEngine.GeneratedBatch batch = MultiOmsTestFix.batch(42L, CHILDREN, 20, "all");
        Map<String, List<Map<Integer, String>>> tapes = MultiOmsTestFix.byOrder(batch);
        List<ExpectedOmsOrder> orders = batch.expectedOrders();

        assertEquals(tapes.size(), orders.size(), "every tape must have exactly one exported row");
        assertEquals(tapes.keySet(), orders.stream().map(ExpectedOmsOrder::globalKey)
                .collect(Collectors.toSet()));

        for (ExpectedOmsOrder order : orders) {
            List<Map<Integer, String>> tape = tapes.get(order.globalKey());
            assertEquals(MultiOmsTopology.globalKey(order.oms(), order.clOrdID()), order.globalKey());
            assertEquals(order.clOrdID(), tape.get(0).get(FixTags.CL_ORD_ID),
                    "ClOrdID must be the D's, which is the cache's OrderKey");
            assertFalse(tape.get(0).containsKey(FixTags.ORDER_ID));
            assertTrue(MultiOmsTopology.HUBS.stream().anyMatch(h -> h.name().equals(order.oms())));
            assertTrue(List.of(ExpectedOmsOrder.LINK_ROOT, ExpectedOmsOrder.LINK_LINKED,
                    ExpectedOmsOrder.LINK_DANGLING).contains(order.linkState()), order.linkState());
        }
    }

    @Test
    @DisplayName("every row's terminal state is the last snapshot on its own tape")
    void rowsMatchTheirTape() {
        MultiOmsScenarioEngine.GeneratedBatch batch = MultiOmsTestFix.batch(42L, CHILDREN, 20, "all");
        Map<String, List<Map<Integer, String>>> tapes = MultiOmsTestFix.byOrder(batch);

        for (ExpectedOmsOrder order : batch.expectedOrders()) {
            List<Map<Integer, String>> tape = tapes.get(order.globalKey());
            Map<Integer, String> last = tape.stream().filter(MultiOmsTestFix::isExec)
                    .reduce((a, b) -> b).orElseThrow();
            assertEquals(FixTags.ordStatusName(last.get(FixTags.ORD_STATUS)), order.ordStatus(),
                    order.globalKey());
            assertEquals(MultiOmsTestFix.num(last, FixTags.CUM_QTY), order.cumQty(), order.globalKey());
            assertEquals(MultiOmsTestFix.num(last, FixTags.LEAVES_QTY), order.leavesQty(), order.globalKey());
            assertEquals(MultiOmsTestFix.num(last, FixTags.AVG_PX), order.avgPx(), order.globalKey());
            assertEquals(last.get(FixTags.ORDER_ID), order.orderID(), order.globalKey());
            MultiOmsTopology.Hub hub = MultiOmsTestFix.hub(order.oms());
            assertEquals(hub.isRoot() ? "" : tape.get(0).get(hub.linkTag()), order.extOrdID(),
                    order.globalKey());
        }
    }

    @Test
    @DisplayName("every family member shares the OMS-A RootGlobalKey; a dangling order roots at itself")
    void rootGlobalKeyIsTheFamilyId() {
        MultiOmsScenarioEngine.GeneratedBatch batch = MultiOmsTestFix.batch(42L, CHILDREN, 20, "all");
        for (MultiOmsScenarioEngine.MultiOmsChain chain : batch.chains()) {
            assertTrue(chain.rootGlobalKey().startsWith(MultiOmsTopology.OMS_A.name() + "|"));
            for (ExpectedOmsOrder order : chain.orders()) {
                if (order.linkState().equals(ExpectedOmsOrder.LINK_DANGLING)) {
                    assertEquals(order.globalKey(), order.rootGlobalKey(),
                            "a dangling order is its own root until its parent appears");
                } else {
                    assertEquals(chain.rootGlobalKey(), order.rootGlobalKey(), order.globalKey());
                }
            }
        }
    }

    @Test
    @DisplayName("the JSON array carries exactly the doc 09 §8 keys, with numeric quantities")
    void jsonShape() {
        MultiOmsScenarioEngine.GeneratedBatch batch =
                MultiOmsTestFix.batch(MultiOmsScenarioCatalog.CLEAN_FILL, 42L, 1, 1);
        String json = GeneratorMain.toOmsJson(batch.expectedOrders());

        List<String> columns = List.of("Oms", "ClOrdID", "OrderID", "ExtOrdID", "GlobalKey", "RootGlobalKey",
                "Scenario", "OrdStatus", "CumQty", "LeavesQty", "AvgPx", "LinkState", "BreakKind");
        assertTrue(json.startsWith("[\n  {"));
        assertTrue(json.strip().endsWith("]"));
        for (String column : columns) {
            assertEquals(batch.expectedOrders().size(), json.split("\"" + column + "\":", -1).length - 1,
                    column + " must appear once per row");
        }
        // No key beyond the frozen list: every `"..":` in one object is one of them.
        String first = batch.expectedOrders().get(0).toJson();
        assertEquals(columns.size(), first.split("\":", -1).length - 1, first);
        assertTrue(first.matches("(?s).*\"CumQty\":\\d+\\.\\d+.*"), "quantities render as JSON numbers");
        assertTrue(json.contains("\"OMS-A|A-0001\""));
        assertTrue(json.contains("\"clean_fill\""));
        assertTrue(json.contains("\"ExtOrdID\":\"\""), "a root order exports an empty ExtOrdID");
    }

    // ---------------------------------------------------------------- the cross-check

    @ParameterizedTest(name = "{0}")
    @EnumSource(MultiOmsScenarioCatalog.class)
    @DisplayName("the export equals a recon rebuilt independently from the emitted messages")
    void exportMatchesAnIndependentRecon(MultiOmsScenarioCatalog scenario) {
        MultiOmsScenarioEngine.GeneratedBatch batch = MultiOmsTestFix.batch(scenario, 99L, CHILDREN, FAMILIES);
        MultiOmsTestFix.Recon recon = MultiOmsTestFix.recon(batch);

        assertEquals(recon.rows().size(), batch.expectedOrders().size());
        for (ExpectedOmsOrder order : batch.expectedOrders()) {
            MultiOmsTestFix.ReconRow row = recon.row(order.globalKey());
            assertEquals(row.oms(), order.oms(), order.globalKey());
            assertEquals(row.clOrdID(), order.clOrdID(), order.globalKey());
            assertEquals(row.orderID(), order.orderID(), order.globalKey());
            assertEquals(row.extOrdID(), order.extOrdID(), order.globalKey());
            assertEquals(row.rootGlobalKey(), order.rootGlobalKey(), order.globalKey());
            assertEquals(row.linkState(), order.linkState(), order.globalKey());
            assertEquals(row.ordStatus(), order.ordStatus(), order.globalKey());
            assertEquals(row.cumQty(), order.cumQty(), 1e-9, order.globalKey());
            assertEquals(row.leavesQty(), order.leavesQty(), 1e-9, order.globalKey());
            assertEquals(row.avgPx(), order.avgPx(), 1e-9, order.globalKey());
            assertEquals(row.breakKind(), order.breakKind(),
                    order.globalKey() + ": the generator's edge math must match the app's");
            assertEquals(scenario.cliName(), order.scenario());
        }
    }

    @Test
    @DisplayName("the taxonomy's precedence is DANGLING > QTY > NOTIONAL > UNROUTED > NONE")
    void breakKindPrecedence() {
        assertEquals(ExpectedOmsOrder.BREAK_DANGLING, ExpectedOmsOrder.breakKind(
                ExpectedOmsOrder.LINK_DANGLING, true, 10, 100, 10));
        assertEquals(ExpectedOmsOrder.BREAK_QTY, ExpectedOmsOrder.breakKind(
                ExpectedOmsOrder.LINK_LINKED, true, 10, 100, 10));
        assertEquals(ExpectedOmsOrder.BREAK_NOTIONAL, ExpectedOmsOrder.breakKind(
                ExpectedOmsOrder.LINK_LINKED, true, 0, 100, 10));
        assertEquals(ExpectedOmsOrder.BREAK_UNROUTED, ExpectedOmsOrder.breakKind(
                ExpectedOmsOrder.LINK_LINKED, true, 0, 0, 10));
        assertEquals(ExpectedOmsOrder.BREAK_NONE, ExpectedOmsOrder.breakKind(
                ExpectedOmsOrder.LINK_LINKED, true, 0, 0, 0));
        // A leaf order never breaks, and the tolerances are the doc 09 §3 defaults.
        assertEquals(ExpectedOmsOrder.BREAK_NONE, ExpectedOmsOrder.breakKind(
                ExpectedOmsOrder.LINK_LINKED, false, 99, 99, 99));
        assertEquals(ExpectedOmsOrder.BREAK_NONE, ExpectedOmsOrder.breakKind(
                ExpectedOmsOrder.LINK_ROOT, true, 1e-7, 0.009, 1e-7));
        assertEquals(ExpectedOmsOrder.BREAK_QTY, ExpectedOmsOrder.breakKind(
                ExpectedOmsOrder.LINK_ROOT, true, 1e-5, 0, 0));
        assertEquals(ExpectedOmsOrder.BREAK_NOTIONAL, ExpectedOmsOrder.breakKind(
                ExpectedOmsOrder.LINK_ROOT, true, 0, 0.011, 0));
    }

    // ---------------------------------------------------------------- per scenario

    @Test
    @DisplayName("clean_fill: everything FILLED and LINKED, no break anywhere")
    void cleanFill() {
        MultiOmsScenarioEngine.GeneratedBatch batch =
                MultiOmsTestFix.batch(MultiOmsScenarioCatalog.CLEAN_FILL, 21L, CHILDREN, FAMILIES);
        for (ExpectedOmsOrder order : batch.expectedOrders()) {
            assertEquals("FILLED", order.ordStatus(), order.globalKey());
            assertEquals(0.0, order.leavesQty(), order.globalKey());
            assertTrue(order.cumQty() > 0, order.globalKey());
            assertTrue(order.avgPx() > 0, order.globalKey());
            assertEquals(ExpectedOmsOrder.BREAK_NONE, order.breakKind(), order.globalKey());
            assertEquals(order.oms().equals(MultiOmsTopology.OMS_A.name())
                            ? ExpectedOmsOrder.LINK_ROOT : ExpectedOmsOrder.LINK_LINKED,
                    order.linkState(), order.globalKey());
        }
    }

    @Test
    @DisplayName("working_fanout: still working at every level, leaves consistent, no break")
    void workingFanout() {
        MultiOmsScenarioEngine.GeneratedBatch batch =
                MultiOmsTestFix.batch(MultiOmsScenarioCatalog.WORKING_FANOUT, 21L, CHILDREN, FAMILIES);
        for (ExpectedOmsOrder order : batch.expectedOrders()) {
            assertEquals("PARTIALLY_FILLED", order.ordStatus(), order.globalKey());
            assertTrue(order.leavesQty() > 0, order.globalKey() + " should still be working");
            assertTrue(order.cumQty() > 0, order.globalKey());
            assertEquals(ExpectedOmsOrder.BREAK_NONE, order.breakKind(), order.globalKey());
        }
    }

    @Test
    @DisplayName("partial_route: UNROUTED at OMS-B-parent only; A and Bp partially filled, the rest filled")
    void partialRoute() {
        MultiOmsScenarioEngine.GeneratedBatch batch =
                MultiOmsTestFix.batch(MultiOmsScenarioCatalog.PARTIAL_ROUTE, 21L, CHILDREN, FAMILIES);

        Set<String> unrouted = keysWithBreak(batch, ExpectedOmsOrder.BREAK_UNROUTED);
        assertEquals(FAMILIES, unrouted.size(), "one unrouted hop per family");
        assertTrue(unrouted.stream().allMatch(key -> key.startsWith(parentOms() + "|")), unrouted.toString());

        for (ExpectedOmsOrder order : batch.expectedOrders()) {
            if (order.oms().equals(parentOms())) {
                assertEquals(ExpectedOmsOrder.BREAK_UNROUTED, order.breakKind(), order.globalKey());
                assertEquals("PARTIALLY_FILLED", order.ordStatus(), order.globalKey());
                assertTrue(order.leavesQty() > 0, order.globalKey());
            } else if (order.oms().equals(MultiOmsTopology.OMS_A.name())) {
                assertEquals(ExpectedOmsOrder.BREAK_NONE, order.breakKind(),
                        order.globalKey() + ": A's single child mirrors it exactly");
                assertEquals("PARTIALLY_FILLED", order.ordStatus(), order.globalKey());
            } else {
                assertEquals(ExpectedOmsOrder.BREAK_NONE, order.breakKind(), order.globalKey());
                assertEquals("FILLED", order.ordStatus(), order.globalKey());
                assertEquals(0.0, order.leavesQty(), order.globalKey());
            }
        }
        assertTrue(keysWithBreak(batch, ExpectedOmsOrder.BREAK_QTY).isEmpty(), "unrouted is amber, not red");
        assertTrue(keysWithBreak(batch, ExpectedOmsOrder.BREAK_NOTIONAL).isEmpty());
    }

    @ParameterizedTest(name = "--children {0}")
    @ValueSource(ints = {1, 2, 3})
    @DisplayName("missed_fill: the OMS-B-parent tape is exactly one 8 short of the OMS-A tape")
    void missedFillOmitsOneExecutionReport(int children) {
        MultiOmsScenarioEngine.GeneratedBatch batch =
                MultiOmsTestFix.batch(MultiOmsScenarioCatalog.MISSED_FILL, 21L, children, FAMILIES);
        MultiOmsTestFix.Recon recon = MultiOmsTestFix.recon(batch);
        Map<String, List<Map<Integer, String>>> tapes = MultiOmsTestFix.byOrder(batch);

        for (MultiOmsTestFix.ReconRow parent : recon.ofOms(parentOms())) {
            List<Map<Integer, String>> parentTape = tapes.get(parent.globalKey());
            List<Map<Integer, String>> rootTape = tapes.get(parent.parentGlobalKey());
            long parentFills = parentTape.stream().filter(MultiOmsTestFix::isFill).count();
            long rootFills = rootTape.stream().filter(MultiOmsTestFix::isFill).count();

            // OMS-A reports every execution; OMS-B-parent is missing precisely the last one.
            assertEquals(rootFills - 1, parentFills, parent.globalKey());
            assertTrue(parentFills >= 1, parent.globalKey() + ": a fill must survive the gap");
            assertEquals(rootTape.size() - 1, parentTape.size(), parent.globalKey());
            assertTrue(MultiOmsTestFix.isFill(parentTape.get(parentTape.size() - 1)),
                    parent.globalKey() + ": the tape still ends on a fill, just an earlier one");

            // The children never saw a gap, so their executions outnumber the parent's reports.
            long childFills = recon.childrenOf(parent.globalKey()).stream()
                    .mapToLong(child -> tapes.get(child.globalKey()).stream()
                            .filter(MultiOmsTestFix::isFill).count())
                    .sum();
            assertEquals(childFills - 1, parentFills, parent.globalKey());
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 7L, 21L, 42L, 99L})
    @DisplayName("missed_fill: QTY_BREAK on exactly the OMS-A and OMS-B-parent rows, for any fan-out")
    void missedFillBreaksTheUpperEdges(long seed) {
        for (int children = 1; children <= 4; children++) {
            assertMissedFillShape(MultiOmsTestFix.batch(
                    MultiOmsScenarioCatalog.MISSED_FILL, seed, children, FAMILIES));
        }
    }

    private static void assertMissedFillShape(MultiOmsScenarioEngine.GeneratedBatch batch) {
        Set<String> broken = keysWithBreak(batch, ExpectedOmsOrder.BREAK_QTY);
        assertEquals(FAMILIES * 2, broken.size(), "two broken rows per family");
        assertTrue(broken.stream().allMatch(key -> key.startsWith(MultiOmsTopology.OMS_A.name() + "|")
                        || key.startsWith(parentOms() + "|")), broken.toString());

        for (ExpectedOmsOrder order : batch.expectedOrders()) {
            boolean upper = order.oms().equals(MultiOmsTopology.OMS_A.name()) || order.oms().equals(parentOms());
            assertEquals(upper ? ExpectedOmsOrder.BREAK_QTY : ExpectedOmsOrder.BREAK_NONE,
                    order.breakKind(), order.globalKey());
            if (order.oms().equals(parentOms())) {
                assertEquals("PARTIALLY_FILLED", order.ordStatus(),
                        order.globalKey() + ": the withheld report leaves it short, never back at its ack");
                assertTrue(order.leavesQty() > 0, order.globalKey());
                assertTrue(order.cumQty() > 0,
                        order.globalKey() + ": the tape must keep at least one fill");
                assertTrue(order.avgPx() > 0, order.globalKey());
            } else {
                assertEquals("FILLED", order.ordStatus(),
                        order.globalKey() + ": every other tape saw the whole fill");
                assertEquals(0.0, order.leavesQty(), order.globalKey());
            }
        }
    }

    @Test
    @DisplayName("missed_fill: the OMS-B-parent row reports a lower CumQty than OMS-A and its children")
    void missedFillLowersTheParentCumQty() {
        MultiOmsScenarioEngine.GeneratedBatch batch =
                MultiOmsTestFix.batch(MultiOmsScenarioCatalog.MISSED_FILL, 21L, CHILDREN, FAMILIES);
        MultiOmsTestFix.Recon recon = MultiOmsTestFix.recon(batch);

        for (MultiOmsTestFix.ReconRow parent : recon.ofOms(parentOms())) {
            double childCum = recon.childrenOf(parent.globalKey()).stream()
                    .mapToDouble(MultiOmsTestFix.ReconRow::cumQty).sum();
            assertTrue(parent.cumQty() < childCum,
                    parent.globalKey() + ": " + parent.cumQty() + " should trail its children's " + childCum);
            assertEquals(parent.orderQty(), childCum, 1e-9, "the children still filled the whole order");
            assertEquals(childCum, recon.row(parent.parentGlobalKey()).cumQty(), 1e-9,
                    "OMS-A saw every fill; only the OMS-B-parent tape has the gap");
        }
    }

    @Test
    @DisplayName("dangling_child: one extra OMS-C order, DANGLING, while its family stays clean")
    void danglingChild() {
        MultiOmsScenarioEngine.GeneratedBatch batch =
                MultiOmsTestFix.batch(MultiOmsScenarioCatalog.DANGLING_CHILD, 21L, CHILDREN, FAMILIES);

        List<ExpectedOmsOrder> dangling = batch.expectedOrders().stream()
                .filter(order -> order.linkState().equals(ExpectedOmsOrder.LINK_DANGLING))
                .toList();
        assertEquals(FAMILIES, dangling.size(), "one orphan per family");

        for (ExpectedOmsOrder orphan : dangling) {
            assertEquals(MultiOmsTopology.OMS_C.name(), orphan.oms());
            assertEquals(ExpectedOmsOrder.BREAK_DANGLING, orphan.breakKind(), orphan.globalKey());
            assertTrue(orphan.extOrdID().startsWith("MISSING-"), orphan.extOrdID());
            assertEquals("NEW", orphan.ordStatus(), "the orphan is acked but never fills");
            assertEquals(0.0, orphan.cumQty());
            assertTrue(orphan.leavesQty() > 0);
            assertEquals(orphan.globalKey(), orphan.rootGlobalKey());
        }

        Set<String> danglingKeys = dangling.stream().map(ExpectedOmsOrder::globalKey)
                .collect(Collectors.toSet());
        for (ExpectedOmsOrder order : batch.expectedOrders()) {
            if (danglingKeys.contains(order.globalKey())) {
                continue;
            }
            assertEquals(ExpectedOmsOrder.BREAK_NONE, order.breakKind(),
                    order.globalKey() + ": the rest of the family is a clean fill");
            assertEquals("FILLED", order.ordStatus(), order.globalKey());
        }

        // The referenced id must appear nowhere else on any tape.
        Set<String> everyId = batch.messages().stream()
                .map(m -> MultiOmsTestFix.parse(m).get(FixTags.CL_ORD_ID))
                .collect(Collectors.toSet());
        dangling.forEach(orphan -> assertFalse(everyId.contains(orphan.extOrdID()),
                orphan.extOrdID() + " must never be defined by any tape"));
    }

    @Test
    @DisplayName("late_parent: out of order on the wire, but the settled state is clean and LINKED")
    void lateParent() {
        MultiOmsScenarioEngine.GeneratedBatch batch =
                MultiOmsTestFix.batch(MultiOmsScenarioCatalog.LATE_PARENT, 21L, CHILDREN, FAMILIES);
        for (ExpectedOmsOrder order : batch.expectedOrders()) {
            assertEquals(ExpectedOmsOrder.BREAK_NONE, order.breakKind(), order.globalKey());
            assertEquals("FILLED", order.ordStatus(), order.globalKey());
            assertEquals(order.oms().equals(MultiOmsTopology.OMS_A.name())
                            ? ExpectedOmsOrder.LINK_ROOT : ExpectedOmsOrder.LINK_LINKED,
                    order.linkState(), order.globalKey());
        }
    }

    @Test
    @DisplayName("a mixed run carries every break kind the taxonomy defines except NOTIONAL_BREAK")
    void mixedRunCoversTheTaxonomy() {
        MultiOmsScenarioEngine.GeneratedBatch batch = MultiOmsTestFix.batch(42L, CHILDREN, 30, "all");
        Set<String> kinds = batch.expectedOrders().stream()
                .map(ExpectedOmsOrder::breakKind)
                .collect(Collectors.toSet());
        assertEquals(Set.of(ExpectedOmsOrder.BREAK_NONE, ExpectedOmsOrder.BREAK_UNROUTED,
                ExpectedOmsOrder.BREAK_QTY, ExpectedOmsOrder.BREAK_DANGLING), kinds,
                "fills always execute at the limit price, so notional never breaks on its own");
        assertNotNull(GeneratorMain.toOmsJson(batch.expectedOrders()));
    }
}
