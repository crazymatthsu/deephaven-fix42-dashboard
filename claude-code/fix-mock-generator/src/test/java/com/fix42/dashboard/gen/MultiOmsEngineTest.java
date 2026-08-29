package com.fix42.dashboard.gen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Wire-level invariants of the multi-OMS tapes: framing and session headers per hub, the link-tag
 * wiring that ties the four hubs together, and the doc 01 §5 lifecycle each tape must fold cleanly.
 */
class MultiOmsEngineTest {

    private static final int FAMILIES = 24;
    private static final int CHILDREN = 3;

    private static MultiOmsScenarioEngine.GeneratedBatch defaultBatch() {
        return MultiOmsTestFix.batch(42L, CHILDREN, FAMILIES, MultiOmsScenarioCatalog.ALL);
    }

    // ---------------------------------------------------------------- framing and routing

    @Test
    @DisplayName("every emitted message round-trips the serializer with valid framing")
    void framingIsValidEverywhere() {
        MultiOmsScenarioEngine.GeneratedBatch batch = defaultBatch();
        assertTrue(batch.messages().size() > FAMILIES * 8);
        for (MultiOmsScenarioEngine.EmittedMessage emitted : batch.messages()) {
            String raw = FixSerializer.serialize(emitted.message());
            assertTrue(TestFix.framingValid(raw), () -> "bad framing: " + FixSerializer.renderPipe(raw));
            assertTrue(raw.startsWith("8=FIX.4.2" + FixTags.SOH), raw);
        }
    }

    @Test
    @DisplayName("each message carries its hub's name in 49, DROPCOPY in 56, and routes to that hub's topic")
    void tapeFramingAndRouting() {
        for (MultiOmsScenarioEngine.EmittedMessage emitted : defaultBatch().messages()) {
            MultiOmsTopology.Hub hub = MultiOmsTestFix.hub(emitted.oms());
            Map<Integer, String> m = MultiOmsTestFix.parse(emitted);
            assertEquals(hub.topic(), emitted.topic(), "message must route to its hub's topic");
            assertEquals(hub.name(), m.get(FixTags.SENDER_COMP_ID));
            assertEquals(FixTags.TARGET_DROP_COPY, m.get(FixTags.TARGET_COMP_ID));
            assertEquals(emitted.chainKey(), m.get(FixTags.CL_ORD_ID),
                    "the Kafka key is the hub order's D ClOrdID, echoed by every message");
        }
    }

    @Test
    @DisplayName("only the four configured topics are used, and every one of them is")
    void topicsMatchTheTopology() {
        Set<String> topics = new HashSet<>();
        defaultBatch().messages().forEach(m -> topics.add(m.topic()));
        assertEquals(new HashSet<>(MultiOmsTopology.topics()), topics);
    }

    @Test
    @DisplayName("MsgSeqNum increments by one per hub session, in wire order")
    void sequenceNumbersArePerHub() {
        Map<String, Long> next = new HashMap<>();
        for (MultiOmsScenarioEngine.EmittedMessage emitted : defaultBatch().messages()) {
            long expected = next.merge(emitted.oms(), 1L, Long::sum);
            assertEquals(expected, MultiOmsTestFix.num(MultiOmsTestFix.parse(emitted), FixTags.MSG_SEQ_NUM),
                    "hub " + emitted.oms());
        }
        assertEquals(MultiOmsTopology.HUBS.size(), next.size(), "every hub should have a tape");
    }

    @Test
    @DisplayName("SendingTime/TransactTime are monotone in wire order and equal on every message")
    void timestampsAreMonotone() {
        String previous = "";
        for (Map<Integer, String> m : MultiOmsTestFix.parsed(defaultBatch())) {
            String sendingTime = m.get(FixTags.SENDING_TIME);
            assertNotNull(sendingTime);
            assertEquals(sendingTime, m.get(FixTags.TRANSACT_TIME));
            assertTrue(sendingTime.compareTo(previous) >= 0, "timestamps went backwards at " + sendingTime);
            previous = sendingTime;
        }
    }

    @Test
    @DisplayName("ExecIDs are unique within a hub and namespaced by a hub prefix")
    void execIdsAreUniquePerHub() {
        Map<String, Set<String>> seen = new HashMap<>();
        Map<String, String> prefixes = Map.of(
                MultiOmsTopology.OMS_A.name(), "EA-",
                MultiOmsTopology.OMS_B_PARENT.name(), "EBP-",
                MultiOmsTopology.OMS_B_CHILD.name(), "EBC-",
                MultiOmsTopology.OMS_C.name(), "EC-");
        int execs = 0;
        for (MultiOmsScenarioEngine.EmittedMessage emitted : defaultBatch().messages()) {
            Map<Integer, String> m = MultiOmsTestFix.parse(emitted);
            if (!MultiOmsTestFix.isExec(m)) {
                continue;
            }
            execs++;
            String execId = m.get(FixTags.EXEC_ID);
            assertNotNull(execId);
            assertTrue(execId.startsWith(prefixes.get(emitted.oms())), execId + " on " + emitted.oms());
            assertTrue(seen.computeIfAbsent(emitted.oms(), k -> new HashSet<>()).add(execId),
                    "duplicate ExecID " + execId);
        }
        assertTrue(execs > FAMILIES * 4);
    }

    // ---------------------------------------------------------------- linking

    @Test
    @DisplayName("no D carries 37 OrderID, so OrderKey is the D's ClOrdID")
    void noOrderIdOnAnyNewOrderSingle() {
        int news = 0;
        for (Map<Integer, String> m : MultiOmsTestFix.parsed(defaultBatch())) {
            if (MultiOmsTestFix.isD(m)) {
                news++;
                assertFalse(m.containsKey(FixTags.ORDER_ID),
                        "a D with 37 would make OrderKey the OrderID and break GlobalKey");
            } else {
                assertNotNull(m.get(FixTags.ORDER_ID), "every 8 carries the hub's 37");
            }
        }
        assertTrue(news >= FAMILIES * 4);
    }

    @Test
    @DisplayName("each downstream D carries its hub's link tag holding the upstream order's ClOrdID")
    void linkTagsWireEveryEdge() {
        MultiOmsScenarioEngine.GeneratedBatch batch = defaultBatch();
        Map<String, String> clOrdIdByGlobalKey = new LinkedHashMap<>();
        for (MultiOmsScenarioEngine.EmittedMessage emitted : batch.messages()) {
            clOrdIdByGlobalKey.put(
                    MultiOmsTopology.globalKey(emitted.oms(), emitted.chainKey()), emitted.chainKey());
        }

        int edges = 0;
        int dangling = 0;
        for (MultiOmsScenarioEngine.EmittedMessage emitted : batch.messages()) {
            Map<Integer, String> m = MultiOmsTestFix.parse(emitted);
            if (!MultiOmsTestFix.isD(m)) {
                continue;
            }
            MultiOmsTopology.Hub hub = MultiOmsTestFix.hub(emitted.oms());
            if (hub.isRoot()) {
                assertFalse(m.containsKey(FixTags.EXT_ORDER_ID_A_TO_B), "a root D carries no link tag");
                continue;
            }
            String value = m.get(hub.linkTag());
            assertNotNull(value, "missing tag " + hub.linkTag() + " on a " + hub.name() + " D");
            String parentKey = MultiOmsTopology.globalKey(hub.upstream(), value);
            if (clOrdIdByGlobalKey.containsKey(parentKey)) {
                edges++;
            } else {
                dangling++;
                assertTrue(value.startsWith("MISSING-"), "unresolved link must be the scripted orphan: " + value);
            }
        }
        assertTrue(edges > FAMILIES * 2, "expected a linked edge per non-root order");
        assertTrue(dangling > 0, "the mix should contain dangling_child families");
    }

    @Test
    @DisplayName("the link tags are exactly the doc 09 §3 numbers, one per non-root hub")
    void linkTagsAreTheConfiguredNumbers() {
        assertEquals(0, MultiOmsTopology.OMS_A.linkTag());
        assertEquals(FixTags.EXT_ORDER_ID_A_TO_B, MultiOmsTopology.OMS_B_PARENT.linkTag());
        assertEquals(FixTags.EXT_ORDER_ID_B_PARENT_TO_CHILD, MultiOmsTopology.OMS_B_CHILD.linkTag());
        assertEquals(FixTags.EXT_ORDER_ID_C_TO_B_CHILD, MultiOmsTopology.OMS_C.linkTag());
        assertEquals(16666, FixTags.EXT_ORDER_ID_A_TO_B);
        assertEquals(16667, FixTags.EXT_ORDER_ID_B_PARENT_TO_CHILD);
        assertEquals(16668, FixTags.EXT_ORDER_ID_C_TO_B_CHILD);
        assertEquals(List.of("fix42.oms-a", "fix42.oms-b-parent", "fix42.oms-b-child", "fix42.oms-c"),
                MultiOmsTopology.topics());
    }

    @Test
    @DisplayName("an upstream D and its ack precede the downstream D that references it")
    void downstreamOrdersOpenAfterTheirParent() {
        MultiOmsScenarioEngine.GeneratedBatch batch = defaultBatch();
        for (MultiOmsScenarioEngine.MultiOmsChain chain : batch.chains()) {
            if (chain.scenario() == MultiOmsScenarioCatalog.LATE_PARENT) {
                continue;   // this scenario exists precisely to break the rule
            }
            Map<String, Integer> ackIndex = new LinkedHashMap<>();
            List<MultiOmsScenarioEngine.EmittedMessage> messages = chain.messages();
            for (int i = 0; i < messages.size(); i++) {
                Map<Integer, String> m = MultiOmsTestFix.parse(messages.get(i));
                if (MultiOmsTestFix.isExec(m) && FixTags.EXEC_TYPE_NEW.equals(m.get(FixTags.EXEC_TYPE))) {
                    ackIndex.putIfAbsent(
                            MultiOmsTopology.globalKey(messages.get(i).oms(), messages.get(i).chainKey()), i);
                }
            }
            for (int i = 0; i < messages.size(); i++) {
                MultiOmsScenarioEngine.EmittedMessage emitted = messages.get(i);
                Map<Integer, String> m = MultiOmsTestFix.parse(emitted);
                MultiOmsTopology.Hub hub = MultiOmsTestFix.hub(emitted.oms());
                if (!MultiOmsTestFix.isD(m) || hub.isRoot()) {
                    continue;
                }
                Integer parentAck = ackIndex.get(
                        MultiOmsTopology.globalKey(hub.upstream(), m.get(hub.linkTag())));
                if (parentAck != null) {
                    assertTrue(parentAck < i,
                            "downstream D at " + i + " referenced a parent acked at " + parentAck);
                }
            }
        }
    }

    // ---------------------------------------------------------------- per-tape lifecycle

    @ParameterizedTest(name = "{0}")
    @EnumSource(MultiOmsScenarioCatalog.class)
    @DisplayName("each tape is a valid doc 01 lifecycle: D, ack, then fills with absolute snapshots")
    void perTapeLifecycleIsValid(MultiOmsScenarioCatalog scenario) {
        MultiOmsScenarioEngine.GeneratedBatch batch = MultiOmsTestFix.batch(scenario, 7L, CHILDREN, 6);
        Map<String, List<Map<Integer, String>>> tapes = MultiOmsTestFix.byOrder(batch);
        assertFalse(tapes.isEmpty());

        for (Map.Entry<String, List<Map<Integer, String>>> entry : tapes.entrySet()) {
            String key = entry.getKey();
            List<Map<Integer, String>> tape = entry.getValue();

            assertTrue(MultiOmsTestFix.isD(tape.get(0)), key + ": a tape must open with its D");
            assertTrue(MultiOmsTestFix.isExec(tape.get(1)) && FixTags.EXEC_TYPE_NEW.equals(
                            tape.get(1).get(FixTags.EXEC_TYPE)),
                    key + ": the ack must precede any fill");

            double orderQty = MultiOmsTestFix.num(tape.get(0), FixTags.ORDER_QTY);
            double cum = 0;
            for (int i = 1; i < tape.size(); i++) {
                Map<Integer, String> m = tape.get(i);
                assertTrue(MultiOmsTestFix.isExec(m), key + ": only Ds and 8s belong on these tapes");
                assertEquals(FixTags.EXEC_TRANS_NEW, m.get(FixTags.EXEC_TRANS_TYPE));
                assertEquals(orderQty, MultiOmsTestFix.num(m, FixTags.ORDER_QTY),
                        key + ": 38 is stable, nothing amends in this mode");

                double reported = MultiOmsTestFix.num(m, FixTags.CUM_QTY);
                assertTrue(reported >= cum, key + ": 14 CumQty went backwards");
                cum = reported;
                assertEquals(orderQty - cum, MultiOmsTestFix.num(m, FixTags.LEAVES_QTY), 1e-9,
                        key + ": 151 must equal 38 - 14");
                assertEquals(FixTags.EXEC_TYPE_FILL.equals(m.get(FixTags.EXEC_TYPE)), cum == orderQty
                                && MultiOmsTestFix.isFill(m),
                        key + ": 150=2 exactly when the fill completes the order");
                assertEquals(m.get(FixTags.EXEC_TYPE), m.get(FixTags.ORD_STATUS),
                        key + ": 39 tracks 150 for the ack/partial/full triple");
                if (MultiOmsTestFix.isFill(m)) {
                    assertTrue(MultiOmsTestFix.num(m, FixTags.LAST_SHARES) > 0);
                    assertNotNull(m.get(FixTags.LAST_MKT));
                    assertEquals(MultiOmsTestFix.num(m, FixTags.PRICE),
                            MultiOmsTestFix.num(m, FixTags.LAST_PX), 1e-9,
                            key + ": fills execute at the family's limit price");
                    assertEquals(MultiOmsTestFix.num(m, FixTags.PRICE),
                            MultiOmsTestFix.num(m, FixTags.AVG_PX), 1e-9,
                            key + ": so AvgPx is the limit price at every level");
                } else {
                    assertFalse(m.containsKey(FixTags.LAST_SHARES), key + ": a non-fill 8 carries no 32");
                }
            }
        }
    }

    @Test
    @DisplayName("every D is a FIX 4.2-complete limit order: 21, 38, 40=2, 44, 59")
    void requiredOrderTagsPresent() {
        for (Map<Integer, String> m : MultiOmsTestFix.parsed(defaultBatch())) {
            if (!MultiOmsTestFix.isD(m)) {
                continue;
            }
            assertEquals("1", m.get(FixTags.HANDL_INST), "21 HandlInst required on D in FIX 4.2");
            assertEquals(FixTags.ORD_TYPE_LIMIT, m.get(FixTags.ORD_TYPE));
            assertNotNull(m.get(FixTags.PRICE), "44 Price is required when 40=2");
            assertNotNull(m.get(FixTags.TIME_IN_FORCE));
            assertNotNull(m.get(FixTags.ACCOUNT));
            assertTrue(List.of("1", "2", "5").contains(m.get(FixTags.SIDE)));
            assertTrue(MultiOmsTestFix.num(m, FixTags.ORDER_QTY) > 0);
        }
    }

    @Test
    @DisplayName("a family shares one account, symbol, side and limit price across all four hubs")
    void familyTermsAgreeAcrossHubs() {
        for (MultiOmsScenarioEngine.MultiOmsChain chain : defaultBatch().chains()) {
            Set<String> terms = new HashSet<>();
            for (MultiOmsScenarioEngine.EmittedMessage emitted : chain.messages()) {
                Map<Integer, String> m = MultiOmsTestFix.parse(emitted);
                terms.add(m.get(FixTags.ACCOUNT) + "|" + m.get(FixTags.SYMBOL) + "|"
                        + m.get(FixTags.SIDE) + "|" + m.get(FixTags.PRICE));
            }
            assertEquals(1, terms.size(), chain.rootGlobalKey() + ": " + terms);
        }
    }

    // ---------------------------------------------------------------- quantity conservation

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = MultiOmsScenarioCatalog.class,
            names = {"CLEAN_FILL", "WORKING_FANOUT", "LATE_PARENT"})
    @DisplayName("clean scenarios conserve quantity at every edge: parent cum == sum of child cums")
    void quantityIsConservedPerEdge(MultiOmsScenarioCatalog scenario) {
        MultiOmsTestFix.Recon recon = MultiOmsTestFix.recon(MultiOmsTestFix.batch(scenario, 11L, CHILDREN, 8));

        int edges = 0;
        for (MultiOmsTestFix.ReconRow row : recon.rows().values()) {
            List<MultiOmsTestFix.ReconRow> children = recon.childrenOf(row.globalKey());
            if (children.isEmpty()) {
                continue;
            }
            edges++;
            assertEquals(row.cumQty(),
                    children.stream().mapToDouble(MultiOmsTestFix.ReconRow::cumQty).sum(), 1e-9,
                    row.globalKey() + ": CumQty must equal the sum over direct children");
            assertEquals(row.leavesQty(),
                    children.stream().mapToDouble(MultiOmsTestFix.ReconRow::leavesQty).sum(), 1e-9,
                    row.globalKey() + ": LeavesQty must equal the sum over direct children");
        }
        assertTrue(edges >= 8 * 3, "every family contributes A, B-parent and per-child edges");
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(value = MultiOmsScenarioCatalog.class,
            names = {"CLEAN_FILL", "WORKING_FANOUT", "MISSED_FILL", "DANGLING_CHILD", "LATE_PARENT"})
    @DisplayName("a full route splits the parent's quantity exactly across its children")
    void childOrderQtysSumToTheRoutedQty(MultiOmsScenarioCatalog scenario) {
        MultiOmsTestFix.Recon recon = MultiOmsTestFix.recon(MultiOmsTestFix.batch(scenario, 5L, CHILDREN, 8));

        for (MultiOmsTestFix.ReconRow parent : recon.ofOms(MultiOmsTopology.OMS_B_PARENT.name())) {
            double routed = recon.childrenOf(parent.globalKey()).stream()
                    .mapToDouble(MultiOmsTestFix.ReconRow::orderQty)
                    .sum();
            assertEquals(parent.orderQty(), routed, 1e-9,
                    parent.globalKey() + ": children must cover the whole order");
        }
    }

    @Test
    @DisplayName("partial_route routes strictly less than the parent's quantity")
    void partialRouteLeavesAnUnroutedRemainder() {
        MultiOmsTestFix.Recon recon = MultiOmsTestFix.recon(
                MultiOmsTestFix.batch(MultiOmsScenarioCatalog.PARTIAL_ROUTE, 5L, CHILDREN, 8));

        List<MultiOmsTestFix.ReconRow> parents = recon.ofOms(MultiOmsTopology.OMS_B_PARENT.name());
        assertEquals(8, parents.size());
        for (MultiOmsTestFix.ReconRow parent : parents) {
            double routed = recon.childrenOf(parent.globalKey()).stream()
                    .mapToDouble(MultiOmsTestFix.ReconRow::orderQty)
                    .sum();
            assertTrue(routed < parent.orderQty(),
                    parent.globalKey() + ": " + routed + " should be < " + parent.orderQty());
            assertTrue(routed >= 0.35 * parent.orderQty() && routed <= 0.75 * parent.orderQty(),
                    parent.globalKey() + ": routed " + routed + " of " + parent.orderQty());
        }
    }

    // ---------------------------------------------------------------- ordering and determinism

    @Test
    @DisplayName("late_parent emits the whole OMS-B-parent tape after every child and OMS-C message")
    void lateParentDefersTheParentTape() {
        MultiOmsScenarioEngine.GeneratedBatch batch =
                MultiOmsTestFix.batch(MultiOmsScenarioCatalog.LATE_PARENT, 3L, CHILDREN, 6);
        for (MultiOmsScenarioEngine.MultiOmsChain chain : batch.chains()) {
            List<MultiOmsScenarioEngine.EmittedMessage> messages = chain.messages();
            int firstParent = Integer.MAX_VALUE;
            int lastDownstream = -1;
            for (int i = 0; i < messages.size(); i++) {
                String oms = messages.get(i).oms();
                if (oms.equals(MultiOmsTopology.OMS_B_PARENT.name())) {
                    firstParent = Math.min(firstParent, i);
                } else if (oms.equals(MultiOmsTopology.OMS_B_CHILD.name())
                        || oms.equals(MultiOmsTopology.OMS_C.name())) {
                    lastDownstream = Math.max(lastDownstream, i);
                }
            }
            assertTrue(firstParent > lastDownstream,
                    chain.rootGlobalKey() + ": first B-parent message at " + firstParent
                            + " must follow the last downstream message at " + lastDownstream);
        }
    }

    @Test
    @DisplayName("interleaving preserves per-family and therefore per-tape order")
    void interleavingPreservesPerFamilyOrder() {
        MultiOmsScenarioEngine.GeneratedBatch batch = defaultBatch();
        Map<String, List<FixMessage>> streamed = new LinkedHashMap<>();
        for (MultiOmsScenarioEngine.EmittedMessage emitted : batch.messages()) {
            streamed.computeIfAbsent(emitted.chainKey(), k -> new ArrayList<>()).add(emitted.message());
        }
        for (MultiOmsScenarioEngine.MultiOmsChain chain : batch.chains()) {
            Map<String, List<FixMessage>> scripted = new LinkedHashMap<>();
            chain.messages().forEach(m ->
                    scripted.computeIfAbsent(m.chainKey(), k -> new ArrayList<>()).add(m.message()));
            scripted.forEach((key, expected) ->
                    assertEquals(expected, streamed.get(key), key + ": per-order wire order must be preserved"));
        }

        long distinctInFirstWindow = batch.messages().stream()
                .limit(MultiOmsScenarioEngine.MAX_LIVE_CHAINS)
                .map(m -> m.chainKey().substring(m.chainKey().indexOf('-') + 1, m.chainKey().indexOf('-') + 5))
                .distinct()
                .count();
        assertEquals(MultiOmsScenarioEngine.MAX_LIVE_CHAINS, distinctInFirstWindow,
                "families should progress concurrently, not one after another");
    }

    @Test
    @DisplayName("the same seed replays byte-identical messages, topics and keys")
    void deterministicUnderSeed() {
        List<String> first = MultiOmsTestFix.rendered(MultiOmsTestFix.batch(42L, 2, 10, "all"));
        List<String> second = MultiOmsTestFix.rendered(MultiOmsTestFix.batch(42L, 2, 10, "all"));
        assertEquals(first, second);

        assertFalse(first.equals(MultiOmsTestFix.rendered(MultiOmsTestFix.batch(43L, 2, 10, "all"))),
                "a different seed must produce a different stream");
        assertFalse(first.equals(MultiOmsTestFix.rendered(MultiOmsTestFix.batch(42L, 3, 10, "all"))),
                "a different fan-out must produce a different stream");
    }

    @Test
    @DisplayName("the expected export is deterministic too")
    void expectedExportIsDeterministic() {
        assertEquals(MultiOmsTestFix.batch(42L, 3, 10, "all").expectedOrders(),
                MultiOmsTestFix.batch(42L, 3, 10, "all").expectedOrders());
    }

    // ---------------------------------------------------------------- fan-out and arguments

    @ParameterizedTest(name = "--children {0}")
    @ValueSource(ints = {1, 2, 3, 5})
    @DisplayName("fan-out honours --children: 1..N children per parent, never more")
    void fanOutRespectsMaxChildren(int maxChildren) {
        MultiOmsTestFix.Recon recon = MultiOmsTestFix.recon(MultiOmsTestFix.batch(13L, maxChildren, 12, "all"));
        Set<Integer> observed = new HashSet<>();
        for (MultiOmsTestFix.ReconRow parent : recon.ofOms(MultiOmsTopology.OMS_B_PARENT.name())) {
            int children = recon.childrenOf(parent.globalKey()).size();
            assertTrue(children >= 1 && children <= maxChildren,
                    parent.globalKey() + " has " + children + " children, max " + maxChildren);
            observed.add(children);
        }
        assertFalse(observed.isEmpty());
        if (maxChildren >= 2) {
            assertTrue(observed.size() > 1, "the fan-out should vary across families: " + observed);
        }
    }

    @Test
    @DisplayName("every OMS-B-child has exactly one OMS-C order, of the same quantity")
    void eachChildHasOneVenueOrder() {
        MultiOmsTestFix.Recon recon = MultiOmsTestFix.recon(defaultBatch());
        List<MultiOmsTestFix.ReconRow> children = recon.ofOms(MultiOmsTopology.OMS_B_CHILD.name());
        assertTrue(children.size() >= FAMILIES);
        for (MultiOmsTestFix.ReconRow child : children) {
            List<MultiOmsTestFix.ReconRow> venue = recon.childrenOf(child.globalKey());
            assertEquals(1, venue.size(), child.globalKey() + ": one OMS-C order per child");
            assertEquals(child.orderQty(), venue.get(0).orderQty(), 1e-9,
                    child.globalKey() + ": the OMS-C order carries the child's quantity");
        }
    }

    @Test
    @DisplayName("the engine rejects a fan-out below one and a non-positive order count")
    void invalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> new MultiOmsScenarioEngine(1L, 0));
        assertThrows(IllegalArgumentException.class,
                () -> MultiOmsTestFix.engine(1L, 3).generate(0, "all"));
        assertThrows(IllegalArgumentException.class,
                () -> MultiOmsTestFix.engine(1L, 3).generate(1, "nope"));
    }

    @Test
    @DisplayName("--scenario all covers every catalog entry before weighting the remainder")
    void allCoversTheCatalog() {
        MultiOmsScenarioEngine.GeneratedBatch batch =
                MultiOmsTestFix.batch(2L, 3, MultiOmsScenarioCatalog.values().length, "all");
        assertEquals(Set.of(MultiOmsScenarioCatalog.values()),
                new HashSet<>(batch.chains().stream()
                        .map(MultiOmsScenarioEngine.MultiOmsChain::scenario).toList()));
    }
}
