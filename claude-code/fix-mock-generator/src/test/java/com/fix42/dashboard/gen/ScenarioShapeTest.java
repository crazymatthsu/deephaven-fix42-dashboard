package com.fix42.dashboard.gen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Per-scenario message skeletons: the {@code 35=}/{@code 150=}/{@code 39=}/{@code 434=}/{@code 20=}
 * sequences frozen by doc 05 §2.2. Each case runs across several seeds so optional steps and the
 * randomized order terms cannot make the assertions accidental.
 */
class ScenarioShapeTest {

    private static final long[] SEEDS = {1L, 2L, 3L, 7L, 42L};

    private static List<Map<Integer, String>> chain(ScenarioCatalog scenario, long seed) {
        return TestFix.singleChain(scenario, seed);
    }

    private static List<Map<Integer, String>> execs(List<Map<Integer, String>> chain) {
        return chain.stream().filter(m -> FixTags.MSG_EXECUTION_REPORT.equals(m.get(FixTags.MSG_TYPE))).toList();
    }

    private static List<String> execTypes(List<Map<Integer, String>> chain) {
        return execs(chain).stream().map(m -> m.get(FixTags.EXEC_TYPE)).toList();
    }

    private static Map<Integer, String> last(List<Map<Integer, String>> chain) {
        return chain.get(chain.size() - 1);
    }

    private static Map<Integer, String> only(List<Map<Integer, String>> chain, String msgType) {
        List<Map<Integer, String>> hits =
                chain.stream().filter(m -> msgType.equals(m.get(FixTags.MSG_TYPE))).toList();
        assertEquals(1, hits.size(), "expected exactly one 35=" + msgType);
        return hits.get(0);
    }

    private static Map<Integer, String> execWithType(List<Map<Integer, String>> chain, String execType) {
        List<Map<Integer, String>> hits =
                execs(chain).stream().filter(m -> execType.equals(m.get(FixTags.EXEC_TYPE))).toList();
        assertEquals(1, hits.size(), "expected exactly one 8 with 150=" + execType);
        return hits.get(0);
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 7L, 42L})
    @DisplayName("new_ack_fill_full: D -> 8(A) -> 8(0) -> 8(1)xk -> 8(2)")
    void newAckFillFull(long seed) {
        var chain = chain(ScenarioCatalog.NEW_ACK_FILL_FULL, seed);
        assertEquals(FixTags.MSG_NEW_ORDER_SINGLE, chain.get(0).get(FixTags.MSG_TYPE));
        assertTrue(chain.stream().skip(1)
                .allMatch(m -> FixTags.MSG_EXECUTION_REPORT.equals(m.get(FixTags.MSG_TYPE))));

        List<String> types = execTypes(chain);
        assertEquals(FixTags.EXEC_TYPE_PENDING_NEW, types.get(0));
        assertEquals(FixTags.EXEC_TYPE_NEW, types.get(1));
        assertEquals(FixTags.EXEC_TYPE_FILL, types.get(types.size() - 1));
        List<String> partials = types.subList(2, types.size() - 1);
        assertFalse(partials.isEmpty(), "expected at least one partial fill");
        assertTrue(partials.stream().allMatch(FixTags.EXEC_TYPE_PARTIAL_FILL::equals));

        Map<Integer, String> terminal = last(chain);
        assertEquals(FixTags.ORD_STATUS_FILLED, terminal.get(FixTags.ORD_STATUS));
        assertEquals("0", terminal.get(FixTags.LEAVES_QTY));
        assertEquals(terminal.get(FixTags.ORDER_QTY), terminal.get(FixTags.CUM_QTY));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 7L, 42L})
    @DisplayName("new_reject: D -> 8(150=8, 39=8, 103 set)")
    void newReject(long seed) {
        var chain = chain(ScenarioCatalog.NEW_REJECT, seed);
        assertEquals(List.of(FixTags.MSG_NEW_ORDER_SINGLE, FixTags.MSG_EXECUTION_REPORT),
                TestFix.msgTypes(chain));

        Map<Integer, String> reject = chain.get(1);
        assertEquals(FixTags.EXEC_TYPE_REJECTED, reject.get(FixTags.EXEC_TYPE));
        assertEquals(FixTags.ORD_STATUS_REJECTED, reject.get(FixTags.ORD_STATUS));
        assertNotNull(reject.get(FixTags.ORD_REJ_REASON), "103 OrdRejReason required on a reject");
        assertNotNull(reject.get(FixTags.TEXT));
        assertEquals("0", reject.get(FixTags.CUM_QTY));
        assertEquals("0", reject.get(FixTags.LEAVES_QTY));
        assertEquals(chain.get(0).get(FixTags.CL_ORD_ID), reject.get(FixTags.CL_ORD_ID));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 7L, 42L})
    @DisplayName("amend_ack: G -> 8(E) -> 8(5) applies staged terms and rotates ClOrdID")
    void amendAck(long seed) {
        var chain = chain(ScenarioCatalog.AMEND_ACK, seed);
        Map<Integer, String> nos = chain.get(0);
        Map<Integer, String> amend = only(chain, FixTags.MSG_CANCEL_REPLACE_REQUEST);
        Map<Integer, String> pendingReplace = execWithType(chain, FixTags.EXEC_TYPE_PENDING_REPLACE);
        Map<Integer, String> replaced = execWithType(chain, FixTags.EXEC_TYPE_REPLACED);

        assertEquals(nos.get(FixTags.CL_ORD_ID), amend.get(FixTags.ORIG_CL_ORD_ID));
        assertNotEquals(nos.get(FixTags.CL_ORD_ID), amend.get(FixTags.CL_ORD_ID));
        assertEquals("1", amend.get(FixTags.HANDL_INST), "21 HandlInst required on G");

        // The pending-replace confirm still shows the old terms; the replace confirm applies them.
        assertEquals(nos.get(FixTags.ORDER_QTY), pendingReplace.get(FixTags.ORDER_QTY));
        assertEquals(amend.get(FixTags.ORDER_QTY), replaced.get(FixTags.ORDER_QTY));
        assertEquals(amend.get(FixTags.PRICE), replaced.get(FixTags.PRICE));

        // Both confirms echo the request's 11 and the prior 11 as 41.
        for (Map<Integer, String> confirm : List.of(pendingReplace, replaced)) {
            assertEquals(amend.get(FixTags.CL_ORD_ID), confirm.get(FixTags.CL_ORD_ID));
            assertEquals(nos.get(FixTags.CL_ORD_ID), confirm.get(FixTags.ORIG_CL_ORD_ID));
        }

        // Fills after the amend carry the rotated ClOrdID and run to a full fill on the new terms.
        int replacedIdx = chain.indexOf(replaced);
        List<Map<Integer, String>> after = chain.subList(replacedIdx + 1, chain.size());
        assertFalse(after.isEmpty(), "amend_ack fills to filled after the replace confirm");
        assertTrue(after.stream()
                .allMatch(m -> amend.get(FixTags.CL_ORD_ID).equals(m.get(FixTags.CL_ORD_ID))));
        Map<Integer, String> terminal = last(chain);
        assertEquals(FixTags.EXEC_TYPE_FILL, terminal.get(FixTags.EXEC_TYPE));
        assertEquals(FixTags.ORD_STATUS_FILLED, terminal.get(FixTags.ORD_STATUS));
        assertEquals(amend.get(FixTags.ORDER_QTY), terminal.get(FixTags.CUM_QTY));
        assertEquals("0", terminal.get(FixTags.LEAVES_QTY));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 7L, 42L})
    @DisplayName("amend_reject: D -> 8(0) -> G -> 9(434=2, 102, 58)")
    void amendReject(long seed) {
        var chain = chain(ScenarioCatalog.AMEND_REJECT, seed);
        assertEquals(List.of(FixTags.MSG_NEW_ORDER_SINGLE, FixTags.MSG_EXECUTION_REPORT,
                        FixTags.MSG_CANCEL_REPLACE_REQUEST, FixTags.MSG_CANCEL_REJECT),
                TestFix.msgTypes(chain));

        Map<Integer, String> nos = chain.get(0);
        Map<Integer, String> amend = chain.get(2);
        Map<Integer, String> reject = chain.get(3);
        assertEquals(FixTags.CXL_REJ_RESPONSE_TO_REPLACE, reject.get(FixTags.CXL_REJ_RESPONSE_TO));
        assertNotNull(reject.get(FixTags.CXL_REJ_REASON));
        assertNotNull(reject.get(FixTags.TEXT));
        assertEquals(FixTags.ORD_STATUS_NEW, reject.get(FixTags.ORD_STATUS), "39 on the 9 reverts to NEW");
        assertEquals(amend.get(FixTags.CL_ORD_ID), reject.get(FixTags.CL_ORD_ID));
        assertEquals(nos.get(FixTags.CL_ORD_ID), reject.get(FixTags.ORIG_CL_ORD_ID));
        assertNotNull(reject.get(FixTags.ORDER_ID));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 7L, 42L})
    @DisplayName("cancel_ack: F -> 8(150=6) -> 8(150=4), terminal CANCELED with LeavesQty 0")
    void cancelAck(long seed) {
        var chain = chain(ScenarioCatalog.CANCEL_ACK, seed);
        Map<Integer, String> nos = chain.get(0);
        Map<Integer, String> cancel = only(chain, FixTags.MSG_CANCEL_REQUEST);
        Map<Integer, String> pendingCancel = execWithType(chain, FixTags.EXEC_TYPE_PENDING_CANCEL);
        Map<Integer, String> canceled = execWithType(chain, FixTags.EXEC_TYPE_CANCELED);

        assertEquals(nos.get(FixTags.CL_ORD_ID), cancel.get(FixTags.ORIG_CL_ORD_ID));
        assertNotEquals(nos.get(FixTags.CL_ORD_ID), cancel.get(FixTags.CL_ORD_ID));
        for (Map<Integer, String> confirm : List.of(pendingCancel, canceled)) {
            assertEquals(cancel.get(FixTags.CL_ORD_ID), confirm.get(FixTags.CL_ORD_ID));
            assertEquals(nos.get(FixTags.CL_ORD_ID), confirm.get(FixTags.ORIG_CL_ORD_ID));
        }
        assertSame(canceled, last(chain));
        assertEquals(FixTags.ORD_STATUS_PENDING_CANCEL, pendingCancel.get(FixTags.ORD_STATUS));
        assertEquals(FixTags.ORD_STATUS_CANCELED, canceled.get(FixTags.ORD_STATUS));
        assertEquals("0", canceled.get(FixTags.LEAVES_QTY));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 7L, 42L})
    @DisplayName("cancel_reject: D -> 8(0) -> 8(1) -> F -> 9(434=1, 39=1)")
    void cancelReject(long seed) {
        var chain = chain(ScenarioCatalog.CANCEL_REJECT, seed);
        assertEquals(List.of(FixTags.MSG_NEW_ORDER_SINGLE, FixTags.MSG_EXECUTION_REPORT,
                        FixTags.MSG_EXECUTION_REPORT, FixTags.MSG_CANCEL_REQUEST, FixTags.MSG_CANCEL_REJECT),
                TestFix.msgTypes(chain));
        assertEquals(List.of(FixTags.EXEC_TYPE_NEW, FixTags.EXEC_TYPE_PARTIAL_FILL), execTypes(chain));

        Map<Integer, String> reject = last(chain);
        assertEquals(FixTags.CXL_REJ_RESPONSE_TO_CANCEL, reject.get(FixTags.CXL_REJ_RESPONSE_TO));
        assertEquals(FixTags.ORD_STATUS_PARTIALLY_FILLED, reject.get(FixTags.ORD_STATUS));
        assertNotNull(reject.get(FixTags.CXL_REJ_REASON));
        assertEquals(chain.get(3).get(FixTags.CL_ORD_ID), reject.get(FixTags.CL_ORD_ID));
        assertEquals(chain.get(0).get(FixTags.CL_ORD_ID), reject.get(FixTags.ORIG_CL_ORD_ID));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 7L, 42L})
    @DisplayName("fill_bust: 8(20=1) references the fill's ExecID and restates a lower CumQty")
    void fillBust(long seed) {
        var chain = chain(ScenarioCatalog.FILL_BUST, seed);
        assertEquals(4, chain.size());
        Map<Integer, String> fill = chain.get(2);
        Map<Integer, String> bust = chain.get(3);

        assertEquals(FixTags.EXEC_TYPE_PARTIAL_FILL, fill.get(FixTags.EXEC_TYPE));
        assertEquals(FixTags.EXEC_TRANS_NEW, fill.get(FixTags.EXEC_TRANS_TYPE));
        assertEquals(FixTags.EXEC_TRANS_CANCEL, bust.get(FixTags.EXEC_TRANS_TYPE));
        assertEquals(fill.get(FixTags.EXEC_ID), bust.get(FixTags.EXEC_REF_ID));
        assertNotEquals(fill.get(FixTags.EXEC_ID), bust.get(FixTags.EXEC_ID));

        double before = Double.parseDouble(fill.get(FixTags.CUM_QTY));
        double after = Double.parseDouble(bust.get(FixTags.CUM_QTY));
        assertTrue(after < before, "bust must restate a lower CumQty");
        assertEquals(0.0, after);
        assertEquals(bust.get(FixTags.ORDER_QTY), bust.get(FixTags.LEAVES_QTY));
        assertEquals(FixTags.ORD_STATUS_NEW, bust.get(FixTags.ORD_STATUS));
        assertEquals(0.0, Double.parseDouble(bust.get(FixTags.AVG_PX)));
        assertEquals(fill.get(FixTags.LAST_SHARES), bust.get(FixTags.LAST_SHARES));
        assertEquals(fill.get(FixTags.LAST_PX), bust.get(FixTags.LAST_PX));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 7L, 42L})
    @DisplayName("fill_correct: 8(20=2) restates the price only; CumQty unchanged, AvgPx adopts 31")
    void fillCorrect(long seed) {
        var chain = chain(ScenarioCatalog.FILL_CORRECT, seed);
        assertEquals(4, chain.size());
        Map<Integer, String> fill = chain.get(2);
        Map<Integer, String> correct = chain.get(3);

        assertEquals(FixTags.EXEC_TRANS_CORRECT, correct.get(FixTags.EXEC_TRANS_TYPE));
        assertEquals(fill.get(FixTags.EXEC_ID), correct.get(FixTags.EXEC_REF_ID));
        assertEquals(fill.get(FixTags.CUM_QTY), correct.get(FixTags.CUM_QTY));
        assertEquals(fill.get(FixTags.LEAVES_QTY), correct.get(FixTags.LEAVES_QTY));
        assertEquals(fill.get(FixTags.LAST_SHARES), correct.get(FixTags.LAST_SHARES));
        assertNotEquals(fill.get(FixTags.LAST_PX), correct.get(FixTags.LAST_PX));
        assertEquals(Double.parseDouble(correct.get(FixTags.LAST_PX)),
                Double.parseDouble(correct.get(FixTags.AVG_PX)), 1e-6);
        assertEquals(FixTags.ORD_STATUS_PARTIALLY_FILLED, correct.get(FixTags.ORD_STATUS));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 7L, 42L})
    @DisplayName("dk_trade: 35=Q carries 127 and points at the fill's ExecID; no economic change")
    void dkTrade(long seed) {
        var chain = chain(ScenarioCatalog.DK_TRADE, seed);
        assertEquals(List.of(FixTags.MSG_NEW_ORDER_SINGLE, FixTags.MSG_EXECUTION_REPORT,
                        FixTags.MSG_EXECUTION_REPORT, FixTags.MSG_DONT_KNOW_TRADE),
                TestFix.msgTypes(chain));

        Map<Integer, String> fill = chain.get(2);
        Map<Integer, String> dk = chain.get(3);
        assertEquals(fill.get(FixTags.ORDER_ID), dk.get(FixTags.ORDER_ID));
        assertEquals(fill.get(FixTags.EXEC_ID), dk.get(FixTags.EXEC_ID));
        assertNotNull(dk.get(FixTags.DK_REASON));
        assertTrue("ABCDEFZ".contains(dk.get(FixTags.DK_REASON)));
        assertEquals(fill.get(FixTags.LAST_SHARES), dk.get(FixTags.LAST_SHARES));
        assertEquals(fill.get(FixTags.LAST_PX), dk.get(FixTags.LAST_PX));
        assertFalse(dk.containsKey(FixTags.ORD_STATUS), "a DK carries no OrdStatus — no economic change");
        assertEquals(FixTags.SENDER_CLIENT, dk.get(FixTags.SENDER_COMP_ID));
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1L, 2L, 3L, 7L, 42L})
    @DisplayName("partial_then_cancel: D -> 8(0) -> 8(1) -> F -> 8(6) -> 8(4)")
    void partialThenCancel(long seed) {
        var chain = chain(ScenarioCatalog.PARTIAL_THEN_CANCEL, seed);
        assertEquals(List.of(FixTags.MSG_NEW_ORDER_SINGLE, FixTags.MSG_EXECUTION_REPORT,
                        FixTags.MSG_EXECUTION_REPORT, FixTags.MSG_CANCEL_REQUEST,
                        FixTags.MSG_EXECUTION_REPORT, FixTags.MSG_EXECUTION_REPORT),
                TestFix.msgTypes(chain));
        assertEquals(List.of(FixTags.EXEC_TYPE_NEW, FixTags.EXEC_TYPE_PARTIAL_FILL,
                        FixTags.EXEC_TYPE_PENDING_CANCEL, FixTags.EXEC_TYPE_CANCELED),
                execTypes(chain));

        Map<Integer, String> canceled = last(chain);
        assertEquals(FixTags.ORD_STATUS_CANCELED, canceled.get(FixTags.ORD_STATUS));
        assertEquals("0", canceled.get(FixTags.LEAVES_QTY));
        assertTrue(Double.parseDouble(canceled.get(FixTags.CUM_QTY)) > 0, "cancel follows a partial fill");
    }

    @Test
    @DisplayName("--scenario all covers every catalog entry before drawing by weight")
    void allCoversTheCatalog() {
        ScenarioEngine engine = new ScenarioEngine(42L);
        var chains = engine.buildChains(ScenarioCatalog.values().length, ScenarioCatalog.ALL);
        assertEquals(List.of(ScenarioCatalog.values()), chains.stream().map(OrderScenario::scenario).toList());
    }

    @Test
    @DisplayName("--scenario <name> generates only that scenario; unknown names are rejected")
    void singleScenarioSelection() {
        ScenarioEngine engine = new ScenarioEngine(42L);
        var chains = engine.buildChains(4, "fill_bust");
        assertTrue(chains.stream().allMatch(c -> c.scenario() == ScenarioCatalog.FILL_BUST));
        assertThrows(IllegalArgumentException.class, () -> ScenarioCatalog.fromCliName("no_such_scenario"));
        assertEquals(ScenarioCatalog.DK_TRADE, ScenarioCatalog.fromCliName("DK_Trade"));
    }

    @Test
    @DisplayName("every catalog entry has a positive weight and a documented sequence")
    void catalogMetadata() {
        for (ScenarioCatalog scenario : ScenarioCatalog.values()) {
            assertTrue(scenario.weight() > 0, scenario.cliName());
            assertFalse(scenario.sequence().isBlank(), scenario.cliName());
            assertEquals(scenario, ScenarioCatalog.fromCliName(scenario.cliName()));
        }
        assertEquals(10, ScenarioCatalog.values().length);
        for (long seed : SEEDS) {
            assertEquals(10, new ScenarioEngine(seed).buildChains(10, ScenarioCatalog.ALL).size());
        }
    }
}
