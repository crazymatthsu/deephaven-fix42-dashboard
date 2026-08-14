package com.fix42.dashboard.gen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Cross-scenario invariants over a full generated batch: framing, session headers, identifier
 * uniqueness and chain linkage, and the venue-side absolute quantity arithmetic of doc 01 §5.
 */
class ScenarioInvariantsTest {

    private static final Instant BASE = Instant.parse("2025-08-14T12:00:00Z");
    private static final int CHAINS = 40;

    private static ScenarioEngine.GeneratedBatch batch(long seed) {
        return new ScenarioEngine(seed, BASE).generate(CHAINS, ScenarioCatalog.ALL);
    }

    private static ScenarioEngine.GeneratedBatch defaultBatch() {
        return batch(42L);
    }

    private static List<Map<Integer, String>> parsed(ScenarioEngine.GeneratedBatch batch) {
        return batch.messages().stream()
                .map(m -> TestFix.parse(FixSerializer.serialize(m.message())))
                .toList();
    }

    private static Map<String, List<Map<Integer, String>>> byChain(ScenarioEngine.GeneratedBatch batch) {
        Map<String, List<Map<Integer, String>>> chains = new LinkedHashMap<>();
        for (ScenarioEngine.EmittedMessage emitted : batch.messages()) {
            chains.computeIfAbsent(emitted.chainKey(), k -> new ArrayList<>())
                    .add(TestFix.parse(FixSerializer.serialize(emitted.message())));
        }
        return chains;
    }

    private static boolean isExec(Map<Integer, String> m) {
        return FixTags.MSG_EXECUTION_REPORT.equals(m.get(FixTags.MSG_TYPE));
    }

    private static boolean isFill(Map<Integer, String> m) {
        String execType = m.get(FixTags.EXEC_TYPE);
        String transType = m.getOrDefault(FixTags.EXEC_TRANS_TYPE, FixTags.EXEC_TRANS_NEW);
        return isExec(m)
                && FixTags.EXEC_TRANS_NEW.equals(transType)
                && (FixTags.EXEC_TYPE_PARTIAL_FILL.equals(execType) || FixTags.EXEC_TYPE_FILL.equals(execType));
    }

    private static double num(Map<Integer, String> m, int tag) {
        return Double.parseDouble(m.get(tag));
    }

    @Test
    @DisplayName("every generated message carries correct BodyLength and CheckSum")
    void framingIsValidEverywhere() {
        ScenarioEngine.GeneratedBatch batch = defaultBatch();
        assertTrue(batch.messages().size() > CHAINS);
        for (ScenarioEngine.EmittedMessage emitted : batch.messages()) {
            String raw = FixSerializer.serialize(emitted.message());
            assertTrue(TestFix.framingValid(raw), () -> "bad framing: " + FixSerializer.renderPipe(raw));
        }
    }

    @Test
    @DisplayName("session header tags are present on every message, TransactTime on every app message")
    void sessionHeaderTags() {
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            assertEquals(FixTags.BEGIN_STRING_FIX42, m.get(FixTags.BEGIN_STRING));
            for (int tag : List.of(FixTags.BODY_LENGTH, FixTags.MSG_TYPE, FixTags.SENDER_COMP_ID,
                    FixTags.TARGET_COMP_ID, FixTags.MSG_SEQ_NUM, FixTags.SENDING_TIME,
                    FixTags.TRANSACT_TIME, FixTags.CHECK_SUM)) {
                assertNotNull(m.get(tag), () -> "missing tag " + tag + " on 35=" + m.get(FixTags.MSG_TYPE));
            }
            assertEquals(m.get(FixTags.SENDING_TIME), m.get(FixTags.TRANSACT_TIME));
        }
    }

    @Test
    @DisplayName("SenderCompID follows the message direction: D/G/F/Q from CLIENT, 8/9 from MOCKVENUE")
    void directionsAreCorrect() {
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            boolean fromVenue = List.of(FixTags.MSG_EXECUTION_REPORT, FixTags.MSG_CANCEL_REJECT)
                    .contains(m.get(FixTags.MSG_TYPE));
            assertEquals(fromVenue ? FixTags.SENDER_VENUE : FixTags.SENDER_CLIENT, m.get(FixTags.SENDER_COMP_ID));
            assertEquals(fromVenue ? FixTags.SENDER_CLIENT : FixTags.SENDER_VENUE, m.get(FixTags.TARGET_COMP_ID));
        }
    }

    @Test
    @DisplayName("MsgSeqNum increments by one per session direction, in wire order")
    void sequenceNumbersArePerSession() {
        Map<String, Long> next = new HashMap<>();
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            String session = m.get(FixTags.SENDER_COMP_ID);
            long expected = next.merge(session, 1L, Long::sum);
            assertEquals(expected, Long.parseLong(m.get(FixTags.MSG_SEQ_NUM)), "session " + session);
        }
        assertEquals(2, next.size());
    }

    @Test
    @DisplayName("SendingTime/TransactTime are monotone in wire order")
    void timestampsAreMonotone() {
        String previous = "";
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            String sendingTime = m.get(FixTags.SENDING_TIME);
            assertTrue(sendingTime.compareTo(previous) >= 0, "timestamps went backwards at " + sendingTime);
            previous = sendingTime;
        }
    }

    @Test
    @DisplayName("HandlInst=1 on D and G; TimeInForce present on every order-bearing message")
    void requiredOrderTagsPresent() {
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            String msgType = m.get(FixTags.MSG_TYPE);
            if (FixTags.MSG_NEW_ORDER_SINGLE.equals(msgType) || FixTags.MSG_CANCEL_REPLACE_REQUEST.equals(msgType)) {
                assertEquals("1", m.get(FixTags.HANDL_INST), "21 HandlInst required on D/G in FIX 4.2");
                assertNotNull(m.get(FixTags.TIME_IN_FORCE));
                assertNotNull(m.get(FixTags.ORD_TYPE));
                assertEquals("2".equals(m.get(FixTags.ORD_TYPE)), m.containsKey(FixTags.PRICE),
                        "44 Price is required exactly when 40=2");
            }
        }
    }

    @Test
    @DisplayName("ExecIDs are globally unique and every 8 carries one")
    void execIdsAreUnique() {
        Set<String> seen = new HashSet<>();
        int execs = 0;
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            if (!isExec(m)) {
                continue;
            }
            execs++;
            String execId = m.get(FixTags.EXEC_ID);
            assertNotNull(execId);
            assertTrue(seen.add(execId), "duplicate ExecID " + execId);
        }
        assertTrue(execs > CHAINS);
    }

    @Test
    @DisplayName("OrderID is stable per chain and equals the Kafka record key")
    void orderIdIsStablePerChain() {
        ScenarioEngine.GeneratedBatch batch = defaultBatch();
        for (ScenarioEngine.EmittedMessage emitted : batch.messages()) {
            Map<Integer, String> m = TestFix.parse(FixSerializer.serialize(emitted.message()));
            if (FixTags.MSG_NEW_ORDER_SINGLE.equals(m.get(FixTags.MSG_TYPE))) {
                assertFalse(m.containsKey(FixTags.ORDER_ID), "37 OrderID only exists once the venue assigns it");
            } else {
                assertEquals(emitted.chainKey(), m.get(FixTags.ORDER_ID));
            }
        }
        for (OrderScenario chain : batch.chains()) {
            assertEquals(chain.chainKey(), chain.expected().orderId());
        }
    }

    @Test
    @DisplayName("every 8 echoes a ClOrdID; ClOrdIDs are unique within a chain")
    void clOrdIdEchoAndUniqueness() {
        for (List<Map<Integer, String>> chain : byChain(defaultBatch()).values()) {
            Set<String> requested = new HashSet<>();
            for (Map<Integer, String> m : chain) {
                if (isExec(m)) {
                    assertNotNull(m.get(FixTags.CL_ORD_ID), "8 must echo 11");
                } else if (!FixTags.MSG_DONT_KNOW_TRADE.equals(m.get(FixTags.MSG_TYPE))
                        && !FixTags.MSG_CANCEL_REJECT.equals(m.get(FixTags.MSG_TYPE))) {
                    assertTrue(requested.add(m.get(FixTags.CL_ORD_ID)),
                            "ClOrdID reused within a chain: " + m.get(FixTags.CL_ORD_ID));
                }
            }
        }
    }

    @Test
    @DisplayName("G/F carry a new ClOrdID with 41 = the chain's prior current ClOrdID")
    void chainLinkageThroughOrigClOrdId() {
        int linked = 0;
        for (List<Map<Integer, String>> chain : byChain(defaultBatch()).values()) {
            String current = null;
            for (Map<Integer, String> m : chain) {
                String msgType = m.get(FixTags.MSG_TYPE);
                switch (msgType) {
                    case FixTags.MSG_NEW_ORDER_SINGLE -> current = m.get(FixTags.CL_ORD_ID);
                    case FixTags.MSG_CANCEL_REPLACE_REQUEST, FixTags.MSG_CANCEL_REQUEST -> {
                        assertEquals(current, m.get(FixTags.ORIG_CL_ORD_ID), "41 must be the prior current 11");
                        assertFalse(current.equals(m.get(FixTags.CL_ORD_ID)), "G/F must carry a new 11");
                        linked++;
                    }
                    case FixTags.MSG_EXECUTION_REPORT -> {
                        // A replace confirm rotates the current ClOrdID; other 8s just echo it.
                        if (FixTags.EXEC_TYPE_REPLACED.equals(m.get(FixTags.EXEC_TYPE))) {
                            assertEquals(current, m.get(FixTags.ORIG_CL_ORD_ID));
                            current = m.get(FixTags.CL_ORD_ID);
                        }
                    }
                    default -> { }
                }
            }
        }
        assertTrue(linked > 0, "batch should contain amend/cancel requests");
    }

    @Test
    @DisplayName("replace/cancel confirms carry 41; plain acks and fills do not")
    void origClOrdIdOnlyOnConfirms() {
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            if (!isExec(m)) {
                continue;
            }
            boolean isConfirm = List.of(FixTags.EXEC_TYPE_PENDING_REPLACE, FixTags.EXEC_TYPE_REPLACED,
                    FixTags.EXEC_TYPE_PENDING_CANCEL, FixTags.EXEC_TYPE_CANCELED)
                    .contains(m.get(FixTags.EXEC_TYPE));
            assertEquals(isConfirm, m.containsKey(FixTags.ORIG_CL_ORD_ID),
                    "41 presence on 150=" + m.get(FixTags.EXEC_TYPE));
        }
    }

    @Test
    @DisplayName("fills carry 32/31/30; non-fill acks do not")
    void fillsCarryLastSharesPxMkt() {
        int fills = 0;
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            if (!isExec(m)) {
                continue;
            }
            if (isFill(m)) {
                fills++;
                assertNotNull(m.get(FixTags.LAST_SHARES));
                assertNotNull(m.get(FixTags.LAST_PX));
                assertNotNull(m.get(FixTags.LAST_MKT));
                assertTrue(num(m, FixTags.LAST_SHARES) > 0);
                assertTrue(num(m, FixTags.LAST_PX) > 0);
            } else if (!m.containsKey(FixTags.EXEC_REF_ID)) {
                assertFalse(m.containsKey(FixTags.LAST_SHARES), "non-fill 8 must not carry 32");
            }
        }
        assertTrue(fills > 0);
    }

    @Test
    @DisplayName("rejects carry 103; cancel rejects carry 434, 102, 58 and 39")
    void rejectTagsPresent() {
        int orderRejects = 0;
        int cancelRejects = 0;
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            if (isExec(m) && FixTags.EXEC_TYPE_REJECTED.equals(m.get(FixTags.EXEC_TYPE))) {
                orderRejects++;
                assertNotNull(m.get(FixTags.ORD_REJ_REASON));
            }
            if (FixTags.MSG_CANCEL_REJECT.equals(m.get(FixTags.MSG_TYPE))) {
                cancelRejects++;
                assertTrue(List.of(FixTags.CXL_REJ_RESPONSE_TO_CANCEL, FixTags.CXL_REJ_RESPONSE_TO_REPLACE)
                        .contains(m.get(FixTags.CXL_REJ_RESPONSE_TO)));
                assertNotNull(m.get(FixTags.CXL_REJ_REASON));
                assertNotNull(m.get(FixTags.TEXT));
                assertNotNull(m.get(FixTags.ORD_STATUS));
            }
        }
        assertTrue(orderRejects > 0);
        assertTrue(cancelRejects > 0);
    }

    @Test
    @DisplayName("CumQty is monotone within a chain except across a bust or correct restatement")
    void cumQtyMonotoneExceptRestatements() {
        int restatements = 0;
        for (List<Map<Integer, String>> chain : byChain(defaultBatch()).values()) {
            double cum = 0;
            for (Map<Integer, String> m : chain) {
                if (!isExec(m)) {
                    continue;
                }
                double reported = num(m, FixTags.CUM_QTY);
                String transType = m.getOrDefault(FixTags.EXEC_TRANS_TYPE, FixTags.EXEC_TRANS_NEW);
                if (FixTags.EXEC_TRANS_NEW.equals(transType)) {
                    assertTrue(reported >= cum, "CumQty went backwards without a 20=1/20=2 restatement");
                } else {
                    restatements++;
                }
                cum = reported;
            }
        }
        assertTrue(restatements > 0, "batch should contain bust/correct restatements");
    }

    @Test
    @DisplayName("LeavesQty = current OrderQty - CumQty, except on terminal cancel/reject where it is 0")
    void leavesQtyIdentity() {
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            if (!isExec(m)) {
                continue;
            }
            String execType = m.get(FixTags.EXEC_TYPE);
            boolean terminalZero = FixTags.EXEC_TYPE_CANCELED.equals(execType)
                    || FixTags.EXEC_TYPE_REJECTED.equals(execType);
            if (terminalZero) {
                assertEquals(0.0, num(m, FixTags.LEAVES_QTY));
            } else {
                assertEquals(num(m, FixTags.ORDER_QTY) - num(m, FixTags.CUM_QTY),
                        num(m, FixTags.LEAVES_QTY), 1e-9,
                        "151 must equal 38 - 14 on 150=" + execType);
            }
        }
    }

    @Test
    @DisplayName("AvgPx equals traded notional / CumQty, honouring busts and corrects")
    void avgPxConsistency() {
        for (List<Map<Integer, String>> chain : byChain(defaultBatch()).values()) {
            Map<String, double[]> tradesByExecId = new HashMap<>();
            double notional = 0;
            double cum = 0;
            for (Map<Integer, String> m : chain) {
                if (!isExec(m)) {
                    continue;
                }
                String transType = m.getOrDefault(FixTags.EXEC_TRANS_TYPE, FixTags.EXEC_TRANS_NEW);
                switch (transType) {
                    case FixTags.EXEC_TRANS_NEW -> {
                        if (isFill(m)) {
                            double shares = num(m, FixTags.LAST_SHARES);
                            double px = num(m, FixTags.LAST_PX);
                            tradesByExecId.put(m.get(FixTags.EXEC_ID), new double[] {shares, px});
                            notional += shares * px;
                            cum += shares;
                        }
                    }
                    case FixTags.EXEC_TRANS_CANCEL -> {
                        double[] busted = tradesByExecId.remove(m.get(FixTags.EXEC_REF_ID));
                        assertNotNull(busted, "19 ExecRefID must reference a known fill");
                        notional -= busted[0] * busted[1];
                        cum -= busted[0];
                    }
                    case FixTags.EXEC_TRANS_CORRECT -> {
                        double[] original = tradesByExecId.remove(m.get(FixTags.EXEC_REF_ID));
                        assertNotNull(original, "19 ExecRefID must reference a known fill");
                        notional -= original[0] * original[1];
                        cum -= original[0];
                        double shares = num(m, FixTags.LAST_SHARES);
                        double px = num(m, FixTags.LAST_PX);
                        tradesByExecId.put(m.get(FixTags.EXEC_ID), new double[] {shares, px});
                        notional += shares * px;
                        cum += shares;
                    }
                    default -> throw new AssertionError("unexpected 20=" + transType);
                }
                assertEquals(cum, num(m, FixTags.CUM_QTY), 1e-9, "14 CumQty must match the replayed fills");
                double expectedAvgPx = cum > 0 ? notional / cum : 0;
                assertEquals(expectedAvgPx, num(m, FixTags.AVG_PX), 1e-5, "6 AvgPx must be notional / CumQty");
            }
        }
    }

    @Test
    @DisplayName("the same seed replays byte-identical messages and keys")
    void deterministicUnderSeed() {
        List<String> first = batch(42L).messages().stream()
                .map(m -> m.chainKey() + " " + FixSerializer.serialize(m.message())).toList();
        List<String> second = batch(42L).messages().stream()
                .map(m -> m.chainKey() + " " + FixSerializer.serialize(m.message())).toList();
        assertEquals(first, second);

        List<String> other = batch(43L).messages().stream()
                .map(m -> m.chainKey() + " " + FixSerializer.serialize(m.message())).toList();
        assertFalse(first.equals(other), "a different seed must produce a different stream");
    }

    @Test
    @DisplayName("interleaving preserves per-chain order while several chains are live at once")
    void interleavingPreservesPerChainOrder() {
        ScenarioEngine.GeneratedBatch batch = defaultBatch();
        Map<String, List<FixMessage>> streamed = new LinkedHashMap<>();
        for (ScenarioEngine.EmittedMessage emitted : batch.messages()) {
            streamed.computeIfAbsent(emitted.chainKey(), k -> new ArrayList<>()).add(emitted.message());
        }
        assertEquals(CHAINS, streamed.size());
        for (OrderScenario chain : batch.chains()) {
            List<FixMessage> scripted = chain.steps().stream().map(OrderScenario.Step::message).toList();
            assertEquals(scripted, streamed.get(chain.chainKey()), "per-chain order must be preserved");
        }

        long distinctChainsInFirstWindow = batch.messages().stream()
                .limit(ScenarioEngine.MAX_LIVE_CHAINS)
                .map(ScenarioEngine.EmittedMessage::chainKey)
                .distinct()
                .count();
        assertEquals(ScenarioEngine.MAX_LIVE_CHAINS, distinctChainsInFirstWindow,
                "chains should progress concurrently, not one after another");
    }

    @Test
    @DisplayName("the exported expected state matches the final snapshot on the wire")
    void expectedStateMatchesTheStream() {
        ScenarioEngine.GeneratedBatch batch = defaultBatch();
        Map<String, List<Map<Integer, String>>> chains = byChain(batch);
        assertEquals(CHAINS, batch.expectedStates().size());

        for (OrderScenario chain : batch.chains()) {
            ExpectedChainState expected = chain.expected();
            List<Map<Integer, String>> messages = chains.get(chain.chainKey());

            String lastStatus = null;
            double lastCum = Double.NaN;
            double lastLeaves = Double.NaN;
            for (Map<Integer, String> m : messages) {
                if (m.containsKey(FixTags.ORD_STATUS)) {
                    lastStatus = m.get(FixTags.ORD_STATUS);
                }
                if (isExec(m)) {
                    lastCum = num(m, FixTags.CUM_QTY);
                    lastLeaves = num(m, FixTags.LEAVES_QTY);
                }
            }
            assertEquals(FixTags.ordStatusName(lastStatus), expected.ordStatus(), chain.chainKey());
            assertEquals(lastCum, expected.cumQty(), chain.chainKey());
            assertEquals(lastLeaves, expected.leavesQty(), chain.chainKey());
            assertEquals(chain.chainKey(), expected.chainKey());
            assertEquals(chain.scenario().cliName(), expected.scenario());
        }
    }

    @Test
    @DisplayName("the current ClOrdID rotates only on a replace confirm (150=5)")
    void currentClOrdIdRotatesOnlyOnReplace() {
        ScenarioEngine.GeneratedBatch batch = defaultBatch();
        Map<String, List<Map<Integer, String>>> chains = byChain(batch);
        for (OrderScenario chain : batch.chains()) {
            String current = null;
            for (Map<Integer, String> m : chains.get(chain.chainKey())) {
                if (FixTags.MSG_NEW_ORDER_SINGLE.equals(m.get(FixTags.MSG_TYPE))) {
                    current = m.get(FixTags.CL_ORD_ID);
                } else if (isExec(m) && FixTags.EXEC_TYPE_REPLACED.equals(m.get(FixTags.EXEC_TYPE))) {
                    current = m.get(FixTags.CL_ORD_ID);
                }
            }
            assertEquals(current, chain.expected().clOrdID(), chain.chainKey());
        }
    }

    @Test
    @DisplayName("order terms are drawn from the documented pools")
    void termsComeFromTheConfiguredPools() {
        Set<String> accounts = new HashSet<>();
        Set<String> symbols = new HashSet<>();
        for (Map<Integer, String> m : parsed(defaultBatch())) {
            if (m.containsKey(FixTags.ACCOUNT)) {
                accounts.add(m.get(FixTags.ACCOUNT));
            }
            if (m.containsKey(FixTags.SYMBOL)) {
                symbols.add(m.get(FixTags.SYMBOL));
            }
            if (m.containsKey(FixTags.SIDE)) {
                assertTrue(List.of("1", "2", "5").contains(m.get(FixTags.SIDE)));
            }
            if (m.containsKey(FixTags.TIME_IN_FORCE)) {
                assertTrue(List.of("0", "1", "3", "4").contains(m.get(FixTags.TIME_IN_FORCE)));
            }
        }
        assertTrue(accounts.stream().allMatch(a -> a.matches("ACC-[1-5]")), accounts.toString());
        assertTrue(accounts.size() > 1);
        assertTrue(symbols.size() > 1);
        assertTrue(symbols.stream().allMatch(s -> s.matches("[A-Z]{1,5}")), symbols.toString());
    }
}
