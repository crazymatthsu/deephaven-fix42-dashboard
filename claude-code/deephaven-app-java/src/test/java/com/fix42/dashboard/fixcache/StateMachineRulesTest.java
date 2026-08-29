package com.fix42.dashboard.fixcache;

import static com.fix42.dashboard.fixcache.FixTestMessages.cancelReject;
import static com.fix42.dashboard.fixcache.FixTestMessages.cancelRequest;
import static com.fix42.dashboard.fixcache.FixTestMessages.dkTrade;
import static com.fix42.dashboard.fixcache.FixTestMessages.execReport;
import static com.fix42.dashboard.fixcache.FixTestMessages.newOrder;
import static com.fix42.dashboard.fixcache.FixTestMessages.replaceRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fix42.dashboard.fixcache.FixEnums.ExecType;
import com.fix42.dashboard.fixcache.FixEnums.OrdStatus;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * One test per transition rule in doc 01 section 5 (rules 1-7).
 *
 * <p>Mirrors {@code deephaven-scripts/tests/test_state_machine_rules.py} case for case, so a change
 * to the contract fails in both languages.
 */
class StateMachineRulesTest {

    private OrderStateMachine machine;

    @BeforeEach
    void setUp() {
        machine = new OrderStateMachine(new FakeClock());
    }

    /** {@code D} then the venue ack -- the common prelude. */
    private void newAndAck() {
        newAndAck("C1", 1000);
    }

    private void newAndAck(String clOrdId, double qty) {
        machine.process(newOrder(clOrdId).qty(qty).build());
        machine.process(execReport("A").execId("EA").clOrdId(clOrdId).cumQty(0).leavesQty(qty).avgPx(0).build());
        machine.process(execReport("0").execId("E0").clOrdId(clOrdId).cumQty(0).leavesQty(qty).avgPx(0).build());
    }

    private static List<String> eventTypes(Result result) {
        return result.events().stream().map(OrderEventRow::eventType).toList();
    }

    @Nested
    @DisplayName("Rule 1 -- D creates the chain")
    class Rule1 {

        @Test
        void newOrderCreatesChainAndSeedsTerms() {
            Result result = machine.process(newOrder("C1").qty(1000).price(185.50).build());

            assertNull(result.error());
            OrderState state = result.state();
            assertNotNull(state);
            var row = state.toRow();
            assertEquals("C1", state.orderKey());
            assertEquals("PENDING_NEW", row.get("OrdStatus"));
            assertEquals(PendingAction.NEW, row.get("PendingAction"));
            assertEquals("ACC1", row.get("Account"));
            assertEquals("IBM", row.get("Symbol"));
            assertEquals("BUY", row.get("Side"));
            assertEquals("LIMIT", row.get("OrdType"));
            assertEquals("DAY", row.get("TimeInForce"));
            assertEquals(1000.0, row.get("OrderQty"));
            assertEquals(185.50, row.get("Price"));
            assertEquals(0.0, row.get("CumQty"));
            assertEquals(1000.0, row.get("LeavesQty"));
            assertEquals(0.0, row.get("AvgPx"));
            assertEquals("C1", row.get("RootClOrdID"));
            assertEquals("C1", row.get("ClOrdID"));
            assertEquals("C1", row.get("ClOrdIDChain"));
            assertEquals("", row.get("OrderID"));
            assertEquals(1L, row.get("MsgCount"));
            assertEquals(0L, row.get("ExecCount"));
            assertEquals("D", row.get("LastMsgType"));
            assertEquals(Boolean.FALSE, row.get("Terminal"));
            assertNotNull(state.firstSeenTs());
            assertNotNull(state.lastUpdateTs());
        }

        @Test
        void newOrderEmitsNewRequestEventWithProposedTerms() {
            Result result = machine.process(newOrder("C1").qty(1000).price(185.50).build());

            assertEquals(List.of(), result.executions());
            assertEquals(List.of(EventType.NEW_REQUEST), eventTypes(result));
            OrderEventRow event = result.events().get(0);
            assertEquals("D", event.msgType());
            assertEquals(1000.0, event.orderQty());
            assertEquals(185.50, event.price());
            assertTrue(event.detail().contains("new order request"), event.detail());
            assertEquals("PENDING_NEW", event.toRow().get("OrdStatus"));
        }
    }

    @Nested
    @DisplayName("Rule 2 -- execution reports")
    class Rule2 {

        @Test
        void ackSetsNewAndClearsPendingNew() {
            machine.process(newOrder("C1").build());
            Result result = machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).avgPx(0).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(OrdStatus.NEW, state.ordStatus());
            assertEquals(ExecType.NEW, state.lastExecType());
            assertEquals("ORD-1", state.orderId());
            assertEquals(PendingAction.NONE, state.pendingAction());
            assertEquals("", state.pendingClOrdId());
            assertEquals(1L, state.execCount());
            assertEquals(List.of(EventType.NEW_ACK), eventTypes(result));
            assertEquals(1, result.executions().size());
            assertFalse(result.executions().get(0).isFill());
            assertEquals(FillStatus.NORMAL, result.executions().get(0).fillStatus());
        }

        @Test
        void pendingNewReportKeepsTheNewRequestInFlight() {
            machine.process(newOrder("C1").build());
            Result result = machine.process(execReport("A").execId("EA").cumQty(0).leavesQty(1000).build());

            assertNotNull(result.state());
            assertEquals(OrdStatus.PENDING_NEW, result.state().ordStatus());
            assertEquals(PendingAction.NEW, result.state().pendingAction());
            assertEquals(List.of(EventType.PENDING_NEW), eventTypes(result));
        }

        @Test
        @DisplayName("OrdStatus always comes from tag 39, never from ExecType")
        void ordStatusAlwaysComesFromTag39() {
            newAndAck();
            machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                    .cumQty(400).leavesQty(600).avgPx(185.48).build());
            machine.process(replaceRequest("C2", "C1").qty(1000).price(185.55).build());
            Result result = machine.process(execReport("5").execId("E2").clOrdId("C2").origClOrdId("C1")
                    .ordStatus("1").cumQty(400).leavesQty(600).avgPx(185.48).build());

            assertNotNull(result.state());
            assertEquals(OrdStatus.PARTIALLY_FILLED, result.state().ordStatus());
            assertEquals(ExecType.REPLACED, result.state().lastExecType());
        }

        @Test
        void adoptsAbsoluteSnapshotsVerbatim() {
            newAndAck();
            Result result = machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                    .cumQty(400).leavesQty(600).avgPx(185.48).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(400.0, state.cumQty());
            assertEquals(600.0, state.leavesQty());
            assertEquals(185.48, state.avgPx());
        }

        @Test
        void fillReportSetsLastSharesPxAndMkt() {
            newAndAck();
            Result result = machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                    .lastMkt("XNAS").cumQty(400).leavesQty(600).avgPx(185.48).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(400.0, state.lastShares());
            assertEquals(185.48, state.lastPx());
            assertEquals("XNAS", state.lastMkt());
            assertTrue(result.executions().get(0).isFill());
            assertEquals(List.of(EventType.PARTIAL_FILL), eventTypes(result));
        }

        @Test
        void fullFillEmitsFullFillEvent() {
            newAndAck();
            Result result = machine.process(execReport("2").execId("E1").lastShares(1000).lastPx(185.50)
                    .cumQty(1000).leavesQty(0).avgPx(185.50).build());

            assertNotNull(result.state());
            assertEquals(OrdStatus.FILLED, result.state().ordStatus());
            assertTrue(result.state().terminal());
            assertEquals(List.of(EventType.FULL_FILL), eventTypes(result));
        }

        @Test
        @DisplayName("a duplicate ExecID binds and counts but applies nothing")
        void execIdDedupeAppliesNothing() {
            newAndAck();
            String fill = execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                    .cumQty(400).leavesQty(600).avgPx(185.48).build();
            machine.process(fill);
            OrderState before = machine.getByClOrdId("C1");
            Result result = machine.process(fill);

            OrderState state = result.state();
            assertNotNull(before);
            assertNotNull(state);
            assertEquals(400.0, state.cumQty());
            assertEquals(before.cumQty(), state.cumQty());
            assertEquals(before.execCount(), state.execCount());
            assertEquals(before.msgCount() + 1, state.msgCount());
            assertEquals(List.of(), result.events());
            assertEquals(1, result.executions().size());
            assertEquals("E1", result.executions().get(0).execId());
        }

        @Test
        void rejectIsTerminalAndRecordsReason() {
            machine.process(newOrder("C1").build());
            Result result = machine.process(execReport("8").execId("E1").ordRejReason("99")
                    .text("unknown symbol").cumQty(0).leavesQty(0).avgPx(0).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(OrdStatus.REJECTED, state.ordStatus());
            assertTrue(state.terminal());
            assertEquals("99", state.ordRejReason());
            assertEquals("unknown symbol", state.text());
            assertEquals(PendingAction.NONE, state.pendingAction());
            assertEquals(List.of(EventType.NEW_REJECT), eventTypes(result));
            assertTrue(result.events().get(0).detail().contains("reject:"));
        }

        @Test
        void unsolicitedCancelIsAccepted() {
            newAndAck();
            Result result = machine.process(execReport("4").execId("E1").cumQty(0).leavesQty(0).avgPx(0).build());

            assertNotNull(result.state());
            assertEquals(OrdStatus.CANCELED, result.state().ordStatus());
            assertTrue(result.state().terminal());
            assertEquals(PendingAction.NONE, result.state().pendingAction());
            assertEquals(List.of(EventType.CANCEL_ACK), eventTypes(result));
        }

        @Test
        void bustAdoptsRestatedSnapshotsAndMarksTheExec() {
            newAndAck();
            machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                    .cumQty(400).leavesQty(600).avgPx(185.48).build());
            Result result = machine.process(execReport("D").execId("E2").execTransType("1").execRefId("E1")
                    .ordStatus("0").cumQty(0).leavesQty(1000).avgPx(0).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(0.0, state.cumQty());
            assertEquals(1000.0, state.leavesQty());
            assertEquals(0.0, state.avgPx());
            assertEquals(OrdStatus.NEW, state.ordStatus());
            // LastShares/LastPx are untouched by a bust (doc 01 section 5.2).
            assertEquals(400.0, state.lastShares());
            assertEquals(185.48, state.lastPx());
            assertEquals(List.of(EventType.FILL_BUST), eventTypes(result));
            assertEquals(List.of("E2", "E1"), result.executions().stream().map(ExecutionRow::execId).toList());
            assertEquals(FillStatus.BUSTED, result.executions().get(1).fillStatus());
            assertEquals(0.0, result.executions().get(1).cumQty());
        }

        @Test
        void correctAdoptsRestatedSnapshotsAndUpdatesTheExec() {
            newAndAck();
            machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                    .cumQty(400).leavesQty(600).avgPx(185.48).build());
            Result result = machine.process(execReport("D").execId("E2").execTransType("2").execRefId("E1")
                    .ordStatus("1").lastShares(400).lastPx(185.50)
                    .cumQty(400).leavesQty(600).avgPx(185.50).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(400.0, state.cumQty());
            assertEquals(185.50, state.avgPx());
            assertEquals(List.of(EventType.FILL_CORRECT), eventTypes(result));
            ExecutionRow corrected = result.executions().get(1);
            assertEquals("E1", corrected.execId());
            assertEquals(FillStatus.CORRECTED, corrected.fillStatus());
            assertEquals(185.50, corrected.lastPx());
            assertEquals(400.0, corrected.lastShares());
        }

        @Test
        void staleReportSkipsEconomicFields() {
            newAndAck();
            machine.process(execReport("2").execId("E1").lastShares(1000).lastPx(185.50)
                    .cumQty(1000).leavesQty(0).avgPx(185.50).build());
            Result result = machine.process(execReport("1").execId("E2").lastShares(400).lastPx(185.40)
                    .cumQty(400).leavesQty(600).avgPx(185.40).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(1000.0, state.cumQty());
            assertEquals(0.0, state.leavesQty());
            assertEquals(185.50, state.avgPx());
            assertEquals(1000.0, state.lastShares());
            assertEquals(185.50, state.lastPx());
            assertEquals(5L, state.msgCount());
        }

        @Test
        @DisplayName("the stale guard skips economic fields; tag 39 stays venue truth")
        void staleReportStillTakesOrdStatusFromTag39() {
            newAndAck();
            machine.process(execReport("2").execId("E1").cumQty(1000).leavesQty(0).avgPx(185.50).build());
            Result result = machine.process(
                    execReport("1").execId("E2").ordStatus("1").cumQty(400).leavesQty(600).build());

            assertNotNull(result.state());
            assertEquals(OrdStatus.PARTIALLY_FILLED, result.state().ordStatus());
            assertEquals(1000.0, result.state().cumQty());
        }

        @Test
        void executionReportMayCreateTheChain() {
            Result result = machine.process(
                    execReport("0").execId("E0").clOrdId("C1").cumQty(0).leavesQty(1000).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals("ORD-1", state.orderKey());
            assertEquals("C1", state.clOrdId());
            assertEquals("C1", state.rootClOrdId());
            assertEquals("IBM", state.symbol());
            assertEquals(1000.0, state.orderQty());
            assertEquals(1, machine.orderCount());
        }
    }

    @Nested
    @DisplayName("Rule 3 -- G stages the amend")
    class Rule3 {

        @Test
        void amendRequestGoesPendingAndStagesTerms() {
            newAndAck();
            Result result = machine.process(replaceRequest("C2", "C1").qty(1500).price(185.55).tif("1").build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(OrdStatus.PENDING_REPLACE, state.ordStatus());
            assertEquals(PendingAction.REPLACE, state.pendingAction());
            assertEquals("C2", state.pendingClOrdId());
            // Staged, not applied.
            assertEquals(1000.0, state.orderQty());
            assertEquals(185.50, state.price());
            assertEquals("DAY", state.toRow().get("TimeInForce"));
            assertEquals("C1", state.clOrdId());
            assertEquals("C1", state.toRow().get("ClOrdIDChain"));
            assertEquals("C1", state.origClOrdId());
            assertEquals(List.of(EventType.AMEND_REQUEST), eventTypes(result));
            assertEquals(1500.0, result.events().get(0).orderQty());
            assertEquals(185.55, result.events().get(0).price());
        }

        @Test
        void stagedTermsApplyOnlyOnReplaceConfirm() {
            newAndAck();
            machine.process(replaceRequest("C2", "C1").qty(1500).price(185.55).tif("1").build());
            Result pending = machine.process(
                    execReport("E").execId("E1").clOrdId("C2").origClOrdId("C1").cumQty(0).leavesQty(1000).build());
            assertNotNull(pending.state());
            assertEquals(1000.0, pending.state().orderQty());
            assertEquals(185.50, pending.state().price());
            assertEquals(PendingAction.REPLACE, pending.state().pendingAction());
            assertEquals(List.of(EventType.PENDING_AMEND), eventTypes(pending));

            Result result = machine.process(execReport("5").execId("E2").clOrdId("C2").origClOrdId("C1")
                    .ordStatus("0").cumQty(0).leavesQty(1500).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(1500.0, state.orderQty());
            assertEquals(185.55, state.price());
            assertEquals("GTC", state.toRow().get("TimeInForce"));
            assertEquals("C2", state.clOrdId());
            assertEquals("C1,C2", state.toRow().get("ClOrdIDChain"));
            assertEquals("C1", state.rootClOrdId());
            assertEquals(PendingAction.NONE, state.pendingAction());
            assertEquals("", state.pendingClOrdId());
            assertEquals(List.of(EventType.AMEND_ACK), eventTypes(result));
        }
    }

    @Nested
    @DisplayName("Rule 4 -- F requests a cancel")
    class Rule4 {

        @Test
        void cancelRequestGoesPendingCancel() {
            newAndAck();
            Result result = machine.process(cancelRequest("C2", "C1").build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(OrdStatus.PENDING_CANCEL, state.ordStatus());
            assertEquals(PendingAction.CANCEL, state.pendingAction());
            assertEquals("C2", state.pendingClOrdId());
            assertEquals("C1", state.clOrdId(), "a cancel id never becomes the order identity");
            assertEquals("C1", state.toRow().get("ClOrdIDChain"));
            assertEquals(List.of(EventType.CANCEL_REQUEST), eventTypes(result));
        }

        @Test
        void cancelAckClearsThePendingCancel() {
            newAndAck();
            machine.process(cancelRequest("C2", "C1").build());
            Result result = machine.process(execReport("4").execId("E1").clOrdId("C2").origClOrdId("C1")
                    .cumQty(0).leavesQty(0).avgPx(0).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(OrdStatus.CANCELED, state.ordStatus());
            assertTrue(state.terminal());
            assertEquals(PendingAction.NONE, state.pendingAction());
            assertEquals("", state.pendingClOrdId());
            assertEquals(List.of(EventType.CANCEL_ACK), eventTypes(result));
        }
    }

    @Nested
    @DisplayName("Rule 5 -- 9 reverts the pending request")
    class Rule5 {

        @Test
        void cancelRejectRevertsToTheSnapshottedStatus() {
            newAndAck();
            machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                    .cumQty(400).leavesQty(600).avgPx(185.48).build());
            machine.process(cancelRequest("C2", "C1").build());
            Result result = machine.process(cancelReject("C2", "C1").responseTo("1")
                    .cxlRejReason("0").text("too late to cancel").build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(OrdStatus.PARTIALLY_FILLED, state.ordStatus());
            assertEquals(PendingAction.NONE, state.pendingAction());
            assertEquals("", state.pendingClOrdId());
            assertEquals("0", state.cxlRejReason());
            assertEquals("too late to cancel", state.text());
            assertEquals(List.of(EventType.CANCEL_REJECT), eventTypes(result));
            assertEquals("reject: too late to cancel (102=0)", result.events().get(0).detail());
        }

        @Test
        void venueTag39OnTheRejectWinsOverTheSnapshot() {
            newAndAck();
            machine.process(cancelRequest("C2", "C1").build());
            Result result = machine.process(
                    cancelReject("C2", "C1").responseTo("1").ordStatus("1").cxlRejReason("0").build());

            assertNotNull(result.state());
            assertEquals(OrdStatus.PARTIALLY_FILLED, result.state().ordStatus());
        }

        @Test
        void amendRejectDiscardsTheStagedTerms() {
            newAndAck();
            machine.process(replaceRequest("C2", "C1").qty(1500).price(185.55).build());
            Result result = machine.process(
                    cancelReject("C2", "C1").responseTo("2").cxlRejReason("2").text("too late").build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(OrdStatus.NEW, state.ordStatus());
            assertEquals(1000.0, state.orderQty());
            assertEquals(185.50, state.price());
            assertEquals("C1", state.clOrdId());
            assertEquals(PendingAction.NONE, state.pendingAction());
            assertEquals(List.of(EventType.AMEND_REJECT), eventTypes(result));

            // A later replace confirm must not resurrect the discarded terms.
            Result later = machine.process(execReport("5").execId("E9").clOrdId("C3").origClOrdId("C1")
                    .ordStatus("0").cumQty(0).leavesQty(1000).build());
            assertNotNull(later.state());
            assertEquals(1000.0, later.state().orderQty());
            assertEquals(185.50, later.state().price());
        }

        @Test
        void tag434SelectsWhichPendingFlagClears() {
            newAndAck();
            machine.process(replaceRequest("C2", "C1").qty(1500).price(185.55).build());
            machine.process(cancelRequest("C3", "C1").build());
            Result result = machine.process(cancelReject("C3", "C1").responseTo("1").cxlRejReason("0").build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(PendingAction.REPLACE, state.pendingAction());
            assertEquals("C2", state.pendingClOrdId());
            assertEquals(OrdStatus.PENDING_REPLACE, state.ordStatus());
        }
    }

    @Nested
    @DisplayName("Rule 6 -- Q marks an execution disputed")
    class Rule6 {

        @Test
        void dkRecordsReasonWithoutEconomicChange() {
            newAndAck();
            machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                    .cumQty(400).leavesQty(600).avgPx(185.48).build());
            OrderState before = machine.getByClOrdId("C1");
            Result result = machine.process(dkTrade("E1").dkReason("A").text("unknown order").build());

            OrderState state = result.state();
            assertNotNull(before);
            assertNotNull(state);
            assertEquals(before.cumQty(), state.cumQty());
            assertEquals(before.leavesQty(), state.leavesQty());
            assertEquals(before.avgPx(), state.avgPx());
            assertEquals(before.ordStatus(), state.ordStatus());
            assertEquals("A", state.dkReason());
            assertEquals(List.of(EventType.DK_TRADE), eventTypes(result));
            assertEquals(1, result.executions().size());
            ExecutionRow dkRow = result.executions().get(0);
            assertEquals("E1", dkRow.execId());
            assertEquals(FillStatus.DK, dkRow.fillStatus());
            assertEquals(400.0, dkRow.lastShares(), "the disputed execution's own values");
        }
    }

    @Nested
    @DisplayName("Rule 7 -- bookkeeping applies to every message type")
    class Rule7 {

        @Test
        void everyMessageBindsIdsCountsAndStamps() {
            machine.process(newOrder("C1").build());
            machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
            machine.process(replaceRequest("C2", "C1").qty(1000).price(185.55).build());
            machine.process(execReport("5").execId("E1").clOrdId("C2").origClOrdId("C1")
                    .ordStatus("0").cumQty(0).leavesQty(1000).build());
            machine.process(cancelRequest("C3", "C2").build());
            Result result = machine.process(cancelReject("C3", "C2").responseTo("1").build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(6L, state.msgCount());
            assertEquals("9", state.lastMsgType());
            assertNotNull(state.lastUpdateTs());
            assertNotNull(state.firstSeenTs());
            assertTrue(state.firstSeenTs().isBefore(state.lastUpdateTs()));
            for (String identifier : List.of("C1", "C2", "C3")) {
                assertEquals("C1", machine.keyByClOrdId().get(identifier));
            }
            assertEquals("C1", machine.keyByOrderId().get("ORD-1"));
            assertEquals("C1", machine.keyByExecId().get("E0"));
        }

        @Test
        void terminalOrdersStillAcceptLateReportsAndCanReopen() {
            newAndAck();
            machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                    .cumQty(400).leavesQty(600).avgPx(185.48).build());
            machine.process(execReport("2").execId("E2").lastShares(600).lastPx(185.55)
                    .cumQty(1000).leavesQty(0).avgPx(185.522).build());
            OrderState filled = machine.getByClOrdId("C1");
            assertNotNull(filled);
            assertTrue(filled.terminal());

            Result result = machine.process(execReport("D").execId("E3").execTransType("1").execRefId("E2")
                    .ordStatus("1").cumQty(400).leavesQty(600).avgPx(185.48).build());

            OrderState state = result.state();
            assertNotNull(state);
            assertEquals(OrdStatus.PARTIALLY_FILLED, state.ordStatus());
            assertFalse(state.terminal());
            assertEquals(400.0, state.cumQty());
        }
    }
}
