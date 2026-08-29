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

import com.fix42.dashboard.fixcache.FixEnums.OrdStatus;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The twelve edge cases enumerated in doc 01 section 7.
 *
 * <p>Mirrors {@code deephaven-scripts/tests/test_state_machine_edge_cases.py}.
 */
class StateMachineEdgeCasesTest {

    private OrderStateMachine machine;

    @BeforeEach
    void setUp() {
        machine = new OrderStateMachine(new FakeClock());
    }

    @Test
    @DisplayName("01: searches by ClOrdID work before the venue assigns tag 37")
    void orderIdAbsentUntilFirstExecutionReport() {
        machine.process(newOrder("C1").build());

        assertNull(machine.getByOrderId("ORD-1"));
        OrderState byClOrdId = machine.getByClOrdId("C1");
        assertNotNull(byClOrdId);
        assertEquals("C1", byClOrdId.orderKey());
        assertEquals("", byClOrdId.orderId());

        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());

        OrderState healed = machine.getByOrderId("ORD-1");
        assertNotNull(healed);
        assertEquals("C1", healed.orderKey());
        assertEquals("ORD-1", healed.orderId());
    }

    @Test
    @DisplayName("02: an amend chain C1 -> C2 -> C3 shares one OrderKey")
    void amendChainSharesOneOrderKey() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        machine.process(replaceRequest("C2", "C1").qty(1000).price(185.55).build());
        machine.process(execReport("5").execId("E1").clOrdId("C2").origClOrdId("C1")
                .ordStatus("0").cumQty(0).leavesQty(1000).build());
        machine.process(replaceRequest("C3", "C2").qty(800).price(185.60).build());
        Result result = machine.process(execReport("5").execId("E2").clOrdId("C3").origClOrdId("C2")
                .ordStatus("0").cumQty(0).leavesQty(800).build());

        OrderState state = result.state();
        assertNotNull(state);
        assertEquals("C3", state.clOrdId());
        assertEquals("C1", state.rootClOrdId());
        assertEquals("C1,C2,C3", state.toRow().get("ClOrdIDChain"));
        assertEquals(800.0, state.orderQty());
        assertEquals(185.60, state.price());
        for (String identifier : List.of("C1", "C2", "C3")) {
            OrderState found = machine.getByClOrdId(identifier);
            assertNotNull(found);
            assertEquals("C1", found.orderKey());
        }
        assertEquals(1, machine.orderCount());
    }

    @Test
    @DisplayName("03: a duplicate ExecID replay does not double-count")
    void duplicateExecIdReplayDoesNotDoubleCount() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        String fill = execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                .cumQty(400).leavesQty(600).avgPx(185.48).build();
        machine.process(fill);
        machine.process(fill);
        machine.process(fill);

        OrderState state = machine.getByExecId("E1");
        assertNotNull(state);
        assertEquals(400.0, state.cumQty());
        assertEquals(600.0, state.leavesQty());
        assertEquals(2L, state.execCount());
        assertEquals(5L, state.msgCount());
    }

    @Test
    @DisplayName("04: with two requests in flight, a reject reverts only the cancel")
    void twoInFlightRequestsRejectRevertsOnlyTheCancel() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        machine.process(replaceRequest("C2", "C1").qty(1500).price(185.55).build());
        machine.process(cancelRequest("C3", "C1").build());
        Result result = machine.process(
                cancelReject("C3", "C1").responseTo("1").cxlRejReason("0").text("too late").build());

        OrderState state = result.state();
        assertNotNull(state);
        assertEquals(OrdStatus.PENDING_REPLACE, state.ordStatus());
        assertEquals(PendingAction.REPLACE, state.pendingAction());
        assertEquals("C2", state.pendingClOrdId());

        // The surviving replace still applies its staged terms when confirmed.
        Result confirmed = machine.process(execReport("5").execId("E1").clOrdId("C2").origClOrdId("C1")
                .ordStatus("0").cumQty(0).leavesQty(1500).build());
        assertNotNull(confirmed.state());
        assertEquals(1500.0, confirmed.state().orderQty());
        assertEquals(185.55, confirmed.state().price());
        assertEquals(PendingAction.NONE, confirmed.state().pendingAction());
    }

    @Test
    @DisplayName("05: reject before ack is terminal with no NEW")
    void rejectBeforeAckIsTerminal() {
        machine.process(newOrder("C1").build());
        Result result = machine.process(execReport("8").execId("E0").ordRejReason("1")
                .text("unknown symbol").cumQty(0).leavesQty(0).avgPx(0).build());

        OrderState state = result.state();
        assertNotNull(state);
        assertEquals(OrdStatus.REJECTED, state.ordStatus());
        assertTrue(state.terminal());
        assertEquals("REJECTED", state.toRow().get("LastExecType"));
        assertEquals("1", state.ordRejReason());
        assertEquals(PendingAction.NONE, state.pendingAction());
    }

    @Test
    @DisplayName("06: a bust after a full fill reopens PARTIALLY_FILLED")
    void bustAfterFullFillReopensPartiallyFilled() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                .cumQty(400).leavesQty(600).avgPx(185.48).build());
        machine.process(execReport("2").execId("E2").lastShares(600).lastPx(185.55)
                .cumQty(1000).leavesQty(0).avgPx(185.522).build());
        assertEquals(OrdStatus.FILLED, machine.getByClOrdId("C1").ordStatus());

        Result result = machine.process(execReport("D").execId("E3").execTransType("1").execRefId("E2")
                .ordStatus("1").cumQty(400).leavesQty(600).avgPx(185.48).build());

        OrderState state = result.state();
        assertNotNull(state);
        assertEquals(OrdStatus.PARTIALLY_FILLED, state.ordStatus());
        assertFalse(state.terminal());
        assertEquals(400.0, state.cumQty());
        assertEquals(600.0, state.leavesQty());
        assertEquals(185.48, state.avgPx());
        List<ExecutionRow> busted =
                result.executions().stream().filter(r -> r.execId().equals("E2")).toList();
        assertEquals(1, busted.size());
        assertEquals(FillStatus.BUSTED, busted.get(0).fillStatus());
    }

    @Test
    @DisplayName("07: a correct changes price only")
    void correctChangesPriceOnly() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                .cumQty(400).leavesQty(600).avgPx(185.48).build());
        Result result = machine.process(execReport("D").execId("E2").execTransType("2").execRefId("E1")
                .ordStatus("1").lastShares(400).lastPx(185.52)
                .cumQty(400).leavesQty(600).avgPx(185.52).build());

        OrderState state = result.state();
        assertNotNull(state);
        assertEquals(400.0, state.cumQty());
        assertEquals(600.0, state.leavesQty());
        assertEquals(185.52, state.avgPx());
        List<ExecutionRow> corrected =
                result.executions().stream().filter(r -> r.execId().equals("E1")).toList();
        assertEquals(1, corrected.size());
        assertEquals(FillStatus.CORRECTED, corrected.get(0).fillStatus());
        assertEquals(185.52, corrected.get(0).lastPx());
        assertEquals(400.0, corrected.get(0).lastShares());
    }

    @Test
    @DisplayName("08: a DK on an unknown ExecID still attaches via the known OrderID")
    void dkOnUnknownExecIdButKnownOrderId() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        OrderState before = machine.getByClOrdId("C1");
        Result result = machine.process(dkTrade("E-UNKNOWN").orderId("ORD-1").dkReason("B").build());

        OrderState state = result.state();
        assertNotNull(before);
        assertNotNull(state);
        assertEquals("C1", state.orderKey());
        assertEquals("B", state.dkReason());
        assertEquals(before.cumQty(), state.cumQty());
        assertEquals(before.leavesQty(), state.leavesQty());
        assertEquals(before.avgPx(), state.avgPx());
        assertEquals(before.ordStatus(), state.ordStatus());
        assertEquals(before.execCount(), state.execCount(), "a DK is not an execution");
        assertEquals(1, result.executions().size());
        assertEquals("E-UNKNOWN", result.executions().get(0).execId());
        assertEquals(FillStatus.DK, result.executions().get(0).fillStatus());
        OrderState attached = machine.getByExecId("E-UNKNOWN");
        assertNotNull(attached);
        assertEquals("C1", attached.orderKey());
    }

    @Test
    @DisplayName("09: an execution before the new order, then a late D that merges")
    void executionBeforeNewOrderThenLateDMerges() {
        machine.process(execReport("0").execId("E0").orderId("ORD-1").clOrdId("C1").ordStatus("0")
                .symbol("IBM").side("1").qty(1000).cumQty(0).leavesQty(1000).avgPx(0).build());
        OrderState created = machine.getByOrderId("ORD-1");
        assertNotNull(created);
        assertEquals("ORD-1", created.orderKey());
        assertEquals(OrdStatus.NEW, created.ordStatus());

        Result result = machine.process(newOrder("C1").account("ACC9").qty(1000).price(185.50).build());

        OrderState state = result.state();
        assertNotNull(state);
        assertEquals("ORD-1", state.orderKey(), "venue key wins, a late D does not re-key");
        assertEquals(OrdStatus.NEW, state.ordStatus(), "status untouched");
        assertEquals(PendingAction.NONE, state.pendingAction(), "no PENDING_NEW resurrection");
        assertEquals(185.50, state.price(), "empty term filled from the D");
        assertEquals("ACC9", state.account());
        assertEquals("C1", state.rootClOrdId());
        assertEquals("C1", state.clOrdId());
        assertEquals(0.0, state.cumQty());
        assertEquals(1000.0, state.leavesQty());
        assertEquals(1, machine.orderCount());
    }

    @Test
    @DisplayName("10: a fill while PENDING_CANCEL applies quantities")
    void fillWhilePendingCancelAppliesQuantities() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        machine.process(cancelRequest("C2", "C1").build());
        Result result = machine.process(execReport("1").execId("E1").ordStatus("1")
                .lastShares(400).lastPx(185.48).cumQty(400).leavesQty(600).avgPx(185.48).build());

        OrderState state = result.state();
        assertNotNull(state);
        assertEquals(400.0, state.cumQty());
        assertEquals(600.0, state.leavesQty());
        assertEquals(185.48, state.avgPx());
        assertEquals(OrdStatus.PARTIALLY_FILLED, state.ordStatus(), "venue truth via tag 39");
        assertEquals(PendingAction.CANCEL, state.pendingAction());
        assertEquals("C2", state.pendingClOrdId());

        Result cancelled = machine.process(execReport("4").execId("E2").clOrdId("C2").origClOrdId("C1")
                .ordStatus("4").cumQty(400).leavesQty(0).avgPx(185.48).build());
        assertNotNull(cancelled.state());
        assertEquals(PendingAction.NONE, cancelled.state().pendingAction());
        assertEquals(OrdStatus.CANCELED, cancelled.state().ordStatus());
    }

    @Test
    @DisplayName("10b: a fill while PENDING_REPLACE keeps the request")
    void fillWhilePendingReplaceKeepsTheRequest() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        machine.process(replaceRequest("C2", "C1").qty(1500).price(185.55).build());
        Result result = machine.process(execReport("1").execId("E1").ordStatus("1")
                .lastShares(400).lastPx(185.48).cumQty(400).leavesQty(600).avgPx(185.48).build());

        OrderState state = result.state();
        assertNotNull(state);
        assertEquals(400.0, state.cumQty());
        assertEquals(OrdStatus.PARTIALLY_FILLED, state.ordStatus());
        assertEquals(PendingAction.REPLACE, state.pendingAction());
        assertEquals(1000.0, state.orderQty(), "staged terms still not applied");
    }

    @Test
    @DisplayName("11: an unsolicited cancel is accepted")
    void unsolicitedCancelIsAccepted() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        Result result = machine.process(
                execReport("4").execId("E1").ordStatus("4").cumQty(0).leavesQty(0).avgPx(0).build());

        OrderState state = result.state();
        assertNotNull(state);
        assertEquals(OrdStatus.CANCELED, state.ordStatus());
        assertTrue(state.terminal());
        assertEquals(PendingAction.NONE, state.pendingAction());
    }

    @Test
    @DisplayName("12: a stale lower-CumQty report is ignored economically")
    void staleLowerCumQtyReportIsIgnoredEconomically() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                .cumQty(400).leavesQty(600).avgPx(185.48).build());
        machine.process(execReport("2").execId("E2").lastShares(600).lastPx(185.55)
                .cumQty(1000).leavesQty(0).avgPx(185.522).build());
        OrderState before = machine.getByClOrdId("C1");
        Result result = machine.process(execReport("1").execId("E3").ordStatus("1")
                .lastShares(100).lastPx(180.00).cumQty(100).leavesQty(900).avgPx(180.00).build());

        OrderState state = result.state();
        assertNotNull(before);
        assertNotNull(state);
        assertEquals(1000.0, state.cumQty());
        assertEquals(0.0, state.leavesQty());
        assertEquals(185.522, state.avgPx());
        assertEquals(600.0, state.lastShares());
        assertEquals(185.55, state.lastPx());
        assertEquals(before.msgCount() + 1, state.msgCount());
        assertEquals(before.execCount() + 1, state.execCount(), "the ExecID is still recorded");
    }
}
