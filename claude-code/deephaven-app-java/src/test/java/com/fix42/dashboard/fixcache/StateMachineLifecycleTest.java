package com.fix42.dashboard.fixcache;

import static com.fix42.dashboard.fixcache.FixTestMessages.execReport;
import static com.fix42.dashboard.fixcache.FixTestMessages.newOrder;
import static com.fix42.dashboard.fixcache.FixTestMessages.replaceRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fix42.dashboard.fixcache.FixEnums.ExecType;
import com.fix42.dashboard.fixcache.FixEnums.OrdStatus;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The reference worked lifecycle from the spec (doc 05 section 3.1).
 *
 * <pre>
 * D(C1, 1000 IBM @ 185.50 limit)
 *   -&gt; 8(150=A) -&gt; 8(150=0)
 *   -&gt; 8(150=1, 32=400 @ 185.48, 14=400, 151=600, 6=185.48)
 *   -&gt; G(C2, price 185.55) -&gt; 8(150=E) -&gt; 8(150=5, 39=1)
 *   -&gt; 8(150=2, 32=600 @ 185.55, 14=1000, 151=0, 6=185.522)
 * </pre>
 *
 * <p>Mirrors {@code deephaven-scripts/tests/test_state_machine_lifecycle.py}.
 */
class StateMachineLifecycleTest {

    private static final List<String> EXEC_IDS =
            List.of("EXEC-1", "EXEC-2", "EXEC-3", "EXEC-4", "EXEC-5", "EXEC-6");

    private OrderStateMachine machine;
    private List<Result> results;

    @BeforeEach
    void runReferenceLifecycle() {
        machine = new OrderStateMachine(new FakeClock());
        List<String> messages = List.of(
                newOrder("C1").symbol("IBM").side("1").qty(1000).ordType("2").price(185.50).seq(1).build(),
                execReport("A").execId("EXEC-1").clOrdId("C1").orderId("ORD-1")
                        .cumQty(0).leavesQty(1000).avgPx(0).seq(2).build(),
                execReport("0").execId("EXEC-2").clOrdId("C1").orderId("ORD-1")
                        .cumQty(0).leavesQty(1000).avgPx(0).seq(3).build(),
                execReport("1").execId("EXEC-3").clOrdId("C1").orderId("ORD-1")
                        .lastShares(400).lastPx(185.48).lastMkt("XNAS")
                        .cumQty(400).leavesQty(600).avgPx(185.48).seq(4).build(),
                replaceRequest("C2", "C1").qty(1000).price(185.55).seq(5).build(),
                execReport("E").execId("EXEC-4").clOrdId("C2").origClOrdId("C1").orderId("ORD-1")
                        .cumQty(400).leavesQty(600).avgPx(185.48).seq(6).build(),
                execReport("5").execId("EXEC-5").clOrdId("C2").origClOrdId("C1").orderId("ORD-1")
                        .ordStatus("1").cumQty(400).leavesQty(600).avgPx(185.48).seq(7).build(),
                execReport("2").execId("EXEC-6").clOrdId("C2").origClOrdId("C1").orderId("ORD-1")
                        .lastShares(600).lastPx(185.55).lastMkt("XNAS")
                        .cumQty(1000).leavesQty(0).avgPx(185.522).seq(8).build());

        results = new ArrayList<>();
        for (String raw : messages) {
            results.add(machine.process(raw));
        }
    }

    @Test
    void hasNoErrors() {
        for (Result result : results) {
            assertNotNull(result.state());
            assertNotNull(result.message());
            assertEquals(null, result.error());
        }
        assertEquals(8, results.size());
    }

    @Test
    void finalState() {
        OrderState state = results.get(results.size() - 1).state();
        assertNotNull(state);
        var row = state.toRow();

        assertEquals("FILLED", row.get("OrdStatus"));
        assertEquals("C2", row.get("ClOrdID"));
        assertEquals("C1", row.get("RootClOrdID"));
        assertEquals("C1,C2", row.get("ClOrdIDChain"));
        assertEquals(1000.0, row.get("CumQty"));
        assertEquals(0.0, row.get("LeavesQty"));
        assertEquals(185.522, row.get("AvgPx"));
        assertEquals(185.55, row.get("Price"));
        assertEquals(1000.0, row.get("OrderQty"));
        assertEquals("ORD-1", row.get("OrderID"));
        assertEquals("C1", row.get("OrigClOrdID"));
        assertEquals("IBM", row.get("Symbol"));
        assertEquals("BUY", row.get("Side"));
        assertEquals("LIMIT", row.get("OrdType"));
        assertEquals(600.0, row.get("LastShares"));
        assertEquals(185.55, row.get("LastPx"));
        assertEquals("XNAS", row.get("LastMkt"));
        assertEquals("FILL", row.get("LastExecType"));
        assertEquals(PendingAction.NONE, row.get("PendingAction"));
        assertEquals("", row.get("PendingClOrdID"));
        assertEquals(6L, row.get("ExecCount"));
        assertEquals(8L, row.get("MsgCount"));
        assertEquals("8", row.get("LastMsgType"));
        assertEquals(Boolean.TRUE, row.get("Terminal"));
        assertTrue(state.terminal());
    }

    @Test
    @DisplayName("(400 * 185.48 + 600 * 185.55) / 1000 == 185.522, adopted from tag 6")
    void avgPxMatchesTheVenueSnapshot() {
        OrderState state = results.get(results.size() - 1).state();
        assertNotNull(state);
        assertEquals((400 * 185.48 + 600 * 185.55) / 1000, state.avgPx(), 1e-9);
    }

    @Test
    void everyIdentifierResolvesToOneOrderKey() {
        String orderKey = results.get(results.size() - 1).state().orderKey();
        assertEquals("C1", orderKey, "the D created the chain before any OrderID existed");

        for (String clOrdId : List.of("C1", "C2")) {
            OrderState found = machine.getByClOrdId(clOrdId);
            assertNotNull(found);
            assertEquals(orderKey, found.orderKey());
        }
        OrderState byOrderId = machine.getByOrderId("ORD-1");
        assertNotNull(byOrderId);
        assertEquals(orderKey, byOrderId.orderKey());
        for (String execId : EXEC_IDS) {
            OrderState found = machine.getByExecId(execId);
            assertNotNull(found);
            assertEquals(orderKey, found.orderKey());
        }
        assertEquals(1, machine.orderCount());
    }

    @Test
    void statusProgression() {
        assertEquals(
                List.of(
                        OrdStatus.PENDING_NEW,
                        OrdStatus.PENDING_NEW,
                        OrdStatus.NEW,
                        OrdStatus.PARTIALLY_FILLED,
                        OrdStatus.PENDING_REPLACE,
                        OrdStatus.PENDING_REPLACE,
                        OrdStatus.PARTIALLY_FILLED, // 150=5 with 39=1: tag 39 wins
                        OrdStatus.FILLED),
                results.stream().map(r -> r.state().ordStatus()).toList());
    }

    @Test
    void eventStream() {
        List<OrderEventRow> events = results.stream().flatMap(r -> r.events().stream()).toList();
        assertEquals(
                List.of(
                        EventType.NEW_REQUEST,
                        EventType.PENDING_NEW,
                        EventType.NEW_ACK,
                        EventType.PARTIAL_FILL,
                        EventType.AMEND_REQUEST,
                        EventType.PENDING_AMEND,
                        EventType.AMEND_ACK,
                        EventType.FULL_FILL),
                events.stream().map(OrderEventRow::eventType).toList());

        OrderEventRow amendRequest = events.get(4);
        assertEquals("G", amendRequest.msgType());
        assertEquals("C2", amendRequest.clOrdId());
        assertEquals("C1", amendRequest.origClOrdId());
        assertEquals(185.55, amendRequest.price(), "proposed terms, not yet live");
        for (OrderEventRow event : events) {
            assertEquals("C1", event.orderKey());
            assertNotNull(event.ingestTs());
        }
    }

    @Test
    void executionStream() {
        List<ExecutionRow> executions = results.stream().flatMap(r -> r.executions().stream()).toList();

        assertEquals(EXEC_IDS, executions.stream().map(ExecutionRow::execId).toList());
        assertEquals(
                List.of(false, false, true, false, false, true),
                executions.stream().map(ExecutionRow::isFill).toList());
        for (ExecutionRow row : executions) {
            assertEquals(FillStatus.NORMAL, row.fillStatus());
            assertEquals("C1", row.orderKey());
        }

        ExecutionRow partial = executions.get(2);
        assertEquals(ExecType.PARTIAL_FILL, partial.execType());
        assertEquals(400.0, partial.lastShares());
        assertEquals(185.48, partial.lastPx());
        assertEquals(400.0, partial.cumQty());
        assertEquals(600.0, partial.leavesQty());
        assertEquals("PARTIALLY_FILLED", partial.toRow().get("OrdStatus"));

        ExecutionRow last = executions.get(executions.size() - 1);
        assertEquals(1000.0, last.cumQty());
        assertEquals(0.0, last.leavesQty());
        assertEquals(185.522, last.avgPx());
        assertEquals("C2", last.clOrdId());
        assertNotNull(last.transactTime());
    }

    @Test
    void messageAuditRows() {
        List<MessageRow> messages = results.stream().map(Result::message).toList();

        assertEquals(
                List.of("D", "8", "8", "8", "G", "8", "8", "8"),
                messages.stream().map(MessageRow::msgType).toList());
        for (MessageRow message : messages) {
            assertEquals("C1", message.orderKey());
            assertEquals(Boolean.TRUE, message.checksumOk());
            assertFalse(message.rawFix().indexOf(FixParser.SOH) >= 0);
        }
        assertEquals(
                List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L),
                messages.stream().map(MessageRow::seqNum).toList());
        assertTrue(messages.get(0).rawFix().startsWith("8=FIX.4.2|"));
        assertEquals(1000.0, messages.get(messages.size() - 1).toRow().get("CumQty"));
    }

    @Test
    void stateSnapshotsAreIndependentCopies() {
        OrderState early = results.get(3).state();
        assertNotNull(early);
        assertEquals(400.0, early.cumQty(), "not mutated by the later full fill");

        early.cumQty = -1.0;
        early.clOrdIdChain.add("MUTATED");
        OrderState live = machine.getByClOrdId("C1");
        assertNotNull(live);
        assertEquals(1000.0, live.cumQty());
        assertEquals(List.of("C1", "C2"), live.clOrdIdChain());
    }
}
