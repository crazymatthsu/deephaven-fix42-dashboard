package com.fix42.dashboard.fixcache;

import static com.fix42.dashboard.fixcache.FixTestMessages.buildFix;
import static com.fix42.dashboard.fixcache.FixTestMessages.cancelReject;
import static com.fix42.dashboard.fixcache.FixTestMessages.dkTrade;
import static com.fix42.dashboard.fixcache.FixTestMessages.execReport;
import static com.fix42.dashboard.fixcache.FixTestMessages.newOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Lookups, error handling and the row-emission contract (doc 01 sections 3/6, doc 05 section 3).
 *
 * <p>Mirrors {@code deephaven-scripts/tests/test_state_machine_lookups.py}.
 */
class StateMachineLookupsTest {

    private OrderStateMachine machine;

    @BeforeEach
    void setUp() {
        machine = new OrderStateMachine(new FakeClock());
    }

    private void twoOrders() {
        machine.process(newOrder("C1").account("ACC1").symbol("IBM").build());
        machine.process(execReport("0").execId("E1").clOrdId("C1").orderId("ORD-1")
                .cumQty(0).leavesQty(1000).build());
        machine.process(newOrder("D1").account("ACC2").symbol("MSFT").build());
        machine.process(execReport("0").execId("E2").clOrdId("D1").orderId("ORD-2")
                .symbol("MSFT").cumQty(0).leavesQty(1000).build());
    }

    // ------------------------------------------------------------------ lookups

    @Test
    void lookupsByEveryIdentifier() {
        twoOrders();

        assertEquals("C1", machine.getByClOrdId("C1").orderKey());
        assertEquals("C1", machine.getByOrderId("ORD-1").orderKey());
        assertEquals("C1", machine.getByExecId("E1").orderKey());
        assertEquals("C1", machine.getByKey("C1").orderKey());
        assertEquals("D1", machine.getByOrderId("ORD-2").orderKey());
    }

    @Test
    void lookupsReturnNullForUnknownIdentifiers() {
        twoOrders();

        assertNull(machine.getByClOrdId("NOPE"));
        assertNull(machine.getByOrderId("NOPE"));
        assertNull(machine.getByExecId("NOPE"));
        assertNull(machine.getByKey(""));
    }

    @Test
    void lookupsReturnSnapshotsNotLiveObjects() {
        twoOrders();
        OrderState first = machine.getByClOrdId("C1");
        first.cumQty = 999.0;
        assertEquals(0.0, machine.getByClOrdId("C1").cumQty());
    }

    @Test
    void findByAccountAndSymbol() {
        twoOrders();

        assertEquals(List.of("C1"), machine.findByAccount("ACC1").stream().map(OrderState::orderKey).toList());
        assertEquals(List.of("D1"), machine.findByAccount("ACC2").stream().map(OrderState::orderKey).toList());
        assertEquals(List.of(), machine.findByAccount("ACC3"));
        assertEquals(List.of("C1"), machine.findBySymbol("IBM").stream().map(OrderState::orderKey).toList());
        assertEquals(List.of("D1"), machine.findBySymbol("MSFT").stream().map(OrderState::orderKey).toList());
    }

    @Test
    void orderCountAndSnapshotAll() {
        assertEquals(0, machine.orderCount());
        twoOrders();
        assertEquals(2, machine.orderCount());
        assertEquals(
                List.of("C1", "D1"),
                machine.snapshotAll().stream().map(OrderState::orderKey).sorted().toList());
    }

    @Test
    void bindingIsIdempotentAcrossReplays() {
        String rawNew = newOrder("C1").build();
        String rawAck = execReport("0").execId("E1").clOrdId("C1").cumQty(0).leavesQty(1000).build();
        for (int i = 0; i < 3; i++) {
            machine.process(rawNew);
            machine.process(rawAck);
        }

        assertEquals(1, machine.orderCount());
        assertEquals(Map.of("C1", "C1"), machine.keyByClOrdId());
        assertEquals(Map.of("ORD-1", "C1"), machine.keyByOrderId());
        assertEquals(Map.of("E1", "C1"), machine.keyByExecId());
    }

    @Test
    @DisplayName("a 9 may carry 37=NONE when the target was never acked (doc 01 section 2)")
    void orderIdNoneSentinelIsNeverBound() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E1").cumQty(0).leavesQty(1000).build());
        Result result = machine.process(
                cancelReject("C2", "C1").responseTo("1").orderId("NONE").cxlRejReason("1").build());

        assertNull(result.error());
        assertEquals("C1", result.state().orderKey());
        assertEquals("ORD-1", result.state().orderId());
        assertFalse(machine.keyByOrderId().containsKey("NONE"));
    }

    // ------------------------------------------------------------------ errors

    @Test
    void unknownMsgTypeSetsErrorAndChangesNothing() {
        machine.process(newOrder("C1").build());
        OrderState before = machine.getByClOrdId("C1");
        Result result = machine.process(buildFix("V", fields(11, "C1", 55, "IBM")));

        assertNotNull(result.error());
        assertTrue(result.error().contains("MsgType"));
        assertNull(result.state());
        assertEquals(List.of(), result.executions());
        assertEquals(List.of(), result.events());
        assertNotNull(result.message());
        assertEquals("C1", result.message().orderKey(), "still attributed for the audit table");
        assertEquals("V", result.message().msgType());
        assertEquals(before.msgCount(), machine.getByClOrdId("C1").msgCount());
    }

    @Test
    void missingMsgTypeSetsError() {
        Result result = machine.process("11=C1|55=IBM|");
        assertNotNull(result.error());
        assertTrue(result.error().contains("MsgType"));
        assertNull(result.state());
        assertNotNull(result.message());
        assertEquals("", result.message().msgType());
    }

    @Test
    void unparseableInputSetsErrorWithoutRaising() {
        String twoSoh = String.valueOf(FixParser.SOH) + FixParser.SOH;
        for (String raw : List.of("", "not a fix message", "||||", twoSoh)) {
            Result result = machine.process(raw);
            assertNotNull(result.error(), raw);
            assertNull(result.state(), raw);
            assertNull(result.message(), raw);
        }
    }

    @Test
    void unresolvableMessageStillProducesAnAuditRow() {
        Result result = machine.process(dkTrade("E-UNKNOWN").orderId(null).build());

        assertNotNull(result.error());
        assertTrue(result.error().contains("unresolvable"));
        assertNull(result.state());
        assertNotNull(result.message());
        assertEquals("", result.message().orderKey());
        assertEquals("Q", result.message().msgType());
        assertEquals(0, machine.orderCount());
    }

    @Test
    void processNeverRaisesOnMalformedValues() {
        machine.process(newOrder("C1").build());
        Result result = machine.process(buildFix(
                "8",
                fields(37, "ORD-1", 11, "C1", 17, "E1", 150, "0", 39, "0",
                        14, "not-a-number", 151, "", 6, "x", 60, "garbage")));

        assertNull(result.error());
        assertNotNull(result.state());
        assertEquals(0.0, result.state().cumQty());
        assertNull(result.message().transactTime());
    }

    @Test
    void unknownEnumCodesDegradeToUnknown() {
        machine.process(newOrder("C1").build());
        Result result = machine.process(buildFix(
                "8",
                fields(37, "ORD-1", 11, "C1", 17, "E1", 150, "Z", 39, "Z", 14, 0, 151, 1000, 6, 0)));

        assertNull(result.error());
        assertEquals("UNKNOWN", result.state().toRow().get("OrdStatus"));
        assertEquals("UNKNOWN", result.state().toRow().get("LastExecType"));
        assertEquals("STATUS", result.events().get(0).eventType());
    }

    @Test
    void processFieldsAcceptsPreParsedInput() {
        String raw = newOrder("C1").build();
        Result result = machine.processFields(FixParser.parseFix(raw), raw);

        assertNull(result.error());
        assertEquals("C1", result.state().orderKey());
        assertNotNull(result.message());
        assertTrue(result.message().rawFix().startsWith("8=FIX.4.2|"));
        assertNotNull(result.message().ingestTs());
    }

    @Test
    void sohAndPipeDelimitedInputsBehaveIdentically() {
        OrderStateMachine sohMachine = new OrderStateMachine(new FakeClock());
        OrderStateMachine pipeMachine = new OrderStateMachine(new FakeClock());
        Result sohResult = sohMachine.process(newOrder("C1").delimiter(FixParser.SOH).build());
        Result pipeResult = pipeMachine.process(newOrder("C1").build());

        var sohRow = sohResult.state().toRow();
        var pipeRow = pipeResult.state().toRow();
        for (String column : Columns.ORDER_STATE) {
            if (column.equals("FirstSeenTs") || column.equals("LastUpdateTs")) {
                continue;
            }
            assertEquals(sohRow.get(column), pipeRow.get(column), column);
        }
        assertEquals(sohResult.message().rawFix(), pipeResult.message().rawFix());
    }

    // ------------------------------------------------------------------ emission contract

    @Test
    void everyMessageProducesExactlyOneAuditRow() {
        List<Result> results = List.of(
                machine.process(newOrder("C1").build()),
                machine.process(execReport("0").execId("E1").cumQty(0).leavesQty(1000).build()),
                machine.process(dkTrade("E1").build()));
        for (Result result : results) {
            assertNotNull(result.message());
            assertEquals(Columns.MESSAGE, List.copyOf(result.message().toRow().keySet()));
        }
    }

    @Test
    void requestsEmitNoExecutionRows() {
        assertEquals(List.of(), machine.process(newOrder("C1").build()).executions());
    }

    @Test
    void executionAndDkRowsUseTheFrozenColumns() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E1").cumQty(0).leavesQty(1000).build());
        Result dk = machine.process(dkTrade("E1").build());

        var row = dk.executions().get(0).toRow();
        assertEquals(Columns.EXECUTION, List.copyOf(row.keySet()));
        assertEquals(FillStatus.DK, row.get("FillStatus"));
    }

    @Test
    @DisplayName("a replayed original must not reset a busted ExecID back to NORMAL")
    void bustReemissionKeepsDispositionAcrossAReplay() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        String fill = execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                .cumQty(400).leavesQty(600).avgPx(185.48).build();
        machine.process(fill);
        machine.process(execReport("D").execId("E2").execTransType("1").execRefId("E1")
                .ordStatus("0").cumQty(0).leavesQty(1000).avgPx(0).build());
        Result replay = machine.process(fill);

        assertEquals(1, replay.executions().size());
        assertEquals("E1", replay.executions().get(0).execId());
        assertEquals(FillStatus.BUSTED, replay.executions().get(0).fillStatus());
    }

    @Test
    void bustOfAnUnseenExecIdStillEmitsABustedRow() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        Result result = machine.process(execReport("D").execId("E9").execTransType("1")
                .execRefId("E-UNSEEN").ordStatus("0").cumQty(0).leavesQty(1000).avgPx(0).build());

        assertEquals(List.of("E9", "E-UNSEEN"), result.executions().stream().map(ExecutionRow::execId).toList());
        assertEquals(FillStatus.BUSTED, result.executions().get(1).fillStatus());
        assertEquals(FillStatus.NORMAL, result.executions().get(0).fillStatus());
    }

    @Test
    void dkThenCorrectShowsTheLatestDisposition() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                .cumQty(400).leavesQty(600).avgPx(185.48).build());
        machine.process(dkTrade("E1").build());
        Result corrected = machine.process(execReport("D").execId("E2").execTransType("2").execRefId("E1")
                .ordStatus("1").lastShares(400).lastPx(185.49)
                .cumQty(400).leavesQty(600).avgPx(185.49).build());

        ExecutionRow reemitted = corrected.executions().get(1);
        assertEquals("E1", reemitted.execId());
        assertEquals(FillStatus.CORRECTED, reemitted.fillStatus());
        assertEquals(185.49, reemitted.lastPx());
    }

    @Test
    void isFillIsFalseForBustAndCorrectReports() {
        machine.process(newOrder("C1").build());
        machine.process(execReport("0").execId("E0").cumQty(0).leavesQty(1000).build());
        machine.process(execReport("1").execId("E1").lastShares(400).lastPx(185.48)
                .cumQty(400).leavesQty(600).avgPx(185.48).build());
        Result bust = machine.process(execReport("1").execId("E2").execTransType("1").execRefId("E1")
                .ordStatus("0").lastShares(400).lastPx(185.48)
                .cumQty(0).leavesQty(1000).avgPx(0).build());

        assertFalse(bust.executions().get(0).isFill(), "150=1 but 20=1");
        assertEquals(EventType.FILL_BUST, bust.events().get(0).eventType(), "ExecTransType wins over 150");
    }

    /** Builds an ordered tag map from alternating tag/value arguments. */
    private static Map<Integer, Object> fields(Object... pairs) {
        Map<Integer, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((Integer) pairs[i], pairs[i + 1]);
        }
        return map;
    }
}
