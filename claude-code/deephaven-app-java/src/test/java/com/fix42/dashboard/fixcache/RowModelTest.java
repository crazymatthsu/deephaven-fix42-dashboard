package com.fix42.dashboard.fixcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fix42.dashboard.fixcache.FixEnums.ExecTransType;
import com.fix42.dashboard.fixcache.FixEnums.ExecType;
import com.fix42.dashboard.fixcache.FixEnums.OrdStatus;
import com.fix42.dashboard.fixcache.FixEnums.OrdType;
import com.fix42.dashboard.fixcache.FixEnums.Side;
import com.fix42.dashboard.fixcache.FixEnums.TimeInForce;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The published row models and their frozen column contracts (doc 01 sections 4 and 6).
 *
 * <p>Mirrors {@code deephaven-scripts/tests/test_model.py}.
 */
class RowModelTest {

    @Test
    void orderStateRowUsesExactFrozenColumns() {
        OrderState state = new OrderState("K1");
        assertEquals(Columns.ORDER_STATE, List.copyOf(state.toRow().keySet()));
    }

    @Test
    void orderStateRowDefaults() {
        var row = new OrderState("K1").toRow();
        assertEquals("K1", row.get("OrderKey"));
        assertEquals("", row.get("OrderID"));
        assertEquals("", row.get("ClOrdIDChain"));
        assertEquals("", row.get("Side"));
        assertEquals("", row.get("OrdStatus"));
        assertEquals(PendingAction.NONE, row.get("PendingAction"));
        assertEquals(0.0, row.get("OrderQty"));
        assertEquals(0.0, row.get("AvgPx"));
        assertEquals(0L, row.get("ExecCount"));
        assertEquals(0L, row.get("MsgCount"));
        assertNull(row.get("FirstSeenTs"));
        assertNull(row.get("LastUpdateTs"));
        assertEquals(Boolean.FALSE, row.get("Terminal"));
    }

    @Test
    void orderStateRendersEnumNamesAndJoinedChain() {
        OrderState state = new OrderState("K1");
        state.side = Side.SELL;
        state.ordType = OrdType.LIMIT;
        state.timeInForce = TimeInForce.GTC;
        state.ordStatus = OrdStatus.PARTIALLY_FILLED;
        state.lastExecType = ExecType.PARTIAL_FILL;
        state.clOrdIdChain = new java.util.ArrayList<>(List.of("C1", "C2", "C3"));

        var row = state.toRow();
        assertEquals("SELL", row.get("Side"));
        assertEquals("LIMIT", row.get("OrdType"));
        assertEquals("GTC", row.get("TimeInForce"));
        assertEquals("PARTIALLY_FILLED", row.get("OrdStatus"));
        assertEquals("PARTIAL_FILL", row.get("LastExecType"));
        assertEquals("C1,C2,C3", row.get("ClOrdIDChain"));
    }

    @Test
    void orderStateTerminalIsComputed() {
        OrderState state = new OrderState("K1");
        assertFalse(state.terminal());
        state.ordStatus = OrdStatus.PARTIALLY_FILLED;
        assertFalse(state.terminal());
        state.ordStatus = OrdStatus.FILLED;
        assertTrue(state.terminal());
        assertEquals(Boolean.TRUE, state.toRow().get("Terminal"));
    }

    @Test
    void orderStateCopyIsIndependent() {
        OrderState state = new OrderState("K1");
        state.clOrdIdChain.add("C1");
        OrderState copy = state.copy();
        copy.clOrdIdChain.add("C2");
        copy.cumQty = 99.0;

        assertEquals(List.of("C1"), state.clOrdIdChain());
        assertEquals(0.0, state.cumQty());
    }

    @Test
    void executionRowUsesExactFrozenColumns() {
        assertEquals(Columns.EXECUTION, List.copyOf(new ExecutionRow("K1").toRow().keySet()));
    }

    @Test
    void executionRowRendersEnumNames() {
        ExecutionRow row = new ExecutionRow("K1");
        row.execTransType = ExecTransType.CANCEL;
        row.execType = ExecType.FILL;
        row.ordStatus = OrdStatus.FILLED;
        row.fillStatus = FillStatus.BUSTED;

        var rendered = row.toRow();
        assertEquals("CANCEL", rendered.get("ExecTransType"));
        assertEquals("FILL", rendered.get("ExecType"));
        assertEquals("FILLED", rendered.get("OrdStatus"));
        assertEquals(FillStatus.BUSTED, rendered.get("FillStatus"));
        assertEquals(Boolean.FALSE, rendered.get("IsFill"));
    }

    @Test
    void orderEventRowUsesExactFrozenColumns() {
        assertEquals(Columns.ORDER_EVENT, List.copyOf(new OrderEventRow("K1").toRow().keySet()));
    }

    @Test
    void eventTypeNamesAreTheFrozenDocList() {
        assertEquals(21, EventType.ALL.size());
        assertTrue(EventType.ALL.containsAll(List.of(
                EventType.NEW_REQUEST, EventType.NEW_ACK, EventType.NEW_REJECT,
                EventType.AMEND_REQUEST, EventType.AMEND_ACK, EventType.AMEND_REJECT,
                EventType.CANCEL_REQUEST, EventType.CANCEL_ACK, EventType.CANCEL_REJECT,
                EventType.PENDING_NEW, EventType.PENDING_AMEND, EventType.PENDING_CANCEL,
                EventType.PARTIAL_FILL, EventType.FULL_FILL, EventType.FILL_BUST,
                EventType.FILL_CORRECT, EventType.DK_TRADE, EventType.RESTATED,
                EventType.STATUS, EventType.EXPIRED, EventType.DONE_FOR_DAY)));
        assertEquals(List.of("NORMAL", "BUSTED", "CORRECTED", "DK"), FillStatus.ALL);
        assertEquals(List.of("NONE", "NEW", "CANCEL", "REPLACE"), PendingAction.ALL);
    }

    @Test
    void messageRowUsesExactFrozenColumns() {
        assertEquals(Columns.MESSAGE, List.copyOf(new MessageRow("K1").toRow().keySet()));
    }

    @Test
    void messageRowFromFieldsTypesEveryDocTag() {
        String raw = FixTestMessages.execReport("1")
                .execId("E1")
                .orderId("ORD-1")
                .clOrdId("C1")
                .origClOrdId("C0")
                .execTransType("0")
                .lastShares(400)
                .lastPx(185.48)
                .lastMkt("XNAS")
                .cumQty(400)
                .leavesQty(600)
                .avgPx(185.48)
                .account("ACC1")
                .text("ok")
                .seq(7)
                .build();
        MessageRow row = MessageRow.fromFields(
                FixParser.parseFix(raw), "C1", raw, Instant.parse("2024-01-15T14:30:00Z"));
        Map<String, Object> rendered = row.toRow();

        assertEquals("C1", rendered.get("OrderKey"));
        assertEquals("8", rendered.get("MsgType"));
        assertEquals("PARTIAL_FILL", rendered.get("ExecType"));
        assertEquals("NEW", rendered.get("ExecTransType"));
        assertEquals("PARTIALLY_FILLED", rendered.get("OrdStatus"));
        assertEquals("BUY", rendered.get("Side"));
        assertEquals(400.0, rendered.get("CumQty"));
        assertEquals(600.0, rendered.get("LeavesQty"));
        assertEquals(185.48, rendered.get("AvgPx"));
        assertEquals("XNAS", rendered.get("LastMkt"));
        assertEquals("ok", rendered.get("Text"));
        assertEquals(7L, rendered.get("SeqNum"));
        assertEquals(Boolean.TRUE, rendered.get("ChecksumOk"));
        assertNotNull(rendered.get("TransactTime"));
        assertNotNull(rendered.get("SendingTime"));
        assertEquals(Instant.parse("2024-01-15T14:30:00Z"), rendered.get("IngestTs"));
        assertFalse(((String) rendered.get("RawFix")).indexOf(FixParser.SOH) >= 0);
    }

    @Test
    @DisplayName("absent numeric tags are null, not zero -- the audit table distinguishes them")
    void messageRowAbsentNumericTagsAreNull() {
        String raw = FixTestMessages.buildFix("D", Map.of(11, "C1"));
        var rendered = MessageRow.fromFields(FixParser.parseFix(raw), "C1", raw, null).toRow();

        assertNull(rendered.get("OrderQty"));
        assertNull(rendered.get("Price"));
        assertNull(rendered.get("CumQty"));
        assertNull(rendered.get("LeavesQty"));
        assertNull(rendered.get("AvgPx"));
        assertNull(rendered.get("LastShares"));
        assertNull(rendered.get("LastPx"));
        assertEquals("", rendered.get("Symbol"));
        assertEquals("", rendered.get("Side"));
    }

    @Test
    void messageRowRecordsBadChecksumWithoutRejecting() {
        String raw = "35=D|11=C1|10=001|";
        var rendered = MessageRow.fromFields(FixParser.parseFix(raw), "C1", raw, null).toRow();
        assertEquals(Boolean.FALSE, rendered.get("ChecksumOk"));
        assertEquals("C1", rendered.get("ClOrdID"));
    }
}
