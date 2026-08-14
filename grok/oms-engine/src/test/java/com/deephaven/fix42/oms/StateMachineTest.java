package com.deephaven.fix42.oms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateMachineTest {
    private InMemoryOmsCache cache;

    @BeforeEach
    void setUp() {
        cache = new InMemoryOmsCache(CacheConfig.testing());
    }

    @Test
    void newOrderAckAndReject() {
        cache.ingest(FixTape.d("C1", "PROP", "MSFT", "1000", "420"));
        OrderState pending = cache.getByClOrdId("C1").orElseThrow();
        assertEquals("C1", pending.getOrderKey());
        assertEquals("A", pending.getOrdStatus());
        assertEquals(1000.0, pending.getLeavesQty());

        cache.ingest(FixTape.er("C1", "O9", "E1", "0", "0", "0", "1000"));
        OrderState acked = cache.getByClOrdId("C1").orElseThrow();
        assertEquals("O9", acked.getOrderKey());
        assertEquals("0", acked.getOrdStatus());
        assertEquals(acked.getOrderKey(), cache.getByOrderId("O9").orElseThrow().getOrderKey());

        cache.ingest(FixTape.d("R1", "PROP", "IBM", "10", "1"));
        cache.ingest(FixTape.er("R1", "OR", "ER", "8", "8", "0", "0", "103=1", "58=rejected"));
        OrderState rejected = cache.getByClOrdId("R1").orElseThrow();
        assertEquals("8", rejected.getOrdStatus());
        assertEquals("1", rejected.getOrdRejReason());
        assertTrue(rejected.isTerminal());
    }

    @Test
    void amendAckAndReject() {
        seedWorking("C1", "O9");
        cache.ingest(FixTape.msg(
                "G",
                "11=C2",
                "41=C1",
                "37=O9",
                "55=MSFT",
                "54=1",
                "38=800",
                "40=2",
                "44=421",
                "60=20260814-10:01:00"));
        OrderState pending = cache.getByClOrdId("C2").orElseThrow();
        assertTrue(pending.isPendingReplace());
        assertEquals("E", pending.getOrdStatus());
        assertEquals(800.0, pending.getOrderQty());

        cache.ingest(FixTape.er("C2", "O9", "E3", "5", "0", "0", "800", "41=C1", "44=421", "38=800"));
        OrderState replaced = cache.getByClOrdId("C1").orElseThrow();
        assertEquals("C2", replaced.getClOrdId());
        assertFalse(replaced.isPendingReplace());
        assertEquals("0", replaced.getOrdStatus());
        assertEquals(replaced.getOrderKey(), cache.getByClOrdId("C2").orElseThrow().getOrderKey());

        cache.ingest(FixTape.msg(
                "G",
                "11=C3",
                "41=C2",
                "37=O9",
                "55=MSFT",
                "54=1",
                "38=700",
                "40=2",
                "44=422",
                "60=20260814-10:02:00"));
        cache.ingest(FixTape.msg(
                "9",
                "11=C3",
                "41=C2",
                "37=O9",
                "39=0",
                "434=2",
                "102=0",
                "58=too late"));
        OrderState afterReject = cache.getByOrderId("O9").orElseThrow();
        assertFalse(afterReject.isPendingReplace());
        assertEquals("0", afterReject.getOrdStatus());
        assertEquals("2", afterReject.getCxlRejResponseTo());
    }

    @Test
    void cancelAckAndReject() {
        seedWorking("C1", "O9");
        cache.ingest(FixTape.msg("F", "11=CX", "41=C1", "37=O9", "55=MSFT", "54=1", "60=20260814-10:03:00"));
        assertTrue(cache.getByOrderId("O9").orElseThrow().isPendingCancel());
        assertEquals("6", cache.getByOrderId("O9").orElseThrow().getOrdStatus());

        cache.ingest(FixTape.er("CX", "O9", "EC", "4", "4", "0", "0", "41=C1"));
        assertEquals("4", cache.getByOrderId("O9").orElseThrow().getOrdStatus());
        assertTrue(cache.getByOrderId("O9").orElseThrow().isTerminal());

        InMemoryOmsCache other = new InMemoryOmsCache(CacheConfig.testing());
        other.ingest(FixTape.d("C1", "PROP", "MSFT", "1000", "420"));
        other.ingest(FixTape.er("C1", "O9", "E1", "0", "0", "0", "1000"));
        other.ingest(FixTape.msg("F", "11=CX", "41=C1", "37=O9", "55=MSFT", "54=1", "60=20260814-10:03:00"));
        other.ingest(FixTape.msg("9", "11=CX", "41=C1", "37=O9", "39=0", "434=1", "102=0"));
        assertFalse(other.getByOrderId("O9").orElseThrow().isPendingCancel());
        assertEquals("0", other.getByOrderId("O9").orElseThrow().getOrdStatus());
    }

    @Test
    void fillsBustAndCorrect() {
        seedWorking("C1", "O9");
        cache.ingest(FixTape.er("C1", "O9", "E2", "1", "1", "400", "600", "32=400", "31=420", "6=420"));
        OrderState partial = cache.getByOrderId("O9").orElseThrow();
        assertEquals("1", partial.getOrdStatus());
        assertEquals(400.0, partial.getCumQty());
        assertEquals(600.0, partial.getLeavesQty());
        assertEquals(400.0, partial.getLastQty());

        cache.ingest(FixTape.er("C1", "O9", "E3", "2", "2", "1000", "0", "32=600", "31=421", "6=420.6"));
        assertEquals("2", cache.getByOrderId("O9").orElseThrow().getOrdStatus());
        assertEquals(1000.0, cache.getByOrderId("O9").orElseThrow().getCumQty());

        cache.ingest(FixTape.msg(
                "8",
                "11=C1",
                "37=O9",
                "17=E4",
                "19=E3",
                "20=1",
                "150=1",
                "39=1",
                "14=400",
                "151=600",
                "32=600",
                "31=421",
                "6=420",
                "60=20260814-10:05:00"));
        OrderState busted = cache.getByOrderId("O9").orElseThrow();
        assertEquals(400.0, busted.getCumQty());
        assertEquals("1", busted.getOrdStatus());
        assertEquals("1", busted.getExecTransType());

        cache.ingest(FixTape.msg(
                "8",
                "11=C1",
                "37=O9",
                "17=E5",
                "19=E2",
                "20=2",
                "150=1",
                "39=1",
                "14=350",
                "151=650",
                "32=350",
                "31=419",
                "6=419",
                "60=20260814-10:06:00"));
        assertEquals(350.0, cache.getByOrderId("O9").orElseThrow().getCumQty());
        assertEquals("2", cache.getByOrderId("O9").orElseThrow().getExecTransType());
        assertEquals(cache.getByExecId("E2").orElseThrow().getOrderKey(), "O9");
    }

    @Test
    void dontKnowDoesNotUnwindQty() {
        seedWorking("C1", "O9");
        cache.ingest(FixTape.er("C1", "O9", "E2", "1", "1", "400", "600", "32=400", "31=420", "6=420"));
        cache.ingest(FixTape.msg("Q", "37=O9", "17=E2", "127=D", "55=MSFT", "54=1", "58=unknown fill"));
        OrderState state = cache.getByExecId("E2").orElseThrow();
        assertTrue(state.isDkTrade());
        assertEquals("D", state.getDkReason());
        assertEquals(400.0, state.getCumQty());
    }

    @Test
    void duplicateExecIdDoesNotDoubleCount() {
        seedWorking("C1", "O9");
        String fill = FixTape.er("C1", "O9", "E2", "1", "1", "400", "600", "32=400", "31=420", "6=420");
        cache.ingest(fill);
        ProcessResult dup = cache.ingest(fill);
        assertFalse(dup.isApplied());
        assertEquals(400.0, cache.getByOrderId("O9").orElseThrow().getCumQty());
    }

    @Test
    void staleExecReportIgnored() {
        seedWorking("C1", "O9");
        cache.ingest(FixTape.er("C1", "O9", "E2", "1", "1", "400", "600", "32=400", "31=420", "60=20260814-10:10:00"));
        ProcessResult stale = cache.ingest(FixTape.er(
                "C1", "O9", "E0", "0", "0", "0", "1000", "60=20260814-10:00:00"));
        assertFalse(stale.isApplied());
        assertEquals(400.0, cache.getByOrderId("O9").orElseThrow().getCumQty());
        assertEquals("1", cache.getByOrderId("O9").orElseThrow().getOrdStatus());
    }

    @Test
    void missingNewStillCreatesFromEr() {
        cache.ingest(FixTape.er("C1", "O9", "E1", "0", "0", "0", "500", "55=IBM", "1=ACCT", "38=500"));
        OrderState state = cache.getByOrderId("O9").orElseThrow();
        assertEquals("IBM", state.getSymbol());
        assertEquals("ACCT", state.getAccount());
        assertEquals(500.0, state.getOrderQty());
    }

    @Test
    void blankFieldsDoNotWipe() {
        cache.ingest(FixTape.d("C1", "PROP", "MSFT", "1000", "420"));
        cache.ingest(FixTape.er("C1", "O9", "E1", "0", "0", "0", "1000"));
        assertEquals("PROP", cache.getByOrderId("O9").orElseThrow().getAccount());
        assertEquals(420.0, cache.getByOrderId("O9").orElseThrow().getPrice());
    }

    @Test
    void lookupsAndParentRollup() {
        cache.ingest(FixTape.msg(
                "D",
                "11=P1",
                "1=PROP",
                "55=MSFT",
                "54=1",
                "38=1000",
                "40=2",
                "44=420"));
        cache.ingest(FixTape.msg(
                "D",
                "11=C1",
                "1=PROP",
                "55=MSFT",
                "54=1",
                "38=400",
                "40=2",
                "44=420",
                "20001=P1",
                "20002=P1"));
        cache.ingest(FixTape.er("C1", "B1", "E1", "0", "0", "0", "400"));
        cache.ingest(FixTape.msg(
                "D",
                "11=C2",
                "1=PROP",
                "55=MSFT",
                "54=1",
                "38=600",
                "40=2",
                "44=420",
                "20001=P1"));
        cache.ingest(FixTape.er("C2", "B2", "E2", "2", "2", "600", "0", "32=600", "31=420", "6=420"));

        assertEquals(1, cache.findByAccount("PROP").stream().filter(s -> "P1".equals(s.getOrderKey())).count());
        assertEquals(3, cache.findBySymbol("MSFT").size());
        assertEquals(2, cache.getChildren("P1").size());
        ChildRollup rollup = cache.rollup("P1");
        assertEquals(2, rollup.getChildCount());
        assertEquals(1000.0, rollup.getOrderQty());
        assertEquals(600.0, rollup.getCumQty());
        assertEquals("P1", cache.getParent("B1").orElseThrow().getOrderKey());
        assertFalse(cache.getHistory("B1").isEmpty());
    }

    @Test
    void statusRequestDoesNotMutate() {
        seedWorking("C1", "O9");
        ProcessResult result = cache.ingest(FixTape.msg("H", "11=C1", "55=MSFT", "54=1"));
        assertFalse(result.isApplied());
        assertEquals("0", cache.getByOrderId("O9").orElseThrow().getOrdStatus());
    }

    @Test
    void unknownTypeAndUnidentifiable() {
        assertThrows(UnsupportedMessageTypeException.class, () -> cache.ingest("8=FIX.4.2|35=A|10=000|"));
        assertThrows(UnidentifiableOrderException.class, () -> cache.ingest("8=FIX.4.2|35=8|17=E1|150=0|39=0|10=000|"));
    }

    private void seedWorking(String cl, String orderId) {
        cache.ingest(FixTape.d(cl, "PROP", "MSFT", "1000", "420"));
        cache.ingest(FixTape.er(cl, orderId, "E1", "0", "0", "0", "1000"));
    }
}
