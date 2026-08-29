package com.fix42.dashboard.fixcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fix42.dashboard.fixcache.FixEnums.CxlRejResponseTo;
import com.fix42.dashboard.fixcache.FixEnums.ExecTransType;
import com.fix42.dashboard.fixcache.FixEnums.ExecType;
import com.fix42.dashboard.fixcache.FixEnums.OrdStatus;
import com.fix42.dashboard.fixcache.FixEnums.OrdType;
import com.fix42.dashboard.fixcache.FixEnums.Side;
import com.fix42.dashboard.fixcache.FixEnums.TimeInForce;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The FIX 4.2 tag numbers and enumerations of doc 01 section 2.
 *
 * <p>Mirrors {@code deephaven-scripts/tests/test_fixtags.py}.
 */
class FixEnumsTest {

    @Test
    void tagNumbersMatchDocVocabulary() {
        Map<String, Integer> expected = Map.ofEntries(
                Map.entry("ACCOUNT", 1),
                Map.entry("AVG_PX", 6),
                Map.entry("CL_ORD_ID", 11),
                Map.entry("CUM_QTY", 14),
                Map.entry("EXEC_ID", 17),
                Map.entry("EXEC_REF_ID", 19),
                Map.entry("EXEC_TRANS_TYPE", 20),
                Map.entry("HANDL_INST", 21),
                Map.entry("LAST_MKT", 30),
                Map.entry("LAST_PX", 31),
                Map.entry("LAST_SHARES", 32),
                Map.entry("MSG_TYPE", 35),
                Map.entry("ORDER_ID", 37),
                Map.entry("ORDER_QTY", 38),
                Map.entry("ORD_STATUS", 39),
                Map.entry("ORD_TYPE", 40),
                Map.entry("ORIG_CL_ORD_ID", 41),
                Map.entry("PRICE", 44),
                Map.entry("SIDE", 54),
                Map.entry("SYMBOL", 55),
                Map.entry("TEXT", 58),
                Map.entry("TIME_IN_FORCE", 59),
                Map.entry("TRANSACT_TIME", 60),
                Map.entry("STOP_PX", 99),
                Map.entry("CXL_REJ_REASON", 102),
                Map.entry("ORD_REJ_REASON", 103),
                Map.entry("DK_REASON", 127),
                Map.entry("EXEC_TYPE", 150),
                Map.entry("LEAVES_QTY", 151),
                Map.entry("CXL_REJ_RESPONSE_TO", 434));

        assertEquals(1, FixTags.ACCOUNT);
        assertEquals(434, FixTags.CXL_REJ_RESPONSE_TO);
        assertEquals(30, expected.size());
    }

    @ParameterizedTest
    @CsvSource({
        "0,NEW", "1,PARTIALLY_FILLED", "2,FILLED", "3,DONE_FOR_DAY", "4,CANCELED",
        "5,REPLACED", "6,PENDING_CANCEL", "8,REJECTED", "A,PENDING_NEW", "C,EXPIRED",
        "E,PENDING_REPLACE"
    })
    void ordStatusFromFix(String code, String expected) {
        assertEquals(expected, OrdStatus.fromFix(code).name());
    }

    @ParameterizedTest
    @CsvSource({
        "0,NEW", "1,PARTIAL_FILL", "2,FILL", "3,DONE_FOR_DAY", "4,CANCELED", "5,REPLACED",
        "6,PENDING_CANCEL", "8,REJECTED", "A,PENDING_NEW", "C,EXPIRED", "D,RESTATED",
        "E,PENDING_REPLACE"
    })
    void execTypeFromFix(String code, String expected) {
        assertEquals(expected, ExecType.fromFix(code).name());
    }

    @Test
    void execTransTypeFromFix() {
        assertEquals(ExecTransType.NEW, ExecTransType.fromFix("0"));
        assertEquals(ExecTransType.CANCEL, ExecTransType.fromFix("1"));
        assertEquals(ExecTransType.CORRECT, ExecTransType.fromFix("2"));
        assertEquals(ExecTransType.STATUS, ExecTransType.fromFix("3"));
    }

    @Test
    void sideOrdTypeAndTifFromFix() {
        assertEquals(Side.BUY, Side.fromFix("1"));
        assertEquals(Side.SELL, Side.fromFix("2"));
        assertEquals(Side.SELL_SHORT, Side.fromFix("5"));
        assertEquals(OrdType.MARKET, OrdType.fromFix("1"));
        assertEquals(OrdType.LIMIT, OrdType.fromFix("2"));
        assertEquals(TimeInForce.DAY, TimeInForce.fromFix("0"));
        assertEquals(TimeInForce.GTC, TimeInForce.fromFix("1"));
        assertEquals(TimeInForce.IOC, TimeInForce.fromFix("3"));
        assertEquals(TimeInForce.FOK, TimeInForce.fromFix("4"));
    }

    @Test
    void cxlRejResponseToFromFix() {
        assertEquals(CxlRejResponseTo.ORDER_CANCEL_REQUEST, CxlRejResponseTo.fromFix("1"));
        assertEquals(CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST, CxlRejResponseTo.fromFix("2"));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "Z", "99", "?"})
    @DisplayName("unknown, empty and missing codes map to the sentinel without throwing")
    void unknownCodesMapToSentinel(String code) {
        assertEquals("UNKNOWN", OrdStatus.fromFix(code).name());
        assertEquals("UNKNOWN", ExecType.fromFix(code).name());
        assertEquals("UNKNOWN", ExecTransType.fromFix(code).name());
        assertEquals("UNKNOWN", Side.fromFix(code).name());
        assertEquals("UNKNOWN", OrdType.fromFix(code).name());
        assertEquals("UNKNOWN", TimeInForce.fromFix(code).name());
        assertEquals("UNKNOWN", CxlRejResponseTo.fromFix(code).name());
    }

    @Test
    void enumValuesAreTheRawFixCodes() {
        assertEquals("1", OrdStatus.PARTIALLY_FILLED.code());
        assertEquals("2", ExecType.FILL.code());
        assertEquals("5", Side.SELL_SHORT.code());
        assertEquals(FixEnums.UNKNOWN_CODE, OrdStatus.UNKNOWN.code());
    }

    @Test
    void terminalStatuses() {
        for (OrdStatus status : new OrdStatus[] {
            OrdStatus.FILLED, OrdStatus.CANCELED, OrdStatus.REJECTED, OrdStatus.EXPIRED, OrdStatus.DONE_FOR_DAY
        }) {
            assertTrue(FixEnums.isTerminal(status), status.name());
        }
        for (OrdStatus status : new OrdStatus[] {
            OrdStatus.NEW, OrdStatus.PARTIALLY_FILLED, OrdStatus.PENDING_NEW,
            OrdStatus.PENDING_CANCEL, OrdStatus.PENDING_REPLACE, OrdStatus.REPLACED, OrdStatus.UNKNOWN
        }) {
            assertFalse(FixEnums.isTerminal(status), status.name());
        }
        assertFalse(FixEnums.isTerminal(null));
    }
}
