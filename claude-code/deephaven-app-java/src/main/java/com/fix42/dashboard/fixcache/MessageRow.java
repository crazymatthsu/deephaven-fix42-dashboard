package com.fix42.dashboard.fixcache;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The raw-message audit row: every doc 01 section 2 tag as a typed column.
 *
 * <p>Port of {@code fix42cache.model.MessageRow}. Unlike {@link OrderState}, the numeric columns here
 * are <b>nullable</b>: the audit table has to distinguish "tag absent" from "tag present and zero",
 * so an absent tag yields {@code null} (a Deephaven null cell) rather than {@code 0.0}. String
 * columns still default to {@code ""}.
 */
public final class MessageRow {

    String orderKey;
    String msgType = "";
    String clOrdId = "";
    String origClOrdId = "";
    String orderId = "";
    String execId = "";
    String execRefId = "";
    String execTransType = "";
    String execType = "";
    String ordStatus = "";
    String account = "";
    String symbol = "";
    String side = "";
    Double orderQty;
    String ordType = "";
    Double price;
    String timeInForce = "";
    Double cumQty;
    Double leavesQty;
    Double avgPx;
    Double lastShares;
    Double lastPx;
    String lastMkt = "";
    String ordRejReason = "";
    String cxlRejReason = "";
    String cxlRejResponseTo = "";
    String dkReason = "";
    String text = "";
    Instant transactTime;
    String handlInst = "";
    String rawFix = "";
    Boolean checksumOk;
    Long seqNum;
    Instant sendingTime;
    Instant ingestTs;

    MessageRow(String orderKey) {
        this.orderKey = orderKey;
    }

    /**
     * Builds an audit row from parsed fields plus the original wire string.
     *
     * @param fields the parsed message
     * @param orderKey may be {@code ""} when the message cannot be attributed to a chain -- the audit
     *     table records everything regardless
     * @param raw the original wire string, rendered pipe-delimited into {@code RawFix}
     * @param ingestTs the ingest clock reading for this message
     * @return the populated row
     */
    public static MessageRow fromFields(
            Map<Integer, String> fields, String orderKey, String raw, Instant ingestTs) {
        MessageRow row = new MessageRow(orderKey);
        row.msgType = str(fields, FixTags.MSG_TYPE);
        row.clOrdId = str(fields, FixTags.CL_ORD_ID);
        row.origClOrdId = str(fields, FixTags.ORIG_CL_ORD_ID);
        row.orderId = str(fields, FixTags.ORDER_ID);
        row.execId = str(fields, FixTags.EXEC_ID);
        row.execRefId = str(fields, FixTags.EXEC_REF_ID);
        row.execTransType = enumName(fields, FixTags.EXEC_TRANS_TYPE, FixEnums.ExecTransType::fromFix);
        row.execType = enumName(fields, FixTags.EXEC_TYPE, FixEnums.ExecType::fromFix);
        row.ordStatus = enumName(fields, FixTags.ORD_STATUS, FixEnums.OrdStatus::fromFix);
        row.account = str(fields, FixTags.ACCOUNT);
        row.symbol = str(fields, FixTags.SYMBOL);
        row.side = enumName(fields, FixTags.SIDE, FixEnums.Side::fromFix);
        row.orderQty = optDouble(fields, FixTags.ORDER_QTY);
        row.ordType = enumName(fields, FixTags.ORD_TYPE, FixEnums.OrdType::fromFix);
        row.price = optDouble(fields, FixTags.PRICE);
        row.timeInForce = enumName(fields, FixTags.TIME_IN_FORCE, FixEnums.TimeInForce::fromFix);
        row.cumQty = optDouble(fields, FixTags.CUM_QTY);
        row.leavesQty = optDouble(fields, FixTags.LEAVES_QTY);
        row.avgPx = optDouble(fields, FixTags.AVG_PX);
        row.lastShares = optDouble(fields, FixTags.LAST_SHARES);
        row.lastPx = optDouble(fields, FixTags.LAST_PX);
        row.lastMkt = str(fields, FixTags.LAST_MKT);
        row.ordRejReason = str(fields, FixTags.ORD_REJ_REASON);
        row.cxlRejReason = str(fields, FixTags.CXL_REJ_REASON);
        row.cxlRejResponseTo =
                enumName(fields, FixTags.CXL_REJ_RESPONSE_TO, FixEnums.CxlRejResponseTo::fromFix);
        row.dkReason = str(fields, FixTags.DK_REASON);
        row.text = str(fields, FixTags.TEXT);
        row.transactTime = FixParser.parseTransactTime(fields.get(FixTags.TRANSACT_TIME));
        row.handlInst = str(fields, FixTags.HANDL_INST);
        row.rawFix = FixParser.renderPipe(raw);
        row.checksumOk = FixParser.checksumOk(raw);
        row.seqNum = optLong(fields, FixTags.MSG_SEQ_NUM);
        row.sendingTime = FixParser.parseTransactTime(fields.get(FixTags.SENDING_TIME));
        row.ingestTs = ingestTs;
        return row;
    }

    /** Renders the frozen doc 01 section 6 fix_messages columns ({@link Columns#MESSAGE}). */
    public Map<String, Object> toRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("OrderKey", orderKey);
        row.put("MsgType", msgType);
        row.put("ClOrdID", clOrdId);
        row.put("OrigClOrdID", origClOrdId);
        row.put("OrderID", orderId);
        row.put("ExecID", execId);
        row.put("ExecRefID", execRefId);
        row.put("ExecTransType", execTransType);
        row.put("ExecType", execType);
        row.put("OrdStatus", ordStatus);
        row.put("Account", account);
        row.put("Symbol", symbol);
        row.put("Side", side);
        row.put("OrderQty", orderQty);
        row.put("OrdType", ordType);
        row.put("Price", price);
        row.put("TimeInForce", timeInForce);
        row.put("CumQty", cumQty);
        row.put("LeavesQty", leavesQty);
        row.put("AvgPx", avgPx);
        row.put("LastShares", lastShares);
        row.put("LastPx", lastPx);
        row.put("LastMkt", lastMkt);
        row.put("OrdRejReason", ordRejReason);
        row.put("CxlRejReason", cxlRejReason);
        row.put("CxlRejResponseTo", cxlRejResponseTo);
        row.put("DKReason", dkReason);
        row.put("Text", text);
        row.put("TransactTime", transactTime);
        row.put("HandlInst", handlInst);
        row.put("RawFix", rawFix);
        row.put("ChecksumOk", checksumOk);
        row.put("SeqNum", seqNum);
        row.put("SendingTime", sendingTime);
        row.put("IngestTs", ingestTs);
        return row;
    }

    // ------------------------------------------------------------------ field helpers

    /** Reads a string tag, defaulting to {@code ""} ({@code fix42cache.model._str}). */
    private static String str(Map<Integer, String> fields, int tag) {
        String value = fields.get(tag);
        return value != null ? value : "";
    }

    /** Reads a numeric tag; {@code null} when absent or unparseable ({@code _opt_float}). */
    private static Double optDouble(Map<Integer, String> fields, int tag) {
        String raw = fields.get(tag);
        if (raw == null) {
            return null;
        }
        try {
            return Double.valueOf(PyFloat.parse(PyDigits.strip(raw)));
        } catch (NumberFormatException notANumber) {
            return null;
        }
    }

    /**
     * Reads an integer tag; {@code null} when absent or unparseable ({@code _opt_int}).
     *
     * <p><b>One documented deviation from the python original.</b> python's {@code int()} is
     * unbounded, so {@code 34=12345678901234567890} yields that exact value there. {@code SeqNum} is
     * a Deephaven {@code long} column, which cannot represent it either way -- python only discovers
     * that later, when the batch is built, and fails the whole batch into {@code ingest_errors}.
     * Returning {@code null} here renders the same "no usable sequence number" fact as a null cell
     * and keeps the other 34 columns of that message in the audit table. Every value that fits a
     * signed 64-bit integer parses identically in both implementations.
     */
    private static Long optLong(Map<Integer, String> fields, int tag) {
        String raw = fields.get(tag);
        if (raw == null) {
            return null;
        }
        try {
            return Long.valueOf(PyInt.parse(PyDigits.strip(raw)).longValueExact());
        } catch (NumberFormatException | ArithmeticException notAnInt) {
            return null;
        }
    }

    /** Renders a tag through its enum, or {@code ""} when the tag is absent ({@code _enum_name}). */
    private static String enumName(
            Map<Integer, String> fields, int tag, java.util.function.Function<String, ? extends Enum<?>> mapper) {
        String raw = fields.get(tag);
        if (raw == null) {
            return "";
        }
        return mapper.apply(raw).name();
    }

    // ------------------------------------------------------------------ accessors

    public String orderKey() {
        return orderKey;
    }

    public String msgType() {
        return msgType;
    }

    public String rawFix() {
        return rawFix;
    }

    public Boolean checksumOk() {
        return checksumOk;
    }

    public Long seqNum() {
        return seqNum;
    }

    public Instant transactTime() {
        return transactTime;
    }

    public Instant sendingTime() {
        return sendingTime;
    }

    public Instant ingestTs() {
        return ingestTs;
    }
}
