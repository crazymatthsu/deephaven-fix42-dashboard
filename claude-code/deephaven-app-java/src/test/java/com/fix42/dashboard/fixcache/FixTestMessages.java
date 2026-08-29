package com.fix42.dashboard.fixcache;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builders for well-formed FIX 4.2 test messages -- the Java port of
 * {@code deephaven-scripts/tests/fixhelpers.py}.
 *
 * <p>Every builder emits correct {@code 9 BodyLength} and {@code 10 CheckSum} framing (doc 01
 * section 1) so parser and state-machine tests share one source of realistic input. Messages are
 * rendered pipe-delimited by default (readable in assertions); call {@code delimiter(SOH)} for the
 * real wire form.
 *
 * <p>python's keyword arguments become fluent setters here; the defaults are the python ones, and a
 * field left unset (or explicitly {@code null}) is omitted from the message exactly as python omits
 * a {@code None} value.
 */
public final class FixTestMessages {

    public static final String DEFAULT_SENDING_TIME = "20240115-14:30:00.000";
    public static final String DEFAULT_TRANSACT_TIME = "20240115-14:30:00.000";

    /** Sensible OrdStatus for a given ExecType when a test does not pin tag 39. */
    private static final Map<String, String> STATUS_FOR_EXEC_TYPE = Map.ofEntries(
            Map.entry("0", "0"),
            Map.entry("1", "1"),
            Map.entry("2", "2"),
            Map.entry("3", "3"),
            Map.entry("4", "4"),
            Map.entry("5", "5"),
            Map.entry("6", "6"),
            Map.entry("8", "8"),
            Map.entry("A", "A"),
            Map.entry("C", "C"),
            Map.entry("D", "D"),
            Map.entry("E", "E"));

    private FixTestMessages() {}

    /**
     * Renders a python value as a FIX field value ({@code fixhelpers._fmt}).
     *
     * <p>python renders an integral float without its fractional part ({@code str(int(1000.0))}) and
     * otherwise falls back to {@code str(value)}, whose {@code repr} is the shortest round-tripping
     * decimal. For every value this suite uses, {@link Double#toString} agrees with that; it would
     * diverge only outside {@code [1e-3, 1e7)}, where Java switches to exponent notation sooner than
     * python does.
     */
    static String fmt(Object value) {
        if (value instanceof Double d) {
            if (Double.isFinite(d) && d == Math.floor(d)) {
                return new BigDecimal(d).toBigInteger().toString();
            }
            return Double.toString(d);
        }
        return String.valueOf(value);
    }

    /**
     * Builds a FIX 4.2 message with correct BodyLength/CheckSum.
     *
     * @param msgType tag 35
     * @param fields body fields in wire order; {@code null} values are omitted
     * @param seq tag 34
     * @param sender tag 49
     * @param target tag 56
     * @param sendingTime tag 52
     * @param delimiter {@link FixParser#PIPE} or {@link FixParser#SOH}
     * @return the framed message
     */
    public static String buildFix(
            String msgType,
            Map<Integer, Object> fields,
            int seq,
            String sender,
            String target,
            String sendingTime,
            char delimiter) {
        List<Map.Entry<Integer, String>> body = new ArrayList<>();
        body.add(Map.entry(35, msgType));
        body.add(Map.entry(34, Integer.toString(seq)));
        body.add(Map.entry(49, sender));
        body.add(Map.entry(56, target));
        body.add(Map.entry(52, sendingTime));
        if (fields != null) {
            for (Map.Entry<Integer, Object> entry : fields.entrySet()) {
                if (entry.getValue() != null) {
                    body.add(Map.entry(entry.getKey(), fmt(entry.getValue())));
                }
            }
        }

        StringBuilder bodyText = new StringBuilder();
        for (Map.Entry<Integer, String> field : body) {
            bodyText.append(field.getKey()).append('=').append(field.getValue()).append(FixParser.SOH);
        }
        String prefix = "8=FIX.4.2" + FixParser.SOH + "9=" + bodyText.length() + FixParser.SOH + bodyText;
        int checksum = 0;
        for (byte b : prefix.getBytes(StandardCharsets.UTF_8)) {
            checksum += (b & 0xFF);
        }
        String message = prefix + "10=" + String.format("%03d", checksum % 256) + FixParser.SOH;
        return delimiter == FixParser.SOH ? message : message.replace(FixParser.SOH, delimiter);
    }

    /** Convenience for a message with default header values, pipe-delimited. */
    public static String buildFix(String msgType, Map<Integer, Object> fields) {
        return buildFix(msgType, fields, 1, "VENUE", "OMS", DEFAULT_SENDING_TIME, FixParser.PIPE);
    }

    /** {@code 35=D} NewOrderSingle. */
    public static NewOrder newOrder(String clOrdId) {
        return new NewOrder(clOrdId);
    }

    /** {@code 35=8} ExecutionReport (tag 39 defaults to the natural status for 150). */
    public static ExecReport execReport(String execType) {
        return new ExecReport(execType);
    }

    /** {@code 35=G} OrderCancelReplaceRequest. */
    public static ReplaceRequest replaceRequest(String clOrdId, String origClOrdId) {
        return new ReplaceRequest(clOrdId, origClOrdId);
    }

    /** {@code 35=F} OrderCancelRequest. */
    public static CancelRequest cancelRequest(String clOrdId, String origClOrdId) {
        return new CancelRequest(clOrdId, origClOrdId);
    }

    /** {@code 35=9} OrderCancelReject (tag 37 defaults to the {@code NONE} sentinel). */
    public static CancelReject cancelReject(String clOrdId, String origClOrdId) {
        return new CancelReject(clOrdId, origClOrdId);
    }

    /** {@code 35=Q} DontKnowTrade. */
    public static DkTrade dkTrade(String execId) {
        return new DkTrade(execId);
    }

    /** Shared header knobs; every builder inherits them. */
    abstract static class Base<S extends Base<S>> {
        int seq = 1;
        char delimiter = FixParser.PIPE;
        String transactTime = DEFAULT_TRANSACT_TIME;

        @SuppressWarnings("unchecked")
        S self() {
            return (S) this;
        }

        public S seq(int value) {
            this.seq = value;
            return self();
        }

        public S delimiter(char value) {
            this.delimiter = value;
            return self();
        }

        public S transactTime(String value) {
            this.transactTime = value;
            return self();
        }

        /** Renders the message. */
        public abstract String build();
    }

    /** {@code 35=D} NewOrderSingle. */
    public static final class NewOrder extends Base<NewOrder> {
        private final String clOrdId;
        private String account = "ACC1";
        private String symbol = "IBM";
        private String side = "1";
        private Object qty = 1000;
        private String ordType = "2";
        private Object price = 185.50;
        private String tif = "0";
        private Object stopPx;
        private String orderId;

        NewOrder(String clOrdId) {
            this.clOrdId = clOrdId;
        }

        public NewOrder account(String v) {
            account = v;
            return this;
        }

        public NewOrder symbol(String v) {
            symbol = v;
            return this;
        }

        public NewOrder side(String v) {
            side = v;
            return this;
        }

        public NewOrder qty(Object v) {
            qty = v;
            return this;
        }

        public NewOrder ordType(String v) {
            ordType = v;
            return this;
        }

        public NewOrder price(Object v) {
            price = v;
            return this;
        }

        public NewOrder tif(String v) {
            tif = v;
            return this;
        }

        public NewOrder stopPx(Object v) {
            stopPx = v;
            return this;
        }

        public NewOrder orderId(String v) {
            orderId = v;
            return this;
        }

        @Override
        public String build() {
            Map<Integer, Object> f = new LinkedHashMap<>();
            f.put(11, clOrdId);
            f.put(37, orderId);
            f.put(1, account);
            f.put(55, symbol);
            f.put(54, side);
            f.put(38, qty);
            f.put(40, ordType);
            f.put(44, price);
            f.put(59, tif);
            f.put(99, stopPx);
            f.put(21, "1");
            f.put(60, transactTime);
            return buildFix("D", f, seq, "VENUE", "OMS", DEFAULT_SENDING_TIME, delimiter);
        }
    }

    /** {@code 35=8} ExecutionReport. */
    public static final class ExecReport extends Base<ExecReport> {
        private final String execType;
        private String execId = "E1";
        private String orderId = "ORD-1";
        private String clOrdId = "C1";
        private String origClOrdId;
        private String ordStatus;
        private boolean ordStatusSet;
        private String execTransType;
        private String execRefId;
        private Object cumQty;
        private Object leavesQty;
        private Object avgPx;
        private Object lastShares;
        private Object lastPx;
        private String lastMkt;
        private String symbol = "IBM";
        private String side = "1";
        private Object qty = 1000;
        private Object price;
        private String account;
        private String ordRejReason;
        private String text;

        ExecReport(String execType) {
            this.execType = execType;
        }

        public ExecReport execId(String v) {
            execId = v;
            return this;
        }

        public ExecReport orderId(String v) {
            orderId = v;
            return this;
        }

        public ExecReport clOrdId(String v) {
            clOrdId = v;
            return this;
        }

        public ExecReport origClOrdId(String v) {
            origClOrdId = v;
            return this;
        }

        public ExecReport ordStatus(String v) {
            ordStatus = v;
            ordStatusSet = true;
            return this;
        }

        public ExecReport execTransType(String v) {
            execTransType = v;
            return this;
        }

        public ExecReport execRefId(String v) {
            execRefId = v;
            return this;
        }

        public ExecReport cumQty(Object v) {
            cumQty = v;
            return this;
        }

        public ExecReport leavesQty(Object v) {
            leavesQty = v;
            return this;
        }

        public ExecReport avgPx(Object v) {
            avgPx = v;
            return this;
        }

        public ExecReport lastShares(Object v) {
            lastShares = v;
            return this;
        }

        public ExecReport lastPx(Object v) {
            lastPx = v;
            return this;
        }

        public ExecReport lastMkt(String v) {
            lastMkt = v;
            return this;
        }

        public ExecReport symbol(String v) {
            symbol = v;
            return this;
        }

        public ExecReport side(String v) {
            side = v;
            return this;
        }

        public ExecReport qty(Object v) {
            qty = v;
            return this;
        }

        public ExecReport price(Object v) {
            price = v;
            return this;
        }

        public ExecReport account(String v) {
            account = v;
            return this;
        }

        public ExecReport ordRejReason(String v) {
            ordRejReason = v;
            return this;
        }

        public ExecReport text(String v) {
            text = v;
            return this;
        }

        @Override
        public String build() {
            Map<Integer, Object> f = new LinkedHashMap<>();
            f.put(37, orderId);
            f.put(11, clOrdId);
            f.put(41, origClOrdId);
            f.put(17, execId);
            f.put(19, execRefId);
            f.put(20, execTransType);
            f.put(150, execType);
            f.put(39, ordStatusSet ? ordStatus : STATUS_FOR_EXEC_TYPE.get(execType));
            f.put(1, account);
            f.put(55, symbol);
            f.put(54, side);
            f.put(38, qty);
            f.put(44, price);
            f.put(32, lastShares);
            f.put(31, lastPx);
            f.put(30, lastMkt);
            f.put(14, cumQty);
            f.put(151, leavesQty);
            f.put(6, avgPx);
            f.put(103, ordRejReason);
            f.put(58, text);
            f.put(60, transactTime);
            return buildFix("8", f, seq, "VENUE", "OMS", DEFAULT_SENDING_TIME, delimiter);
        }
    }

    /** {@code 35=G} OrderCancelReplaceRequest. */
    public static final class ReplaceRequest extends Base<ReplaceRequest> {
        private final String clOrdId;
        private final String origClOrdId;
        private String orderId;
        private String account = "ACC1";
        private String symbol = "IBM";
        private String side = "1";
        private Object qty;
        private String ordType = "2";
        private Object price;
        private String tif;

        ReplaceRequest(String clOrdId, String origClOrdId) {
            this.clOrdId = clOrdId;
            this.origClOrdId = origClOrdId;
        }

        public ReplaceRequest orderId(String v) {
            orderId = v;
            return this;
        }

        public ReplaceRequest account(String v) {
            account = v;
            return this;
        }

        public ReplaceRequest symbol(String v) {
            symbol = v;
            return this;
        }

        public ReplaceRequest side(String v) {
            side = v;
            return this;
        }

        public ReplaceRequest qty(Object v) {
            qty = v;
            return this;
        }

        public ReplaceRequest ordType(String v) {
            ordType = v;
            return this;
        }

        public ReplaceRequest price(Object v) {
            price = v;
            return this;
        }

        public ReplaceRequest tif(String v) {
            tif = v;
            return this;
        }

        @Override
        public String build() {
            Map<Integer, Object> f = new LinkedHashMap<>();
            f.put(11, clOrdId);
            f.put(41, origClOrdId);
            f.put(37, orderId);
            f.put(1, account);
            f.put(55, symbol);
            f.put(54, side);
            f.put(38, qty);
            f.put(40, ordType);
            f.put(44, price);
            f.put(59, tif);
            f.put(21, "1");
            f.put(60, transactTime);
            return buildFix("G", f, seq, "VENUE", "OMS", DEFAULT_SENDING_TIME, delimiter);
        }
    }

    /** {@code 35=F} OrderCancelRequest. */
    public static final class CancelRequest extends Base<CancelRequest> {
        private final String clOrdId;
        private final String origClOrdId;
        private String orderId;
        private String account = "ACC1";
        private String symbol = "IBM";
        private String side = "1";
        private Object qty;

        CancelRequest(String clOrdId, String origClOrdId) {
            this.clOrdId = clOrdId;
            this.origClOrdId = origClOrdId;
        }

        public CancelRequest orderId(String v) {
            orderId = v;
            return this;
        }

        public CancelRequest account(String v) {
            account = v;
            return this;
        }

        public CancelRequest symbol(String v) {
            symbol = v;
            return this;
        }

        public CancelRequest side(String v) {
            side = v;
            return this;
        }

        public CancelRequest qty(Object v) {
            qty = v;
            return this;
        }

        @Override
        public String build() {
            Map<Integer, Object> f = new LinkedHashMap<>();
            f.put(11, clOrdId);
            f.put(41, origClOrdId);
            f.put(37, orderId);
            f.put(1, account);
            f.put(55, symbol);
            f.put(54, side);
            f.put(38, qty);
            f.put(60, transactTime);
            return buildFix("F", f, seq, "VENUE", "OMS", DEFAULT_SENDING_TIME, delimiter);
        }
    }

    /** {@code 35=9} OrderCancelReject. */
    public static final class CancelReject extends Base<CancelReject> {
        private final String clOrdId;
        private final String origClOrdId;
        private String responseTo = "1";
        private String orderId = "NONE";
        private String ordStatus;
        private String cxlRejReason;
        private String text;

        CancelReject(String clOrdId, String origClOrdId) {
            this.clOrdId = clOrdId;
            this.origClOrdId = origClOrdId;
        }

        public CancelReject responseTo(String v) {
            responseTo = v;
            return this;
        }

        public CancelReject orderId(String v) {
            orderId = v;
            return this;
        }

        public CancelReject ordStatus(String v) {
            ordStatus = v;
            return this;
        }

        public CancelReject cxlRejReason(String v) {
            cxlRejReason = v;
            return this;
        }

        public CancelReject text(String v) {
            text = v;
            return this;
        }

        @Override
        public String build() {
            Map<Integer, Object> f = new LinkedHashMap<>();
            f.put(11, clOrdId);
            f.put(41, origClOrdId);
            f.put(37, orderId);
            f.put(39, ordStatus);
            f.put(102, cxlRejReason);
            f.put(434, responseTo);
            f.put(58, text);
            f.put(60, transactTime);
            return buildFix("9", f, seq, "VENUE", "OMS", DEFAULT_SENDING_TIME, delimiter);
        }
    }

    /** {@code 35=Q} DontKnowTrade. */
    public static final class DkTrade extends Base<DkTrade> {
        private final String execId;
        private String orderId = "ORD-1";
        private String dkReason = "A";
        private String symbol = "IBM";
        private String side = "1";
        private Object qty = 1000;
        private Object lastShares;
        private Object lastPx;
        private String text;

        DkTrade(String execId) {
            this.execId = execId;
        }

        public DkTrade orderId(String v) {
            orderId = v;
            return this;
        }

        public DkTrade dkReason(String v) {
            dkReason = v;
            return this;
        }

        public DkTrade symbol(String v) {
            symbol = v;
            return this;
        }

        public DkTrade side(String v) {
            side = v;
            return this;
        }

        public DkTrade qty(Object v) {
            qty = v;
            return this;
        }

        public DkTrade lastShares(Object v) {
            lastShares = v;
            return this;
        }

        public DkTrade lastPx(Object v) {
            lastPx = v;
            return this;
        }

        public DkTrade text(String v) {
            text = v;
            return this;
        }

        @Override
        public String build() {
            Map<Integer, Object> f = new LinkedHashMap<>();
            f.put(37, orderId);
            f.put(17, execId);
            f.put(127, dkReason);
            f.put(55, symbol);
            f.put(54, side);
            f.put(38, qty);
            f.put(32, lastShares);
            f.put(31, lastPx);
            f.put(58, text);
            f.put(60, transactTime);
            return buildFix("Q", f, seq, "VENUE", "OMS", DEFAULT_SENDING_TIME, delimiter);
        }
    }
}
