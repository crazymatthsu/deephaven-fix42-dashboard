package com.fix42.dashboard.fixcache;

import com.fix42.dashboard.fixcache.FixEnums.OrdStatus;
import com.fix42.dashboard.fixcache.FixEnums.OrdType;
import com.fix42.dashboard.fixcache.FixEnums.Side;
import com.fix42.dashboard.fixcache.FixEnums.TimeInForce;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The cache value: one row per order chain (doc 01 section 4).
 *
 * <p>Port of the {@code fix42cache.model.OrderState} dataclass. Instances held by
 * {@link OrderStateMachine} are mutable -- fields are package-private so only the machine in this
 * package writes them -- and every {@link Result} carries a {@link #copy()} so consumers observe an
 * immutable snapshot.
 *
 * <p>Numeric columns default to {@code 0.0}/{@code 0} and string columns to {@code ""}: the snapshot
 * is always fully populated, unlike {@link MessageRow} where an absent tag stays null.
 */
public final class OrderState {

    final String orderKey;
    String orderId = "";
    String clOrdId = "";
    String origClOrdId = "";
    String rootClOrdId = "";
    List<String> clOrdIdChain = new ArrayList<>();
    String account = "";
    String symbol = "";
    Side side;
    OrdType ordType;
    TimeInForce timeInForce;
    double orderQty;
    double price;
    double stopPx;
    OrdStatus ordStatus;
    String pendingAction = PendingAction.NONE;
    String pendingClOrdId = "";
    FixEnums.ExecType lastExecType;
    double cumQty;
    double leavesQty;
    double avgPx;
    double lastShares;
    double lastPx;
    String lastMkt = "";
    String ordRejReason = "";
    String cxlRejReason = "";
    String dkReason = "";
    String text = "";
    long execCount;
    long msgCount;
    Instant firstSeenTs;
    Instant lastUpdateTs;
    String lastMsgType = "";

    OrderState(String orderKey) {
        this.orderKey = orderKey;
    }

    OrderState(String orderKey, Instant firstSeenTs) {
        this.orderKey = orderKey;
        this.firstSeenTs = firstSeenTs;
    }

    /** {@code true} when OrdStatus is FILLED/CANCELED/REJECTED/EXPIRED/DONE_FOR_DAY. */
    public boolean terminal() {
        return FixEnums.isTerminal(ordStatus);
    }

    /** Returns an independent snapshot (the ClOrdID chain list is copied). */
    public OrderState copy() {
        OrderState other = new OrderState(orderKey);
        other.orderId = orderId;
        other.clOrdId = clOrdId;
        other.origClOrdId = origClOrdId;
        other.rootClOrdId = rootClOrdId;
        other.clOrdIdChain = new ArrayList<>(clOrdIdChain);
        other.account = account;
        other.symbol = symbol;
        other.side = side;
        other.ordType = ordType;
        other.timeInForce = timeInForce;
        other.orderQty = orderQty;
        other.price = price;
        other.stopPx = stopPx;
        other.ordStatus = ordStatus;
        other.pendingAction = pendingAction;
        other.pendingClOrdId = pendingClOrdId;
        other.lastExecType = lastExecType;
        other.cumQty = cumQty;
        other.leavesQty = leavesQty;
        other.avgPx = avgPx;
        other.lastShares = lastShares;
        other.lastPx = lastPx;
        other.lastMkt = lastMkt;
        other.ordRejReason = ordRejReason;
        other.cxlRejReason = cxlRejReason;
        other.dkReason = dkReason;
        other.text = text;
        other.execCount = execCount;
        other.msgCount = msgCount;
        other.firstSeenTs = firstSeenTs;
        other.lastUpdateTs = lastUpdateTs;
        other.lastMsgType = lastMsgType;
        return other;
    }

    /** Renders the frozen doc 01 section 4 columns ({@link Columns#ORDER_STATE}). */
    public Map<String, Object> toRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("OrderKey", orderKey);
        row.put("OrderID", orderId);
        row.put("ClOrdID", clOrdId);
        row.put("OrigClOrdID", origClOrdId);
        row.put("RootClOrdID", rootClOrdId);
        row.put("ClOrdIDChain", String.join(",", clOrdIdChain));
        row.put("Account", account);
        row.put("Symbol", symbol);
        row.put("Side", Rows.name(side));
        row.put("OrdType", Rows.name(ordType));
        row.put("TimeInForce", Rows.name(timeInForce));
        row.put("OrderQty", orderQty);
        row.put("Price", price);
        row.put("StopPx", stopPx);
        row.put("OrdStatus", Rows.name(ordStatus));
        row.put("PendingAction", pendingAction);
        row.put("PendingClOrdID", pendingClOrdId);
        row.put("LastExecType", Rows.name(lastExecType));
        row.put("CumQty", cumQty);
        row.put("LeavesQty", leavesQty);
        row.put("AvgPx", avgPx);
        row.put("LastShares", lastShares);
        row.put("LastPx", lastPx);
        row.put("LastMkt", lastMkt);
        row.put("OrdRejReason", ordRejReason);
        row.put("CxlRejReason", cxlRejReason);
        row.put("DKReason", dkReason);
        row.put("Text", text);
        row.put("ExecCount", execCount);
        row.put("MsgCount", msgCount);
        row.put("FirstSeenTs", firstSeenTs);
        row.put("LastUpdateTs", lastUpdateTs);
        row.put("LastMsgType", lastMsgType);
        row.put("Terminal", terminal());
        return row;
    }

    // ------------------------------------------------------------------ accessors

    public String orderKey() {
        return orderKey;
    }

    public String orderId() {
        return orderId;
    }

    public String clOrdId() {
        return clOrdId;
    }

    public String origClOrdId() {
        return origClOrdId;
    }

    public String rootClOrdId() {
        return rootClOrdId;
    }

    public List<String> clOrdIdChain() {
        return List.copyOf(clOrdIdChain);
    }

    public String account() {
        return account;
    }

    public String symbol() {
        return symbol;
    }

    public Side side() {
        return side;
    }

    public OrdType ordType() {
        return ordType;
    }

    public TimeInForce timeInForce() {
        return timeInForce;
    }

    public double orderQty() {
        return orderQty;
    }

    public double price() {
        return price;
    }

    public double stopPx() {
        return stopPx;
    }

    public OrdStatus ordStatus() {
        return ordStatus;
    }

    public String pendingAction() {
        return pendingAction;
    }

    public String pendingClOrdId() {
        return pendingClOrdId;
    }

    public FixEnums.ExecType lastExecType() {
        return lastExecType;
    }

    public double cumQty() {
        return cumQty;
    }

    public double leavesQty() {
        return leavesQty;
    }

    public double avgPx() {
        return avgPx;
    }

    public double lastShares() {
        return lastShares;
    }

    public double lastPx() {
        return lastPx;
    }

    public String lastMkt() {
        return lastMkt;
    }

    public String ordRejReason() {
        return ordRejReason;
    }

    public String cxlRejReason() {
        return cxlRejReason;
    }

    public String dkReason() {
        return dkReason;
    }

    public String text() {
        return text;
    }

    public long execCount() {
        return execCount;
    }

    public long msgCount() {
        return msgCount;
    }

    public Instant firstSeenTs() {
        return firstSeenTs;
    }

    public Instant lastUpdateTs() {
        return lastUpdateTs;
    }

    public String lastMsgType() {
        return lastMsgType;
    }
}
