package com.deephaven.fix42.oms;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Latest state of one order. Blank incoming fields never wipe a populated field. */
public final class OrderState {
    private String orderKey = "";
    private String clOrdId = "";
    private String origClOrdId = "";
    private final List<String> clOrdIdHistory = new ArrayList<>();
    private String orderId = "";
    private String secondaryOrderId = "";
    private String account = "";
    private String symbol = "";
    private String securityId = "";
    private String side = "";
    private String ordType = "";
    private String timeInForce = "";
    private String ordStatus = "";
    private String execType = "";
    private String execTransType = "";
    private String lastExecId = "";
    private double orderQty;
    private double cumQty;
    private double leavesQty;
    private double lastQty;
    private double lastPx;
    private double avgPx;
    private double price;
    private double stopPx;
    private boolean hasOrderQty;
    private boolean hasCumQty;
    private boolean hasLeavesQty;
    private boolean hasLastQty;
    private boolean hasLastPx;
    private boolean hasAvgPx;
    private boolean hasPrice;
    private boolean hasStopPx;
    private String parentOrderId = "";
    private String parentClOrdId = "";
    private final List<String> childOrderKeys = new ArrayList<>();
    private String transactTime = "";
    private String lastMsgType = "";
    private long lastUpdateEpochMs;
    private int version;
    private String text = "";
    private String ordRejReason = "";
    private String cxlRejReason = "";
    private String cxlRejResponseTo = "";
    private String dkReason = "";
    private boolean pendingCancel;
    private boolean pendingReplace;
    private boolean dkTrade;
    private final Set<String> seenExecKeys = new LinkedHashSet<>();

    public OrderState copy() {
        OrderState c = new OrderState();
        c.orderKey = orderKey;
        c.clOrdId = clOrdId;
        c.origClOrdId = origClOrdId;
        c.clOrdIdHistory.addAll(clOrdIdHistory);
        c.orderId = orderId;
        c.secondaryOrderId = secondaryOrderId;
        c.account = account;
        c.symbol = symbol;
        c.securityId = securityId;
        c.side = side;
        c.ordType = ordType;
        c.timeInForce = timeInForce;
        c.ordStatus = ordStatus;
        c.execType = execType;
        c.execTransType = execTransType;
        c.lastExecId = lastExecId;
        c.orderQty = orderQty;
        c.cumQty = cumQty;
        c.leavesQty = leavesQty;
        c.lastQty = lastQty;
        c.lastPx = lastPx;
        c.avgPx = avgPx;
        c.price = price;
        c.stopPx = stopPx;
        c.hasOrderQty = hasOrderQty;
        c.hasCumQty = hasCumQty;
        c.hasLeavesQty = hasLeavesQty;
        c.hasLastQty = hasLastQty;
        c.hasLastPx = hasLastPx;
        c.hasAvgPx = hasAvgPx;
        c.hasPrice = hasPrice;
        c.hasStopPx = hasStopPx;
        c.parentOrderId = parentOrderId;
        c.parentClOrdId = parentClOrdId;
        c.childOrderKeys.addAll(childOrderKeys);
        c.transactTime = transactTime;
        c.lastMsgType = lastMsgType;
        c.lastUpdateEpochMs = lastUpdateEpochMs;
        c.version = version;
        c.text = text;
        c.ordRejReason = ordRejReason;
        c.cxlRejReason = cxlRejReason;
        c.cxlRejResponseTo = cxlRejResponseTo;
        c.dkReason = dkReason;
        c.pendingCancel = pendingCancel;
        c.pendingReplace = pendingReplace;
        c.dkTrade = dkTrade;
        c.seenExecKeys.addAll(seenExecKeys);
        return c;
    }

    public String getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(String orderKey) {
        this.orderKey = nz(orderKey);
    }

    public String getClOrdId() {
        return clOrdId;
    }

    public void setClOrdId(String clOrdId) {
        this.clOrdId = nz(clOrdId);
    }

    public String getOrigClOrdId() {
        return origClOrdId;
    }

    public void setOrigClOrdId(String origClOrdId) {
        this.origClOrdId = nz(origClOrdId);
    }

    public List<String> getClOrdIdHistory() {
        return clOrdIdHistory;
    }

    public String getClOrdIdHistoryCsv() {
        return String.join(",", clOrdIdHistory);
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = nz(orderId);
    }

    public String getSecondaryOrderId() {
        return secondaryOrderId;
    }

    public void setSecondaryOrderId(String secondaryOrderId) {
        this.secondaryOrderId = nz(secondaryOrderId);
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = nz(account);
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = nz(symbol);
    }

    public String getSecurityId() {
        return securityId;
    }

    public void setSecurityId(String securityId) {
        this.securityId = nz(securityId);
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = nz(side);
    }

    public String getOrdType() {
        return ordType;
    }

    public void setOrdType(String ordType) {
        this.ordType = nz(ordType);
    }

    public String getTimeInForce() {
        return timeInForce;
    }

    public void setTimeInForce(String timeInForce) {
        this.timeInForce = nz(timeInForce);
    }

    public String getOrdStatus() {
        return ordStatus;
    }

    public void setOrdStatus(String ordStatus) {
        this.ordStatus = nz(ordStatus);
    }

    public String getExecType() {
        return execType;
    }

    public void setExecType(String execType) {
        this.execType = nz(execType);
    }

    public String getExecTransType() {
        return execTransType;
    }

    public void setExecTransType(String execTransType) {
        this.execTransType = nz(execTransType);
    }

    public String getLastExecId() {
        return lastExecId;
    }

    public void setLastExecId(String lastExecId) {
        this.lastExecId = nz(lastExecId);
    }

    public double getOrderQty() {
        return orderQty;
    }

    public void setOrderQty(double orderQty) {
        this.orderQty = orderQty;
        this.hasOrderQty = true;
    }

    public boolean hasOrderQty() {
        return hasOrderQty;
    }

    public double getCumQty() {
        return cumQty;
    }

    public void setCumQty(double cumQty) {
        this.cumQty = cumQty;
        this.hasCumQty = true;
    }

    public boolean hasCumQty() {
        return hasCumQty;
    }

    public double getLeavesQty() {
        return leavesQty;
    }

    public void setLeavesQty(double leavesQty) {
        this.leavesQty = leavesQty;
        this.hasLeavesQty = true;
    }

    public boolean hasLeavesQty() {
        return hasLeavesQty;
    }

    public double getLastQty() {
        return lastQty;
    }

    public void setLastQty(double lastQty) {
        this.lastQty = lastQty;
        this.hasLastQty = true;
    }

    public double getLastPx() {
        return lastPx;
    }

    public void setLastPx(double lastPx) {
        this.lastPx = lastPx;
        this.hasLastPx = true;
    }

    public double getAvgPx() {
        return avgPx;
    }

    public void setAvgPx(double avgPx) {
        this.avgPx = avgPx;
        this.hasAvgPx = true;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
        this.hasPrice = true;
    }

    public boolean hasPrice() {
        return hasPrice;
    }

    public double getStopPx() {
        return stopPx;
    }

    public void setStopPx(double stopPx) {
        this.stopPx = stopPx;
        this.hasStopPx = true;
    }

    public String getParentOrderId() {
        return parentOrderId;
    }

    public void setParentOrderId(String parentOrderId) {
        this.parentOrderId = nz(parentOrderId);
    }

    public String getParentClOrdId() {
        return parentClOrdId;
    }

    public void setParentClOrdId(String parentClOrdId) {
        this.parentClOrdId = nz(parentClOrdId);
    }

    public List<String> getChildOrderKeys() {
        return childOrderKeys;
    }

    public String getChildOrderKeysCsv() {
        return String.join(",", childOrderKeys);
    }

    public String getTransactTime() {
        return transactTime;
    }

    public void setTransactTime(String transactTime) {
        this.transactTime = nz(transactTime);
    }

    public String getLastMsgType() {
        return lastMsgType;
    }

    public void setLastMsgType(String lastMsgType) {
        this.lastMsgType = nz(lastMsgType);
    }

    public long getLastUpdateEpochMs() {
        return lastUpdateEpochMs;
    }

    public void setLastUpdateEpochMs(long lastUpdateEpochMs) {
        this.lastUpdateEpochMs = lastUpdateEpochMs;
    }

    public int getVersion() {
        return version;
    }

    public void bumpVersion() {
        this.version++;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = nz(text);
    }

    public String getOrdRejReason() {
        return ordRejReason;
    }

    public void setOrdRejReason(String ordRejReason) {
        this.ordRejReason = nz(ordRejReason);
    }

    public String getCxlRejReason() {
        return cxlRejReason;
    }

    public void setCxlRejReason(String cxlRejReason) {
        this.cxlRejReason = nz(cxlRejReason);
    }

    public String getCxlRejResponseTo() {
        return cxlRejResponseTo;
    }

    public void setCxlRejResponseTo(String cxlRejResponseTo) {
        this.cxlRejResponseTo = nz(cxlRejResponseTo);
    }

    public String getDkReason() {
        return dkReason;
    }

    public void setDkReason(String dkReason) {
        this.dkReason = nz(dkReason);
    }

    public boolean isPendingCancel() {
        return pendingCancel;
    }

    public void setPendingCancel(boolean pendingCancel) {
        this.pendingCancel = pendingCancel;
    }

    public boolean isPendingReplace() {
        return pendingReplace;
    }

    public void setPendingReplace(boolean pendingReplace) {
        this.pendingReplace = pendingReplace;
    }

    public boolean isDkTrade() {
        return dkTrade;
    }

    public void setDkTrade(boolean dkTrade) {
        this.dkTrade = dkTrade;
    }

    public Set<String> getSeenExecKeys() {
        return seenExecKeys;
    }

    public boolean isTerminal() {
        return "2".equals(ordStatus)
                || "4".equals(ordStatus)
                || "8".equals(ordStatus)
                || "C".equals(ordStatus)
                || "3".equals(ordStatus);
    }

    public static String nz(String value) {
        return value == null ? "" : value;
    }
}
