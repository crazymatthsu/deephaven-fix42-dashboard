package com.fix42.dashboard.fixcache;

import com.fix42.dashboard.fixcache.FixEnums.ExecTransType;
import com.fix42.dashboard.fixcache.FixEnums.ExecType;
import com.fix42.dashboard.fixcache.FixEnums.OrdStatus;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One executions-stream row: per {@code 35=8}, per {@code 35=Q}, plus re-emissions.
 *
 * <p>Port of {@code fix42cache.model.ExecutionRow}. {@code FillStatus} carries the <em>latest</em>
 * disposition of this {@code ExecID}; bust / correct / DK messages re-emit the referenced
 * execution's row with the new disposition so {@code last_by(ExecID)} downstream shows current
 * truth.
 *
 * <p>{@link #copy()} stands in for python's {@code dataclasses.replace}: the state machine keeps the
 * last emitted row per ExecID and re-emits an edited copy, never mutating the stored one in place.
 */
public final class ExecutionRow {

    String orderKey;
    String orderId = "";
    String clOrdId = "";
    String execId = "";
    String execRefId = "";
    ExecTransType execTransType;
    ExecType execType;
    OrdStatus ordStatus;
    double lastShares;
    double lastPx;
    double cumQty;
    double leavesQty;
    double avgPx;
    String lastMkt = "";
    String text = "";
    boolean isFill;
    String fillStatus = FillStatus.NORMAL;
    Instant transactTime;
    Instant ingestTs;

    ExecutionRow(String orderKey) {
        this.orderKey = orderKey;
    }

    /** Returns an independent copy (every field is immutable, so a field-wise copy suffices). */
    public ExecutionRow copy() {
        ExecutionRow other = new ExecutionRow(orderKey);
        other.orderId = orderId;
        other.clOrdId = clOrdId;
        other.execId = execId;
        other.execRefId = execRefId;
        other.execTransType = execTransType;
        other.execType = execType;
        other.ordStatus = ordStatus;
        other.lastShares = lastShares;
        other.lastPx = lastPx;
        other.cumQty = cumQty;
        other.leavesQty = leavesQty;
        other.avgPx = avgPx;
        other.lastMkt = lastMkt;
        other.text = text;
        other.isFill = isFill;
        other.fillStatus = fillStatus;
        other.transactTime = transactTime;
        other.ingestTs = ingestTs;
        return other;
    }

    /** Renders the frozen doc 01 section 6 executions columns ({@link Columns#EXECUTION}). */
    public Map<String, Object> toRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("OrderKey", orderKey);
        row.put("OrderID", orderId);
        row.put("ClOrdID", clOrdId);
        row.put("ExecID", execId);
        row.put("ExecRefID", execRefId);
        row.put("ExecTransType", Rows.name(execTransType));
        row.put("ExecType", Rows.name(execType));
        row.put("OrdStatus", Rows.name(ordStatus));
        row.put("LastShares", lastShares);
        row.put("LastPx", lastPx);
        row.put("CumQty", cumQty);
        row.put("LeavesQty", leavesQty);
        row.put("AvgPx", avgPx);
        row.put("LastMkt", lastMkt);
        row.put("Text", text);
        row.put("IsFill", isFill);
        row.put("FillStatus", fillStatus);
        row.put("TransactTime", transactTime);
        row.put("IngestTs", ingestTs);
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

    public String execId() {
        return execId;
    }

    public String execRefId() {
        return execRefId;
    }

    public ExecTransType execTransType() {
        return execTransType;
    }

    public ExecType execType() {
        return execType;
    }

    public OrdStatus ordStatus() {
        return ordStatus;
    }

    public double lastShares() {
        return lastShares;
    }

    public double lastPx() {
        return lastPx;
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

    public String lastMkt() {
        return lastMkt;
    }

    public String text() {
        return text;
    }

    public boolean isFill() {
        return isFill;
    }

    public String fillStatus() {
        return fillStatus;
    }

    public Instant transactTime() {
        return transactTime;
    }

    public Instant ingestTs() {
        return ingestTs;
    }
}
