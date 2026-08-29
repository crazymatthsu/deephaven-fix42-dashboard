package com.fix42.dashboard.fixcache;

import com.fix42.dashboard.fixcache.FixEnums.OrdStatus;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One lifecycle event feeding the order-history panel (doc 01 section 6).
 *
 * <p>Port of {@code fix42cache.model.OrderEventRow}.
 */
public final class OrderEventRow {

    String orderKey;
    String clOrdId = "";
    String origClOrdId = "";
    String orderId = "";
    String eventType = EventType.STATUS;
    String msgType = "";
    OrdStatus ordStatus;
    double orderQty;
    double price;
    String detail = "";
    Instant transactTime;
    Instant ingestTs;

    OrderEventRow(String orderKey) {
        this.orderKey = orderKey;
    }

    /** Renders the frozen doc 01 section 6 order_events columns ({@link Columns#ORDER_EVENT}). */
    public Map<String, Object> toRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("OrderKey", orderKey);
        row.put("ClOrdID", clOrdId);
        row.put("OrigClOrdID", origClOrdId);
        row.put("OrderID", orderId);
        row.put("EventType", eventType);
        row.put("MsgType", msgType);
        row.put("OrdStatus", Rows.name(ordStatus));
        row.put("OrderQty", orderQty);
        row.put("Price", price);
        row.put("Detail", detail);
        row.put("TransactTime", transactTime);
        row.put("IngestTs", ingestTs);
        return row;
    }

    // ------------------------------------------------------------------ accessors

    public String orderKey() {
        return orderKey;
    }

    public String clOrdId() {
        return clOrdId;
    }

    public String origClOrdId() {
        return origClOrdId;
    }

    public String orderId() {
        return orderId;
    }

    public String eventType() {
        return eventType;
    }

    public String msgType() {
        return msgType;
    }

    public OrdStatus ordStatus() {
        return ordStatus;
    }

    public double orderQty() {
        return orderQty;
    }

    public double price() {
        return price;
    }

    public String detail() {
        return detail;
    }

    public Instant transactTime() {
        return transactTime;
    }

    public Instant ingestTs() {
        return ingestTs;
    }
}
