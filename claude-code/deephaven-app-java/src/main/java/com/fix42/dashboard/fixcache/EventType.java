package com.fix42.dashboard.fixcache;

import java.util.List;

/** Frozen {@code EventType} names for the order_events stream (doc 01 section 6). */
public final class EventType {

    public static final String NEW_REQUEST = "NEW_REQUEST";
    public static final String NEW_ACK = "NEW_ACK";
    public static final String NEW_REJECT = "NEW_REJECT";
    public static final String AMEND_REQUEST = "AMEND_REQUEST";
    public static final String AMEND_ACK = "AMEND_ACK";
    public static final String AMEND_REJECT = "AMEND_REJECT";
    public static final String CANCEL_REQUEST = "CANCEL_REQUEST";
    public static final String CANCEL_ACK = "CANCEL_ACK";
    public static final String CANCEL_REJECT = "CANCEL_REJECT";
    public static final String PENDING_NEW = "PENDING_NEW";
    public static final String PENDING_AMEND = "PENDING_AMEND";
    public static final String PENDING_CANCEL = "PENDING_CANCEL";
    public static final String PARTIAL_FILL = "PARTIAL_FILL";
    public static final String FULL_FILL = "FULL_FILL";
    public static final String FILL_BUST = "FILL_BUST";
    public static final String FILL_CORRECT = "FILL_CORRECT";
    public static final String DK_TRADE = "DK_TRADE";
    public static final String RESTATED = "RESTATED";
    public static final String STATUS = "STATUS";
    public static final String EXPIRED = "EXPIRED";
    public static final String DONE_FOR_DAY = "DONE_FOR_DAY";

    public static final List<String> ALL = List.of(
            NEW_REQUEST,
            NEW_ACK,
            NEW_REJECT,
            AMEND_REQUEST,
            AMEND_ACK,
            AMEND_REJECT,
            CANCEL_REQUEST,
            CANCEL_ACK,
            CANCEL_REJECT,
            PENDING_NEW,
            PENDING_AMEND,
            PENDING_CANCEL,
            PARTIAL_FILL,
            FULL_FILL,
            FILL_BUST,
            FILL_CORRECT,
            DK_TRADE,
            RESTATED,
            STATUS,
            EXPIRED,
            DONE_FOR_DAY);

    private EventType() {}
}
