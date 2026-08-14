package com.deephaven.fix42.codec;

public final class FixConstants {
    public static final char SOH = '\u0001';
    public static final char PIPE = '|';
    public static final String BEGIN_STRING_42 = "FIX.4.2";

    public static final String MSG_NEW_ORDER_SINGLE = "D";
    public static final String MSG_EXECUTION_REPORT = "8";
    public static final String MSG_ORDER_CANCEL_REJECT = "9";
    public static final String MSG_ORDER_CANCEL_REQUEST = "F";
    public static final String MSG_ORDER_CANCEL_REPLACE = "G";
    public static final String MSG_ORDER_STATUS_REQUEST = "H";
    public static final String MSG_DONT_KNOW_TRADE = "Q";

    private FixConstants() {}
}
