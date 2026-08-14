package com.deephaven.fix42.codec;

/** FIX 4.2 tags the state machine and parser name in code. */
public final class Tags {
    public static final int ACCOUNT = 1;
    public static final int AVG_PX = 6;
    public static final int BEGIN_STRING = 8;
    public static final int BODY_LENGTH = 9;
    public static final int CHECK_SUM = 10;
    public static final int CL_ORD_ID = 11;
    public static final int CUM_QTY = 14;
    public static final int CURRENCY = 15;
    public static final int EXEC_ID = 17;
    public static final int EXEC_REF_ID = 19;
    public static final int EXEC_TRANS_TYPE = 20;
    public static final int HANDL_INST = 21;
    public static final int SECURITY_ID = 48;
    public static final int SENDER_COMP_ID = 49;
    public static final int SIDE = 54;
    public static final int SYMBOL = 55;
    public static final int TARGET_COMP_ID = 56;
    public static final int TEXT = 58;
    public static final int TIME_IN_FORCE = 59;
    public static final int TRANSACT_TIME = 60;
    public static final int MSG_TYPE = 35;
    public static final int MSG_SEQ_NUM = 34;
    public static final int SENDING_TIME = 52;
    public static final int LAST_PX = 31;
    public static final int LAST_SHARES = 32;
    public static final int LAST_MKT = 30;
    public static final int ORDER_ID = 37;
    public static final int ORDER_QTY = 38;
    public static final int ORD_STATUS = 39;
    public static final int ORD_TYPE = 40;
    public static final int ORIG_CL_ORD_ID = 41;
    public static final int PRICE = 44;
    public static final int STOP_PX = 99;
    public static final int CXL_REJ_REASON = 102;
    public static final int ORD_REJ_REASON = 103;
    public static final int DK_REASON = 127;
    public static final int EXEC_TYPE = 150;
    public static final int LEAVES_QTY = 151;
    public static final int SECONDARY_ORDER_ID = 198;
    public static final int CXL_REJ_RESPONSE_TO = 434;

    /** Default parent-link tags (not in the FIX 4.2 spec). */
    public static final int PARENT_ORDER_ID = 20001;
    public static final int PARENT_CL_ORD_ID = 20002;

    private Tags() {}
}
