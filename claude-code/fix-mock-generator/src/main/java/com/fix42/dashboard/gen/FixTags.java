package com.fix42.dashboard.gen;

/**
 * FIX 4.2 tag numbers and enum-name mappings used by this project.
 *
 * <p>The vocabulary is frozen by {@code docs/01-fix42-messages-and-state-machine.md} §2. Enum-ish
 * values travel the wire as FIX codes; the readable names produced here are only used for the
 * expected-state export consumed by the integration test.
 */
public final class FixTags {

    private FixTags() {}

    public static final String BEGIN_STRING_FIX42 = "FIX.4.2";

    /** Field separator (SOH). */
    public static final char SOH = '\u0001';

    /** Display separator used by {@code --dry-run} and by the Deephaven audit table. */
    public static final char PIPE = '|';

    public static final int BEGIN_STRING = 8;
    public static final int BODY_LENGTH = 9;
    public static final int MSG_TYPE = 35;
    public static final int SENDER_COMP_ID = 49;
    public static final int TARGET_COMP_ID = 56;
    public static final int MSG_SEQ_NUM = 34;
    public static final int SENDING_TIME = 52;
    public static final int CHECK_SUM = 10;

    public static final int ACCOUNT = 1;
    public static final int AVG_PX = 6;
    public static final int CL_ORD_ID = 11;
    public static final int CUM_QTY = 14;
    public static final int EXEC_ID = 17;
    public static final int EXEC_REF_ID = 19;
    public static final int EXEC_TRANS_TYPE = 20;
    public static final int HANDL_INST = 21;
    public static final int LAST_MKT = 30;
    public static final int LAST_PX = 31;
    public static final int LAST_SHARES = 32;
    public static final int ORDER_ID = 37;
    public static final int ORDER_QTY = 38;
    public static final int ORD_STATUS = 39;
    public static final int ORD_TYPE = 40;
    public static final int ORIG_CL_ORD_ID = 41;
    public static final int PRICE = 44;
    public static final int SIDE = 54;
    public static final int SYMBOL = 55;
    public static final int TEXT = 58;
    public static final int TIME_IN_FORCE = 59;
    public static final int TRANSACT_TIME = 60;
    public static final int ORD_REJ_REASON = 103;
    public static final int CXL_REJ_REASON = 102;
    public static final int DK_REASON = 127;
    public static final int EXEC_TYPE = 150;
    public static final int LEAVES_QTY = 151;
    public static final int CXL_REJ_RESPONSE_TO = 434;

    public static final String MSG_NEW_ORDER_SINGLE = "D";
    public static final String MSG_CANCEL_REPLACE_REQUEST = "G";
    public static final String MSG_CANCEL_REQUEST = "F";
    public static final String MSG_EXECUTION_REPORT = "8";
    public static final String MSG_CANCEL_REJECT = "9";
    public static final String MSG_DONT_KNOW_TRADE = "Q";

    public static final String ORD_STATUS_NEW = "0";
    public static final String ORD_STATUS_PARTIALLY_FILLED = "1";
    public static final String ORD_STATUS_FILLED = "2";
    public static final String ORD_STATUS_CANCELED = "4";
    public static final String ORD_STATUS_PENDING_CANCEL = "6";
    public static final String ORD_STATUS_REJECTED = "8";
    public static final String ORD_STATUS_PENDING_NEW = "A";
    public static final String ORD_STATUS_PENDING_REPLACE = "E";

    public static final String EXEC_TYPE_NEW = "0";
    public static final String EXEC_TYPE_PARTIAL_FILL = "1";
    public static final String EXEC_TYPE_FILL = "2";
    public static final String EXEC_TYPE_CANCELED = "4";
    public static final String EXEC_TYPE_REPLACED = "5";
    public static final String EXEC_TYPE_PENDING_CANCEL = "6";
    public static final String EXEC_TYPE_REJECTED = "8";
    public static final String EXEC_TYPE_PENDING_NEW = "A";
    public static final String EXEC_TYPE_PENDING_REPLACE = "E";

    public static final String EXEC_TRANS_NEW = "0";
    public static final String EXEC_TRANS_CANCEL = "1";
    public static final String EXEC_TRANS_CORRECT = "2";

    public static final String CXL_REJ_RESPONSE_TO_CANCEL = "1";
    public static final String CXL_REJ_RESPONSE_TO_REPLACE = "2";

    public static final String SENDER_VENUE = "MOCKVENUE";
    public static final String SENDER_CLIENT = "CLIENT";

    /** Maps a FIX {@code 39 OrdStatus} code to the readable name used by the Deephaven cache. */
    public static String ordStatusName(String code) {
        return switch (code) {
            case "0" -> "NEW";
            case "1" -> "PARTIALLY_FILLED";
            case "2" -> "FILLED";
            case "3" -> "DONE_FOR_DAY";
            case "4" -> "CANCELED";
            case "5" -> "REPLACED";
            case "6" -> "PENDING_CANCEL";
            case "8" -> "REJECTED";
            case "A" -> "PENDING_NEW";
            case "C" -> "EXPIRED";
            case "E" -> "PENDING_REPLACE";
            default -> throw new IllegalArgumentException("unknown OrdStatus code: " + code);
        };
    }
}
