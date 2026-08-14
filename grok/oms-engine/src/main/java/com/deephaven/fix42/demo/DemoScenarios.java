package com.deephaven.fix42.demo;

import com.deephaven.fix42.codec.FixField;
import com.deephaven.fix42.codec.FixSerializer;
import com.deephaven.fix42.codec.Tags;

import java.util.ArrayList;
import java.util.List;

/**
 * Scripted drop-copy used by unit/integration tests and the Kafka demo producer.
 *
 * <p>Covers: parent/child new, new ack, partial/full fill, amend ack, cancel
 * reject then cancel ack, don't-know trade.
 */
public final class DemoScenarios {
    private static final FixSerializer SERIALIZER = new FixSerializer();

    private DemoScenarios() {}

    public static List<String> allMessages() {
        List<String> out = new ArrayList<>();
        out.add(d("P1", "PROP", "MSFT", "1000", "420", null, null));
        out.add(d("C1", "PROP", "MSFT", "400", "420", "P1", "P1"));
        out.add(er("C1", "B1", "E1", "0", "0", "0", "400", "0", null, null, "0", "60=20260814-13:00:01"));
        out.add(g("C1b", "C1", "B1", "400", "421"));
        out.add(er("C1b", "B1", "E1p", "E", "E", "0", "400", "0", "C1", null, "0", "60=20260814-13:01:01"));
        out.add(er("C1b", "B1", "E1r", "5", "0", "0", "400", "0", "C1", "421", "0", "60=20260814-13:01:02"));
        out.add(er("C1b", "B1", "E2", "2", "2", "400", "0", "420", null, null, "0", "32=400|31=420|60=20260814-13:01:03"));
        out.add(q("B1", "E2", "D", "MSFT"));
        out.add(d("C2", "PROP", "MSFT", "600", "420", "P1", "P1"));
        out.add(er("C2", "B2", "E3", "0", "0", "0", "600", "0", null, null, "0", null));
        out.add(g("C3", "C2", "B2", "500", "419"));
        out.add(cancelReject("C3", "C2", "B2", "0", "2"));
        out.add(f("CX2", "C2", "B2"));
        out.add(er("CX2", "B2", "E4", "4", "4", "0", "0", "0", "C2", null, "0", null));
        return out;
    }

    public static String topic() {
        return "fix42.dropcopy";
    }

    static String d(
            String cl, String acct, String sym, String qty, String px, String parentOrder, String parentCl) {
        List<FixField> fields = base("D");
        add(fields, Tags.CL_ORD_ID, cl);
        add(fields, Tags.ACCOUNT, acct);
        add(fields, Tags.SYMBOL, sym);
        add(fields, Tags.SIDE, "1");
        add(fields, Tags.ORDER_QTY, qty);
        add(fields, Tags.ORD_TYPE, "2");
        add(fields, Tags.PRICE, px);
        add(fields, Tags.TIME_IN_FORCE, "0");
        add(fields, Tags.TRANSACT_TIME, "20260814-13:00:00");
        add(fields, Tags.HANDL_INST, "1");
        if (parentOrder != null) {
            add(fields, Tags.PARENT_ORDER_ID, parentOrder);
        }
        if (parentCl != null) {
            add(fields, Tags.PARENT_CL_ORD_ID, parentCl);
        }
        return pretty(fields);
    }

    static String er(
            String cl,
            String orderId,
            String execId,
            String execType,
            String status,
            String cum,
            String leaves,
            String avg,
            String origCl,
            String px,
            String trans,
            String extras) {
        List<FixField> fields = base("8");
        add(fields, Tags.CL_ORD_ID, cl);
        add(fields, Tags.ORDER_ID, orderId);
        add(fields, Tags.EXEC_ID, execId);
        add(fields, Tags.EXEC_TRANS_TYPE, trans == null ? "0" : trans);
        add(fields, Tags.EXEC_TYPE, execType);
        add(fields, Tags.ORD_STATUS, status);
        add(fields, Tags.CUM_QTY, cum);
        add(fields, Tags.LEAVES_QTY, leaves);
        add(fields, Tags.AVG_PX, avg == null ? "0" : avg);
        boolean extraHasTx = extras != null && extras.contains("60=");
        if (!extraHasTx) {
            add(fields, Tags.TRANSACT_TIME, "20260814-13:00:01");
        }
        if (origCl != null) {
            add(fields, Tags.ORIG_CL_ORD_ID, origCl);
        }
        if (px != null) {
            add(fields, Tags.PRICE, px);
        }
        if (extras != null) {
            for (String pair : extras.split("\\|")) {
                if (pair.isEmpty()) {
                    continue;
                }
                int eq = pair.indexOf('=');
                add(fields, Integer.parseInt(pair.substring(0, eq)), pair.substring(eq + 1));
            }
        }
        return pretty(fields);
    }

    static String g(String cl, String orig, String orderId, String qty, String px) {
        List<FixField> fields = base("G");
        add(fields, Tags.CL_ORD_ID, cl);
        add(fields, Tags.ORIG_CL_ORD_ID, orig);
        add(fields, Tags.ORDER_ID, orderId);
        add(fields, Tags.SYMBOL, "MSFT");
        add(fields, Tags.SIDE, "1");
        add(fields, Tags.ORDER_QTY, qty);
        add(fields, Tags.ORD_TYPE, "2");
        add(fields, Tags.PRICE, px);
        add(fields, Tags.HANDL_INST, "1");
        add(fields, Tags.TRANSACT_TIME, "20260814-13:01:00");
        return pretty(fields);
    }

    static String f(String cl, String orig, String orderId) {
        List<FixField> fields = base("F");
        add(fields, Tags.CL_ORD_ID, cl);
        add(fields, Tags.ORIG_CL_ORD_ID, orig);
        add(fields, Tags.ORDER_ID, orderId);
        add(fields, Tags.SYMBOL, "MSFT");
        add(fields, Tags.SIDE, "1");
        add(fields, Tags.TRANSACT_TIME, "20260814-13:02:00");
        return pretty(fields);
    }

    static String cancelReject(String cl, String orig, String orderId, String status, String resp) {
        List<FixField> fields = base("9");
        add(fields, Tags.CL_ORD_ID, cl);
        add(fields, Tags.ORIG_CL_ORD_ID, orig);
        add(fields, Tags.ORDER_ID, orderId);
        add(fields, Tags.ORD_STATUS, status);
        add(fields, Tags.CXL_REJ_RESPONSE_TO, resp);
        add(fields, Tags.CXL_REJ_REASON, "0");
        add(fields, Tags.TEXT, "replace rejected");
        return pretty(fields);
    }

    static String q(String orderId, String execId, String reason, String symbol) {
        List<FixField> fields = base("Q");
        add(fields, Tags.ORDER_ID, orderId);
        add(fields, Tags.EXEC_ID, execId);
        add(fields, Tags.DK_REASON, reason);
        add(fields, Tags.SYMBOL, symbol);
        add(fields, Tags.SIDE, "1");
        add(fields, Tags.TEXT, "don't know this fill");
        return pretty(fields);
    }

    private static List<FixField> base(String msgType) {
        List<FixField> fields = new ArrayList<>();
        add(fields, Tags.MSG_TYPE, msgType);
        add(fields, Tags.SENDER_COMP_ID, "DROPCOPY");
        add(fields, Tags.TARGET_COMP_ID, "OMS");
        add(fields, Tags.MSG_SEQ_NUM, "1");
        add(fields, Tags.SENDING_TIME, "20260814-13:00:00");
        return fields;
    }

    private static void add(List<FixField> fields, int tag, String value) {
        fields.add(new FixField(tag, value));
    }

    private static String pretty(List<FixField> fields) {
        return SERIALIZER.serializePretty(fields);
    }
}
