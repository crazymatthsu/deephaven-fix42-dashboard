package com.deephaven.fix42.oms;

import com.deephaven.fix42.codec.FixConstants;
import com.deephaven.fix42.codec.FixMessage;
import com.deephaven.fix42.codec.Tags;

final class OrderStateUpdater {
    private final CacheConfig config;

    OrderStateUpdater(CacheConfig config) {
        this.config = config;
    }

    /**
     * @return true if economic / status fields were applied
     */
    boolean apply(OrderState state, FixMessage msg) {
        String type = msg.msgType();
        state.setLastMsgType(type);
        state.setLastUpdateEpochMs(System.currentTimeMillis());
        return switch (type) {
            case FixConstants.MSG_NEW_ORDER_SINGLE -> applyNew(state, msg);
            case FixConstants.MSG_EXECUTION_REPORT -> applyExec(state, msg);
            case FixConstants.MSG_ORDER_CANCEL_REPLACE -> applyReplaceRequest(state, msg);
            case FixConstants.MSG_ORDER_CANCEL_REQUEST -> applyCancelRequest(state, msg);
            case FixConstants.MSG_ORDER_CANCEL_REJECT -> applyCancelReject(state, msg);
            case FixConstants.MSG_DONT_KNOW_TRADE -> applyDk(state, msg);
            case FixConstants.MSG_ORDER_STATUS_REQUEST -> false;
            default -> throw new UnsupportedMessageTypeException(type);
        };
    }

    private boolean applyNew(OrderState state, FixMessage msg) {
        mergeInstruction(state, msg);
        mergeIdentityHints(state, msg);
        mergeParent(state, msg);
        if (state.getOrdStatus().isEmpty()) {
            state.setOrdStatus("A");
        }
        if (state.hasOrderQty() && !state.hasLeavesQty()) {
            state.setLeavesQty(state.getOrderQty());
        }
        return true;
    }

    private boolean applyExec(OrderState state, FixMessage msg) {
        String incomingTx = FixValues.str(msg, Tags.TRANSACT_TIME);
        if (!config.applyStaleExecReports()
                && !incomingTx.isEmpty()
                && !state.getTransactTime().isEmpty()
                && FixValues.compareTransactTime(incomingTx, state.getTransactTime()) < 0) {
            mergeIdentityHints(state, msg);
            bindExecId(state, msg);
            return false;
        }

        String execId = FixValues.str(msg, Tags.EXEC_ID);
        String trans = FixValues.str(msg, Tags.EXEC_TRANS_TYPE);
        if (trans.isEmpty()) {
            trans = "0";
        }
        String execKey = execId + ":" + trans;
        boolean duplicate = !execId.isEmpty() && state.getSeenExecKeys().contains(execKey);
        mergeIdentityHints(state, msg);
        bindExecId(state, msg);
        if (duplicate) {
            return false;
        }
        if (!execId.isEmpty()) {
            state.getSeenExecKeys().add(execKey);
        }

        FixValues.mergeString(state::setExecType, FixValues.str(msg, Tags.EXEC_TYPE));
        FixValues.mergeString(state::setExecTransType, trans);
        FixValues.mergeString(state::setOrdStatus, FixValues.str(msg, Tags.ORD_STATUS));
        FixValues.mergeString(state::setOrdRejReason, FixValues.str(msg, Tags.ORD_REJ_REASON));
        FixValues.mergeString(state::setText, FixValues.str(msg, Tags.TEXT));
        FixValues.mergeString(state::setTransactTime, incomingTx);
        mergeInstruction(state, msg);

        FixValues.mergeNumber(state::setCumQty, FixValues.number(msg, Tags.CUM_QTY));
        FixValues.mergeNumber(state::setLeavesQty, FixValues.number(msg, Tags.LEAVES_QTY));
        FixValues.mergeNumber(state::setLastQty, FixValues.number(msg, Tags.LAST_SHARES));
        FixValues.mergeNumber(state::setLastPx, FixValues.number(msg, Tags.LAST_PX));
        FixValues.mergeNumber(state::setAvgPx, FixValues.number(msg, Tags.AVG_PX));

        String status = state.getOrdStatus();
        if (!"6".equals(status)) {
            state.setPendingCancel(false);
        }
        if (!"E".equals(status)) {
            state.setPendingReplace(false);
        }
        if (state.isTerminal()) {
            state.setPendingCancel(false);
            state.setPendingReplace(false);
        }

        String execType = state.getExecType();
        if ("5".equals(execType)) {
            String newCl = FixValues.str(msg, Tags.CL_ORD_ID);
            if (!newCl.isEmpty() && !newCl.equals(state.getClOrdId())) {
                rotateClOrdId(state, newCl);
            }
        }
        return true;
    }

    private boolean applyReplaceRequest(OrderState state, FixMessage msg) {
        mergeIdentityHints(state, msg);
        String newCl = FixValues.str(msg, Tags.CL_ORD_ID);
        if (!newCl.isEmpty() && !newCl.equals(state.getClOrdId())) {
            if (!state.getClOrdId().isEmpty() && !state.getClOrdIdHistory().contains(state.getClOrdId())) {
                state.getClOrdIdHistory().add(state.getClOrdId());
            }
            state.setOrigClOrdId(state.getClOrdId().isEmpty() ? FixValues.str(msg, Tags.ORIG_CL_ORD_ID) : state.getClOrdId());
            state.setClOrdId(newCl);
        }
        if (!state.isTerminal()) {
            state.setPendingReplace(true);
            state.setOrdStatus("E");
        }
        mergeInstruction(state, msg);
        mergeParent(state, msg);
        return true;
    }

    private boolean applyCancelRequest(OrderState state, FixMessage msg) {
        mergeIdentityHints(state, msg);
        String newCl = FixValues.str(msg, Tags.CL_ORD_ID);
        if (!newCl.isEmpty() && !newCl.equals(state.getClOrdId())) {
            if (!state.getClOrdId().isEmpty() && !state.getClOrdIdHistory().contains(state.getClOrdId())) {
                state.getClOrdIdHistory().add(state.getClOrdId());
            }
            state.setOrigClOrdId(state.getClOrdId().isEmpty() ? FixValues.str(msg, Tags.ORIG_CL_ORD_ID) : state.getClOrdId());
            state.setClOrdId(newCl);
        }
        if (!state.isTerminal()) {
            state.setPendingCancel(true);
            state.setOrdStatus("6");
        }
        mergeParent(state, msg);
        return true;
    }

    private boolean applyCancelReject(OrderState state, FixMessage msg) {
        mergeIdentityHints(state, msg);
        FixValues.mergeString(state::setOrdStatus, FixValues.str(msg, Tags.ORD_STATUS));
        FixValues.mergeString(state::setCxlRejReason, FixValues.str(msg, Tags.CXL_REJ_REASON));
        FixValues.mergeString(state::setCxlRejResponseTo, FixValues.str(msg, Tags.CXL_REJ_RESPONSE_TO));
        FixValues.mergeString(state::setText, FixValues.str(msg, Tags.TEXT));
        String resp = FixValues.str(msg, Tags.CXL_REJ_RESPONSE_TO);
        if ("1".equals(resp)) {
            state.setPendingCancel(false);
        } else if ("2".equals(resp)) {
            state.setPendingReplace(false);
        } else {
            state.setPendingCancel(false);
            state.setPendingReplace(false);
        }
        return true;
    }

    private boolean applyDk(OrderState state, FixMessage msg) {
        mergeIdentityHints(state, msg);
        bindExecId(state, msg);
        state.setDkTrade(true);
        FixValues.mergeString(state::setDkReason, FixValues.str(msg, Tags.DK_REASON));
        FixValues.mergeString(state::setText, FixValues.str(msg, Tags.TEXT));
        return true;
    }

    private void mergeInstruction(OrderState state, FixMessage msg) {
        FixValues.mergeString(state::setAccount, FixValues.str(msg, Tags.ACCOUNT));
        FixValues.mergeString(state::setSymbol, FixValues.str(msg, Tags.SYMBOL));
        FixValues.mergeString(state::setSecurityId, FixValues.str(msg, Tags.SECURITY_ID));
        FixValues.mergeString(state::setSide, FixValues.str(msg, Tags.SIDE));
        FixValues.mergeString(state::setOrdType, FixValues.str(msg, Tags.ORD_TYPE));
        FixValues.mergeString(state::setTimeInForce, FixValues.str(msg, Tags.TIME_IN_FORCE));
        FixValues.mergeNumber(state::setOrderQty, FixValues.number(msg, Tags.ORDER_QTY));
        FixValues.mergeNumber(state::setPrice, FixValues.number(msg, Tags.PRICE));
        FixValues.mergeNumber(state::setStopPx, FixValues.number(msg, Tags.STOP_PX));
        // Client request TransactTime (D/G/F) must not become the venue watermark
        // used to drop stale execution reports.
        if (state.getTransactTime().isEmpty()) {
            FixValues.mergeString(state::setTransactTime, FixValues.str(msg, Tags.TRANSACT_TIME));
        }
    }

    private void mergeIdentityHints(OrderState state, FixMessage msg) {
        FixValues.mergeString(state::setOrderId, FixValues.str(msg, Tags.ORDER_ID));
        FixValues.mergeString(state::setSecondaryOrderId, FixValues.str(msg, Tags.SECONDARY_ORDER_ID));
        String cl = FixValues.str(msg, Tags.CL_ORD_ID);
        if (!cl.isEmpty() && state.getClOrdId().isEmpty()) {
            state.setClOrdId(cl);
        }
        FixValues.mergeString(state::setOrigClOrdId, FixValues.str(msg, Tags.ORIG_CL_ORD_ID));
    }

    private void mergeParent(OrderState state, FixMessage msg) {
        FixValues.mergeString(state::setParentOrderId, FixValues.str(msg, config.parentOrderIdTag()));
        FixValues.mergeString(state::setParentClOrdId, FixValues.str(msg, config.parentClOrdIdTag()));
    }

    private static void bindExecId(OrderState state, FixMessage msg) {
        String execId = FixValues.str(msg, Tags.EXEC_ID);
        if (!execId.isEmpty()) {
            state.setLastExecId(execId);
        }
    }

    private static void rotateClOrdId(OrderState state, String newCl) {
        if (!state.getClOrdId().isEmpty() && !state.getClOrdIdHistory().contains(state.getClOrdId())) {
            state.getClOrdIdHistory().add(state.getClOrdId());
        }
        state.setOrigClOrdId(state.getClOrdId());
        state.setClOrdId(newCl);
    }
}
