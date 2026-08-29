package com.fix42.dashboard.dh;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.util.SafeCloseable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Declarative derived nodes -- doc 03 section 2.4, and the Java port of {@code dh_app.dag}.
 *
 * <p>Everything here is a pure, incrementally-computed table operation over the five blink streams
 * published by {@link Fix42Pipeline}. No state, no callbacks: late and duplicate data is already
 * resolved inside {@code fixcache}.
 *
 * <p>Node names are the frozen globals the dashboard, the query API and the integration test bind
 * to ({@link Names#DERIVED_TABLES}).
 */
public final class Fix42Dag {

    private Fix42Dag() {}

    /**
     * Builds the derived DAG from the publisher blink tables.
     *
     * @param streams the blink tables keyed by their doc 03 section 2.3 names
     * @return a map keyed by {@link Names#DERIVED_TABLES}:
     *     <dl>
     *       <dt>{@code order_state_latest}
     *       <dd>THE cache: {@code lastBy("OrderKey")} over the blink snapshot stream, so memory is
     *           O(#orders) rather than O(#messages) (doc 02 section 1.1).
     *       <dt>{@code executions} / {@code order_events} / {@code fix_messages}
     *       <dd>Append-only history for the panels and the audit trail.
     *       <dt>{@code executions_latest}
     *       <dd>{@code lastBy("ExecID")} -- current disposition after bust/correct/DK.
     *       <dt>{@code clordid_index} / {@code execid_index}
     *       <dd>Alias to {@code OrderKey} resolution for the query API.
     *       <dt>{@code status_summary} / {@code symbol_summary} / {@code account_list}
     *       <dd>Dashboard summary bar and filter sources.
     *       <dt>{@code open_orders}
     *       <dd>Non-terminal orders.
     *     </dl>
     */
    public static Map<String, Table> buildDerived(Map<String, Table> streams) {
        Table orderStateBlink = streams.get(Names.ORDER_STATE_BLINK);
        // lastBy/aggBy/countBy/selectDistinct/where/view/sort/blinkToAppendOnly are all safe from a
        // non-update-graph thread, but holding the shared lock across the whole build costs nothing
        // and removes the question entirely.
        try (SafeCloseable ignored = orderStateBlink.getUpdateGraph().sharedLock().lockCloseable()) {
            return build(streams, orderStateBlink);
        }
    }

    private static Map<String, Table> build(Map<String, Table> streams, Table orderStateBlink) {
        Table executionsBlink = streams.get(Names.EXECUTIONS_BLINK);
        Table orderEventsBlink = streams.get(Names.ORDER_EVENTS_BLINK);
        Table fixMessagesBlink = streams.get(Names.FIX_MESSAGES_BLINK);

        Table orderStateLatest = orderStateBlink.lastBy("OrderKey");
        Table executions = BlinkTableTools.blinkToAppendOnly(executionsBlink);
        Table executionsLatest = executionsBlink.lastBy("ExecID");
        Table orderEvents = BlinkTableTools.blinkToAppendOnly(orderEventsBlink);
        Table fixMessages = BlinkTableTools.blinkToAppendOnly(fixMessagesBlink);

        Table clOrdIdIndex = orderEvents
                .where("ClOrdID != ``")
                .lastBy("ClOrdID")
                .view("ClOrdID", "OrderKey");
        Table execIdIndex = executionsLatest.where("ExecID != ``").view("ExecID", "OrderKey");

        Table statusSummary = orderStateLatest.countBy("Count", "OrdStatus").sort("OrdStatus");
        Table symbolSummary = orderStateLatest.aggBy(
                List.of(Aggregation.AggCount("Orders"), Aggregation.AggSum("CumQty", "OrderQty")), "Symbol");
        Table openOrders = orderStateLatest.where("!Terminal");
        Table accountList = orderStateLatest.selectDistinct("Account").sort("Account");

        Map<String, Table> derived = new LinkedHashMap<>();
        derived.put(Names.ORDER_STATE_LATEST, orderStateLatest);
        derived.put(Names.EXECUTIONS, executions);
        derived.put(Names.EXECUTIONS_LATEST, executionsLatest);
        derived.put(Names.ORDER_EVENTS, orderEvents);
        derived.put(Names.FIX_MESSAGES, fixMessages);
        derived.put(Names.CLORDID_INDEX, clOrdIdIndex);
        derived.put(Names.EXECID_INDEX, execIdIndex);
        derived.put(Names.STATUS_SUMMARY, statusSummary);
        derived.put(Names.SYMBOL_SUMMARY, symbolSummary);
        derived.put(Names.OPEN_ORDERS, openOrders);
        derived.put(Names.ACCOUNT_LIST, accountList);
        return Collections.unmodifiableMap(derived);
    }
}
