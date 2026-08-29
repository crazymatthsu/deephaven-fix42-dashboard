package com.fix42.dashboard.amps;

import com.fix42.dashboard.amps.config.AmpsSourceProperties;
import com.fix42.dashboard.amps.config.ColumnType;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.DeephavenTableProperties;
import com.fix42.dashboard.amps.config.DeephavenTableType;
import com.fix42.dashboard.amps.config.FieldMapping;
import com.fix42.dashboard.amps.config.SourceFormat;
import com.fix42.dashboard.amps.config.UpdateMode;
import java.util.List;

/** Builders for the connector configurations the tests exercise. */
public final class TestConnectors {

    /** SOH, the default FIX/NVFIX field separator. */
    public static final char SOH = (char) 0x01;

    private TestConnectors() {
    }

    /** A FIX connector over a SOW topic, keyed by {@code ClOrdID}. */
    public static ConnectorProperties fixOrders() {
        ConnectorProperties connector = base("orders-fix", SourceFormat.FIX, "Orders", true);
        connector.getDeephaven().setTable("amps_orders");
        connector.getDeephaven().setKeyColumns(List.of("ClOrdID"));
        connector.setFields(List.of(
                field("11", "ClOrdID", ColumnType.STRING),
                field("55", "Symbol", ColumnType.STRING),
                field("38", "OrderQty", ColumnType.DOUBLE),
                field("44", "Price", ColumnType.DOUBLE),
                field("60", "TransactTime", ColumnType.INSTANT)));
        return connector;
    }

    /** An NVFIX connector over a SOW topic, delta end to end, keyed by account and symbol. */
    public static ConnectorProperties nvfixPositions() {
        ConnectorProperties connector = base("positions-nvfix", SourceFormat.NVFIX, "Positions", true);
        connector.getSource().setSubscriptionMode(UpdateMode.DELTA);
        DeephavenTableProperties target = connector.getDeephaven();
        target.setTable("amps_positions");
        target.setKeyColumns(List.of("Account", "Symbol"));
        target.setPublishMode(UpdateMode.DELTA);
        connector.setFields(List.of(
                field("Account", "Account", ColumnType.STRING),
                field("Symbol", "Symbol", ColumnType.STRING),
                field("Quantity", "Quantity", ColumnType.DOUBLE),
                field("AvgCost", "AvgCost", ColumnType.DOUBLE)));
        return connector;
    }

    /** A JSON connector over a journal topic, publishing into an append-only table. */
    public static ConnectorProperties jsonTrades() {
        ConnectorProperties connector = base("trades-json", SourceFormat.JSON, "Trades", false);
        connector.getDeephaven().setTable("amps_trades");
        connector.setFields(List.of(
                field("tradeId", "TradeID", ColumnType.STRING),
                field("symbol", "Symbol", ColumnType.STRING),
                field("quantity", "Quantity", ColumnType.LONG),
                field("execution.venue", "Venue", ColumnType.STRING)));
        return connector;
    }

    /** A JSON connector over a journal topic, published into a blink table. */
    public static ConnectorProperties jsonTicks() {
        ConnectorProperties connector = base("ticks-json", SourceFormat.JSON, "Ticks", false);
        connector.getDeephaven().setTable("amps_ticks");
        connector.getDeephaven().setTableType(DeephavenTableType.BLINK);
        connector.setFields(List.of(
                field("symbol", "Symbol", ColumnType.STRING),
                field("price", "Price", ColumnType.DOUBLE)));
        return connector;
    }

    /** The same feed kept as a bounded tail instead: a ring table over the blink table. */
    public static ConnectorProperties jsonTicksRing(int capacity) {
        ConnectorProperties connector = jsonTicks();
        connector.setName("ticks-json-ring");
        connector.getDeephaven().setTable("amps_ticks_ring");
        connector.getDeephaven().setTableType(DeephavenTableType.RING);
        connector.getDeephaven().setRingCapacity(capacity);
        return connector;
    }

    /** A field mapping. */
    public static FieldMapping field(String tag, String column, ColumnType type) {
        FieldMapping mapping = new FieldMapping();
        mapping.setTag(tag);
        mapping.setColumn(column);
        mapping.setType(type);
        return mapping;
    }

    private static ConnectorProperties base(
            String name, SourceFormat format, String topic, boolean sow) {
        ConnectorProperties connector = new ConnectorProperties();
        connector.setName(name);
        connector.setFormat(format);
        AmpsSourceProperties source = new AmpsSourceProperties();
        source.setTopic(topic);
        source.setSow(sow);
        source.setDriver(AmpsSourceProperties.Driver.SIMULATED);
        connector.setSource(source);
        connector.setDeephaven(new DeephavenTableProperties());
        return connector;
    }

    /** Render {@code tag=value} pairs as a SOH-delimited FIX/NVFIX payload. */
    public static String delimited(String... tagsAndValues) {
        StringBuilder payload = new StringBuilder();
        for (int i = 0; i < tagsAndValues.length; i += 2) {
            payload.append(tagsAndValues[i]).append('=').append(tagsAndValues[i + 1]).append(SOH);
        }
        return payload.toString();
    }
}
