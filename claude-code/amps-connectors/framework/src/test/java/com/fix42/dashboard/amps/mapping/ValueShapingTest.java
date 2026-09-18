package com.fix42.dashboard.amps.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.config.ColumnType;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.FieldMapping;
import com.fix42.dashboard.amps.config.FixValueDecode;
import com.fix42.dashboard.amps.source.AmpsRecord;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** {@code decode} / {@code values} / {@code default-value}: doc 07 section 5.2. */
class ValueShapingTest {

    private static final Instant INGEST = Instant.parse("2024-01-15T14:30:00Z");

    private final TableSchema schema = TableSchema.of(TestConnectors.fixShapedOrders());
    private final FieldMapper mapper = new FieldMapper(schema);

    private MappedRow map(Map<String, String> fields) {
        return mapper.map(AmpsRecord.of("payload", "SOW-1"), fields, INGEST);
    }

    private Object valueOf(MappedRow row, String column) {
        return row.values()[schema.indexOf(column)];
    }

    @Test
    @DisplayName("54=1 publishes as BUY")
    void decodesABuiltInFixTable() {
        assertThat(valueOf(map(Map.of("11", "C-1", "54", "1")), "Side")).isEqualTo("BUY");
        assertThat(valueOf(map(Map.of("11", "C-1", "54", "2")), "Side")).isEqualTo("SELL");
        assertThat(valueOf(map(Map.of("11", "C-1", "54", "5")), "Side")).isEqualTo("SELL_SHORT");
        assertThat(valueOf(map(Map.of("11", "C-1", "39", "E")), "OrdStatus"))
                .isEqualTo("PENDING_REPLACE");
    }

    @Test
    @DisplayName("a code the table does not name stays visible rather than becoming null")
    void passesUnknownCodesThrough() {
        assertThat(valueOf(map(Map.of("11", "C-1", "54", "Q")), "Side")).isEqualTo("Q");
    }

    @Test
    @DisplayName("inline values are applied over the named table")
    void inlineValuesOverrideTheNamedTable() {
        // Z is not a FIX 4.2 OrdStatus; the connector's inline map names it anyway.
        assertThat(valueOf(map(Map.of("11", "C-1", "39", "Z")), "OrdStatus"))
                .isEqualTo("VENUE_HELD");
        // ...and the rest of the built-in table still applies.
        assertThat(valueOf(map(Map.of("11", "C-1", "39", "2")), "OrdStatus")).isEqualTo("FILLED");
    }

    @Test
    @DisplayName("an inline map alone needs no built-in table, and is not FIX-specific")
    void inlineValuesWithoutADecode() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        FieldMapping side = TestConnectors.field("side", "Side", ColumnType.STRING);
        side.setValues(Map.of("B", "BUY", "S", "SELL"));
        connector.setFields(List.of(TestConnectors.field("tradeId", "TradeID", ColumnType.STRING),
                side));
        TableSchema shaped = TableSchema.of(connector);

        MappedRow row = new FieldMapper(shaped).map(
                AmpsRecord.of("payload"), Map.of("tradeId", "T-1", "side", "S"), INGEST);
        assertThat(row.values()[shaped.indexOf("Side")]).isEqualTo("SELL");
    }

    @Test
    @DisplayName("a rewrite happens before coercion, so it can feed a non-string column")
    void rewriteFeedsTheCoercion() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        FieldMapping flag = TestConnectors.field("flag", "Flag", ColumnType.INT);
        flag.setValues(Map.of("Y", "1", "N", "0"));
        connector.setFields(List.of(TestConnectors.field("tradeId", "TradeID", ColumnType.STRING),
                flag));
        TableSchema shaped = TableSchema.of(connector);

        MappedRow row = new FieldMapper(shaped).map(
                AmpsRecord.of("payload"), Map.of("tradeId", "T-1", "flag", "Y"), INGEST);
        assertThat(row.values()[shaped.indexOf("Flag")]).isEqualTo(1);
    }

    @Test
    @DisplayName("an absent field publishes its default, coerced to the column type")
    void defaultsAnAbsentField() {
        MappedRow row = map(Map.of("11", "C-1"));

        assertThat(valueOf(row, "Account")).isEqualTo("DUMMY");
        assertThat(valueOf(row, "OrderQty")).isEqualTo(0.0d);
        // No default configured, so the column is still null.
        assertThat(valueOf(row, "Side")).isNull();
    }

    @Test
    @DisplayName("the payload wins over the default whenever it carries the field")
    void thePayloadWinsOverTheDefault() {
        assertThat(valueOf(map(Map.of("11", "C-1", "1", "ACC-9")), "Account")).isEqualTo("ACC-9");
    }

    @Test
    @DisplayName("a field sent empty is an explicit clear, not an absent field")
    void doesNotDefaultAnExplicitClear() {
        Map<String, String> fields = new HashMap<>();
        fields.put("11", "C-1");
        fields.put("1", "");
        MappedRow row = map(fields);

        assertThat(valueOf(row, "Account")).isNull();
        assertThat(row.present()[schema.indexOf("Account")]).isTrue();
    }

    @Test
    @DisplayName("a default fills the value but is not marked present")
    void aDefaultIsNotPresence() {
        MappedRow row = map(Map.of("11", "C-1"));

        assertThat(valueOf(row, "Account")).isEqualTo("DUMMY");
        assertThat(row.present()[schema.indexOf("Account")])
                .as("a default is not the payload speaking")
                .isFalse();
    }

    @Test
    @DisplayName("...so a later delta does not overwrite a stored value with the default")
    void aDefaultSeedsButNeverClobbers() {
        ConnectorProperties connector = TestConnectors.nvfixPositions();
        FieldMapping currency = TestConnectors.field("Currency", "Currency", ColumnType.STRING);
        currency.setDefaultValue("USD");
        connector.setFields(List.of(
                TestConnectors.field("Account", "Account", ColumnType.STRING),
                TestConnectors.field("Symbol", "Symbol", ColumnType.STRING),
                TestConnectors.field("Quantity", "Quantity", ColumnType.DOUBLE),
                currency));
        TableSchema delta = TableSchema.of(connector);
        FieldMapper deltaMapper = new FieldMapper(delta);
        DeltaRowMerger merger = new DeltaRowMerger(delta);

        // The key's first message omits Currency, so the default seeds the base row.
        MappedRow first = merger.merge(deltaMapper.map(AmpsRecord.of("p", "K"),
                Map.of("Account", "ACC-1", "Symbol", "AAPL", "Quantity", "100"), INGEST));
        assertThat(first.values()[delta.indexOf("Currency")]).isEqualTo("USD");

        // A later message sets it explicitly...
        merger.merge(deltaMapper.map(AmpsRecord.of("p", "K"),
                Map.of("Account", "ACC-1", "Symbol", "AAPL", "Currency", "EUR"), INGEST));

        // ...and a delta that omits it again must keep EUR, not fall back to the default.
        MappedRow third = merger.merge(deltaMapper.map(AmpsRecord.of("p", "K"),
                Map.of("Account", "ACC-1", "Symbol", "AAPL", "Quantity", "150"), INGEST));
        assertThat(third.values()[delta.indexOf("Currency")]).isEqualTo("EUR");
        assertThat(third.values()[delta.indexOf("Quantity")]).isEqualTo(150.0d);
    }

    @Test
    @DisplayName("a default that does not coerce is refused when the schema is resolved")
    void refusesADefaultThatDoesNotCoerce() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        FieldMapping price = TestConnectors.field("price", "Price", ColumnType.DOUBLE);
        price.setDefaultValue("not-a-number");
        connector.setFields(List.of(price));

        assertThatThrownBy(() -> TableSchema.of(connector))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column Price")
                .hasMessageContaining("default-value")
                .hasMessageContaining("not a valid DOUBLE");
    }

    @Test
    @DisplayName("the built-in tables cover the tags people actually read")
    void builtInTables() {
        assertThat(FixValueDecode.SIDE.table()).containsEntry("1", "BUY").containsEntry("2", "SELL");
        assertThat(FixValueDecode.MSG_TYPE.table())
                .containsEntry("D", "NEW_ORDER_SINGLE")
                .containsEntry("8", "EXECUTION_REPORT");
        assertThat(FixValueDecode.EXEC_TYPE.table()).containsEntry("D", "RESTATED");
        for (FixValueDecode decode : FixValueDecode.values()) {
            assertThat(decode.table()).as("%s is populated", decode).isNotEmpty();
            assertThat(decode.table().values()).as("%s names are distinct", decode)
                    .doesNotHaveDuplicates();
        }
    }
}
