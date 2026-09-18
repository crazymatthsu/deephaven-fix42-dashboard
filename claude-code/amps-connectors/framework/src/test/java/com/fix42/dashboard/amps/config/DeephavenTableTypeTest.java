package com.fix42.dashboard.amps.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.mapping.TableSchema;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DeephavenTableTypeTest {

    @Test
    @DisplayName("an unset table-type follows the topic, as it did before the setting existed")
    void defaultsToWhatTheTopicImplies() {
        assertThat(DeephavenTableType.resolve(null, true)).isEqualTo(DeephavenTableType.KEYED);
        assertThat(DeephavenTableType.resolve(null, false))
                .isEqualTo(DeephavenTableType.APPEND_ONLY);
    }

    @Test
    void aConfiguredTypeWins() {
        assertThat(DeephavenTableType.resolve(DeephavenTableType.BLINK, true))
                .isEqualTo(DeephavenTableType.BLINK);
        assertThat(DeephavenTableType.resolve(DeephavenTableType.KEYED, false))
                .isEqualTo(DeephavenTableType.KEYED);
    }

    @Test
    @DisplayName("only KEYED is keyed; only BLINK and RING go through a publisher")
    void classifiesTheTypes() {
        assertThat(DeephavenTableType.KEYED.keyed()).isTrue();
        assertThat(DeephavenTableType.APPEND_ONLY.keyed()).isFalse();
        assertThat(DeephavenTableType.BLINK.keyed()).isFalse();
        assertThat(DeephavenTableType.RING.keyed()).isFalse();

        assertThat(DeephavenTableType.KEYED.publisherBacked()).isFalse();
        assertThat(DeephavenTableType.APPEND_ONLY.publisherBacked()).isFalse();
        assertThat(DeephavenTableType.BLINK.publisherBacked()).isTrue();
        assertThat(DeephavenTableType.RING.publisherBacked()).isTrue();

        assertThat(DeephavenTableType.RING.bounded()).isTrue();
        assertThat(DeephavenTableType.BLINK.bounded()).isFalse();
    }

    @Test
    @DisplayName("the schema drops key columns a non-keyed type could not use")
    void schemaIsSelfConsistentForEveryType() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getDeephaven().setTableType(DeephavenTableType.BLINK);

        TableSchema schema = TableSchema.of(connector);
        assertThat(schema.tableType()).isEqualTo(DeephavenTableType.BLINK);
        assertThat(schema.keyed()).isFalse();
        assertThat(schema.keyColumns()).isEmpty();
        assertThat(schema.rowKey(new Object[schema.size()])).isNull();
        // The columns themselves are untouched: only the keys go.
        assertThat(schema.columns()).extracting(c -> c.name())
                .containsExactly("ClOrdID", "Symbol", "OrderQty", "Price", "TransactTime");
    }

    @Test
    void ringCapacityReachesTheSchema() {
        TableSchema schema = TableSchema.of(TestConnectors.jsonTicksRing(250));
        assertThat(schema.tableType()).isEqualTo(DeephavenTableType.RING);
        assertThat(schema.ringCapacity()).isEqualTo(250);
    }

    @Test
    @DisplayName("a journal topic can still be keyed when the configuration says so")
    void anExplicitKeyedTypeOverAJournalTopic() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getDeephaven().setTableType(DeephavenTableType.KEYED);
        connector.getDeephaven().setKeyColumns(List.of("TradeID"));

        TableSchema schema = TableSchema.of(connector);
        assertThat(schema.keyed()).isTrue();
        assertThat(schema.keyColumns()).containsExactly("TradeID");
    }
}
