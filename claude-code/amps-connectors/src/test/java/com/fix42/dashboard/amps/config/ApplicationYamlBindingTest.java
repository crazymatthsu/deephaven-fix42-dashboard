package com.fix42.dashboard.amps.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.amps.mapping.TableSchema;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * The shipped application.yml binds, validates, and means what the docs say it means.
 *
 * <p>{@code amps.enabled=false} keeps the lifecycle monitor from dialling a Deephaven server:
 * this test is about the configuration model, not about running connectors.
 */
@SpringBootTest(properties = "amps.enabled=false")
@ActiveProfiles("demo")
class ApplicationYamlBindingTest {

    @Autowired
    private AmpsConnectorsProperties properties;

    @Test
    void bindsEveryExampleConnector() {
        assertThat(properties.getConnectors()).extracting(ConnectorProperties::getName)
                .containsExactly("orders-fix", "positions-nvfix", "trades-json", "ticks-json",
                        "portfolios-json", "orders-composite");
    }

    @Test
    void bindsTheDeephavenServerSettings() {
        DeephavenServerProperties deephaven = properties.getDeephaven();
        assertThat(deephaven.getHost()).isEqualTo("localhost");
        assertThat(deephaven.getPort()).isEqualTo(10_000);
        assertThat(deephaven.getAuthentication()).isEqualTo("Anonymous");
        assertThat(deephaven.getConsoleType()).isEqualTo("python");
        assertThat(deephaven.getHealthCheckInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(deephaven.target()).isEqualTo("localhost:10000");
    }

    @Test
    @DisplayName("the FIX example is a SOW topic keyed on ClOrdID")
    void bindsTheFixConnector() {
        ConnectorProperties connector = connector("orders-fix");
        assertThat(connector.getFormat()).isEqualTo(SourceFormat.FIX);
        assertThat(connector.getSource().isSow()).isTrue();
        assertThat(connector.getSource().getTopic()).isEqualTo("Orders");
        assertThat(connector.getSource().getSubscriptionMode()).isEqualTo(UpdateMode.FULL);
        assertThat(connector.getDeephaven().getTable()).isEqualTo("amps_orders");
        assertThat(connector.getDeephaven().getKeyColumns()).containsExactly("ClOrdID");
        assertThat(connector.getDeephaven().isKeyed()).isTrue();
        assertThat(connector.getDeephaven().getTableType()).isEqualTo(DeephavenTableType.KEYED);
        assertThat(connector.getFields()).extracting(FieldMapping::getTag).contains("11", "55", "38");
        assertThat(fieldFor(connector, "60").getType()).isEqualTo(ColumnType.INSTANT);
        assertThat(fieldFor(connector, "38").getType()).isEqualTo(ColumnType.DOUBLE);
    }

    @Test
    @DisplayName("the NVFIX example is delta end to end")
    void bindsTheNvfixConnector() {
        ConnectorProperties connector = connector("positions-nvfix");
        assertThat(connector.getFormat()).isEqualTo(SourceFormat.NVFIX);
        assertThat(connector.getSource().getSubscriptionMode()).isEqualTo(UpdateMode.DELTA);
        assertThat(connector.getDeephaven().getPublishMode()).isEqualTo(UpdateMode.DELTA);
        assertThat(connector.getDeephaven().getKeyColumns()).containsExactly("Account", "Symbol");
    }

    @Test
    @DisplayName("the JSON example is a journal topic with an append-only table")
    void bindsTheJsonConnector() {
        ConnectorProperties connector = connector("trades-json");
        assertThat(connector.getFormat()).isEqualTo(SourceFormat.JSON);
        assertThat(connector.getSource().isSow()).isFalse();
        assertThat(connector.getSource().getBookmark()).isEqualTo("epoch");
        assertThat(connector.getDeephaven().isKeyed()).isFalse();
        assertThat(fieldFor(connector, "execution.venue").getColumn()).isEqualTo("Venue");
    }

    @Test
    @DisplayName("the FIX example decodes its enumerated tags and defaults its account")
    void bindsTheValueShapingKnobs() {
        ConnectorProperties connector = connector("orders-fix");
        assertThat(fieldFor(connector, "54").getDecode()).isEqualTo(FixValueDecode.SIDE);
        assertThat(fieldFor(connector, "39").getDecode()).isEqualTo(FixValueDecode.ORD_STATUS);
        assertThat(fieldFor(connector, "40").getDecode()).isEqualTo(FixValueDecode.ORD_TYPE);
        assertThat(fieldFor(connector, "1").getDefaultValue()).isEqualTo("DUMMY");
        assertThat(fieldFor(connector, "39").getDefaultValue()).isEqualTo("UNKNOWN");
        assertThat(fieldFor(connector, "54").resolveValueTable())
                .containsEntry("1", "BUY")
                .containsEntry("2", "SELL");
    }

    @Test
    @DisplayName("the JSON example spells its side B/S, which an inline values map covers")
    void bindsAnInlineValuesMap() {
        ConnectorProperties connector = connector("trades-json");
        assertThat(fieldFor(connector, "side").getValues())
                .containsExactlyInAnyOrderEntriesOf(Map.of("B", "BUY", "S", "SELL"));
        assertThat(fieldFor(connector, "side").getDecode()).isNull();
        assertThat(fieldFor(connector, "execution.venue").getDefaultValue()).isEqualTo("UNKNOWN");
    }

    @Test
    @DisplayName("the ring example bounds its table at ring-capacity rows")
    void bindsTheRingConnector() {
        ConnectorProperties connector = connector("ticks-json");
        assertThat(connector.getSource().isSow()).isFalse();
        assertThat(connector.getDeephaven().getTableType()).isEqualTo(DeephavenTableType.RING);
        assertThat(connector.getDeephaven().getRingCapacity()).isEqualTo(5_000);
        assertThat(connector.getDeephaven().getKeyColumns()).isEmpty();
        assertThat(TableSchema.of(connector).tableType().publisherBacked()).isTrue();
    }

    @Test
    @DisplayName("the explode example keys on the member name and maps inside its value")
    void bindsTheExplodeConnector() {
        ConnectorProperties connector = connector("portfolios-json");
        ExplodeProperties explode = connector.getExplode();
        assertThat(explode).isNotNull();
        assertThat(explode.getTag()).isEqualTo("value");
        assertThat(explode.getKeyColumn()).isEqualTo("Symbol");
        assertThat(explode.getFields()).extracting(FieldMapping::getTag)
                .containsExactly("qty", "px", ".");
        assertThat(connector.getDeephaven().getKeyColumns())
                .containsExactly("OuterKey", "Symbol");
        assertThat(TableSchema.of(connector).columns()).hasSize(6);
    }

    @Test
    @DisplayName("the composite example names its parts and its server-registered type")
    void bindsTheCompositeConnector() {
        ConnectorProperties connector = connector("orders-composite");
        assertThat(connector.getFormat()).isEqualTo(SourceFormat.COMPOSITE);
        assertThat(connector.getCompositeParts())
                .containsExactly(SourceFormat.JSON, SourceFormat.FIX);
        assertThat(connector.getSource().getMessageType()).isEqualTo("composite-json-fix");
        assertThat(connector.getSource().resolveUri(connector.getFormat()))
                .isEqualTo("tcp://localhost:9007/amps/composite-json-fix");
        assertThat(fieldFor(connector, "1.54").getDecode()).isEqualTo(FixValueDecode.SIDE);
    }

    @Test
    @DisplayName("the demo profile swaps every source for the simulator")
    void demoProfileSelectsTheSimulatedDriver() {
        assertThat(properties.getConnectors())
                .extracting(c -> c.getSource().getDriver())
                .containsOnly(AmpsSourceProperties.Driver.SIMULATED);
    }

    @Test
    void theShippedConfigurationPassesValidation() {
        assertThat(ConnectorValidator.validate(properties)).isEmpty();
    }

    @Test
    void defaultsApplyWhereTheYamlIsSilent() {
        AmpsSourceProperties source = connector("positions-nvfix").getSource();
        assertThat(source.getTransport()).isEqualTo("tcp");
        assertThat(source.getFieldSeparator()).isEqualTo((char) 0x01);
        assertThat(source.getPort()).isEqualTo(9007);
        assertThat(connector("positions-nvfix").getDeephaven().isCreateIfMissing()).isTrue();
    }

    private ConnectorProperties connector(String name) {
        List<ConnectorProperties> matches = properties.getConnectors().stream()
                .filter(c -> name.equals(c.getName())).toList();
        assertThat(matches).as("connector %s", name).hasSize(1);
        return matches.get(0);
    }

    private static FieldMapping fieldFor(ConnectorProperties connector, String tag) {
        return connector.getFields().stream()
                .filter(f -> tag.equals(f.getTag()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no mapping for tag " + tag));
    }
}
