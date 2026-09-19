package com.fix42.dashboard.connectors.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fix42.dashboard.connectors.app.ConnectorApplication;
import com.fix42.dashboard.connectors.mapping.TableSchema;
import com.fix42.dashboard.connectors.source.SimulatedSource;
import com.fix42.dashboard.connectors.source.SourceFactory;
import com.fix42.dashboard.connectors.source.SourceResolver;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * The shipped configuration -- baked defaults plus the demo profile's six example connectors --
 * binds, validates, and means what the docs say it means.
 *
 * <p>{@code dh-connectors.enabled=false} keeps the lifecycle monitor from dialling a Deephaven
 * server: this test is about the configuration model, not about running connectors. The
 * deployable per-application files under {@code config/} get the same treatment in
 * {@code ConfigTreeTest}.
 */
@SpringBootTest(classes = ConnectorApplication.class, properties = "dh-connectors.enabled=false")
@ActiveProfiles("demo")
class ApplicationYamlBindingTest {

    @Autowired
    private ConnectorsProperties properties;

    @Autowired
    private SourceResolver sources;

    @Autowired
    private List<SourceFactory> sourceFactories;

    @Test
    @DisplayName("the runner carries a source module for the examples' transport")
    void theAmpsSourceModuleIsOnTheRunnersClasspath() {
        // One image serves the whole fleet, so the drivers have to arrive with it: this is
        // the assertion that catches a source module dropped from connector-app's build file
        // or an auto-configuration that never registered its factory.
        assertThat(sourceFactories)
                .anySatisfy(factory -> assertThat(properties.getConnectors())
                        .allSatisfy(c -> assertThat(factory.supports(c)).isTrue()));
    }

    @Test
    @DisplayName("driver: SIMULATED resolves to the in-process generator, module or no module")
    void theDemoProfileResolvesToTheSimulator() {
        assertThat(properties.getConnectors())
                .allSatisfy(c -> assertThat(sources.resolve(c))
                        .isInstanceOf(SimulatedSource.class));
    }

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
        assertThat(connector.getSource().stateful()).isTrue();
        assertThat(connector.getSource().getAmps().isSow()).isTrue();
        assertThat(connector.getSource().getAmps().getTopic()).isEqualTo("Orders");
        assertThat(connector.getSource().subscriptionMode()).isEqualTo(UpdateMode.FULL);
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
        assertThat(connector.getSource().subscriptionMode()).isEqualTo(UpdateMode.DELTA);
        assertThat(connector.getDeephaven().getPublishMode()).isEqualTo(UpdateMode.DELTA);
        assertThat(connector.getDeephaven().getKeyColumns()).containsExactly("Account", "Symbol");
    }

    @Test
    @DisplayName("the JSON example is a journal topic with an append-only table")
    void bindsTheJsonConnector() {
        ConnectorProperties connector = connector("trades-json");
        assertThat(connector.getFormat()).isEqualTo(SourceFormat.JSON);
        assertThat(connector.getSource().stateful()).isFalse();
        assertThat(connector.getSource().getAmps().isSow()).isFalse();
        assertThat(connector.getSource().getAmps().getBookmark()).isEqualTo("epoch");
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
        assertThat(connector.getSource().stateful()).isFalse();
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
        assertThat(connector.getSource().getAmps().getMessageType())
                .isEqualTo("composite-json-fix");
        assertThat(connector.getSource().getAmps().resolveUri(connector.getFormat()))
                .isEqualTo("tcp://localhost:9007/amps/composite-json-fix");
        assertThat(fieldFor(connector, "1.54").getDecode()).isEqualTo(FixValueDecode.SIDE);
    }

    @Test
    @DisplayName("the demo profile swaps every source for the simulator")
    void demoProfileSelectsTheSimulatedDriver() {
        assertThat(properties.getConnectors())
                .extracting(c -> c.getSource().getDriver())
                .containsOnly(SourceProperties.Driver.SIMULATED);
    }

    @Test
    @DisplayName("the simulated examples keep their amps block, so they validate as deployed")
    void everyExampleStillNamesItsTransport() {
        assertThat(properties.getConnectors())
                .allSatisfy(c -> assertThat(c.getSource().configuredBlocks())
                        .containsExactly("amps"));
    }

    @Test
    @DisplayName("the simulator's rate and key count sit above the transport block")
    void bindsTheSimulatorKnobs() {
        SourceProperties source = connector("orders-fix").getSource();
        assertThat(source.getSimulatedRate()).isEqualTo(20);
        assertThat(source.getSimulatedKeys()).isEqualTo(25);
    }

    @Test
    void theShippedConfigurationPassesValidation() {
        assertThat(ConnectorValidator.validate(properties)).isEmpty();
    }

    @Test
    void defaultsApplyWhereTheYamlIsSilent() {
        AmpsSourceProperties source = connector("positions-nvfix").getSource().getAmps();
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
