package com.fix42.dashboard.connectors.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fix42.dashboard.connectors.TestConnectors;
import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.KafkaSourceProperties;
import com.fix42.dashboard.connectors.config.SourceProperties;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Which source a connector gets, and what happens when the answer is not exactly one. */
class SourceResolverTest {

    /** A factory that claims connectors configuring one named block. */
    private record BlockFactory(String block) implements SourceFactory {
        @Override
        public boolean supports(ConnectorProperties connector) {
            return connector.getSource().configuredBlocks().contains(block);
        }

        @Override
        public RecordSource create(ConnectorProperties connector) {
            return new FakeRecordSource();
        }
    }

    private static ConnectorProperties real(ConnectorProperties connector) {
        connector.getSource().setDriver(SourceProperties.Driver.REAL);
        return connector;
    }

    @Test
    @DisplayName("SIMULATED bypasses the factories entirely, keeping its transport block")
    void simulatedNeedsNoSourceModuleOnTheClasspath() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        assertThat(new SourceResolver(List.of()).resolve(connector))
                .isInstanceOf(SimulatedSource.class);
        assertThat(connector.getSource().getAmps()).as("the block still describes the feed")
                .isNotNull();
    }

    @Test
    void picksTheFactoryThatClaimsTheConfiguredBlock() {
        SourceResolver resolver = new SourceResolver(
                List.of(new BlockFactory("kafka"), new BlockFactory("amps")));
        assertThat(resolver.resolve(real(TestConnectors.fixOrders())))
                .isInstanceOf(FakeRecordSource.class);
    }

    @Test
    @DisplayName("no factory means the source module is missing, not that the config is fine")
    void refusesWhenNothingClaimsTheConnector() {
        SourceResolver resolver = new SourceResolver(List.of(new BlockFactory("kafka")));
        assertThatThrownBy(() -> resolver.resolve(real(TestConnectors.fixOrders())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("orders-fix")
                .hasMessageContaining("no source implementation for source.amps");
    }

    @Test
    void refusesWhenTwoFactoriesClaimTheConnector() {
        ConnectorProperties connector = real(TestConnectors.fixOrders());
        KafkaSourceProperties kafka = new KafkaSourceProperties();
        kafka.setBootstrapServers("localhost:9092");
        kafka.setTopic("Orders");
        connector.getSource().setKafka(kafka);

        SourceResolver resolver = new SourceResolver(
                List.of(new BlockFactory("amps"), new BlockFactory("kafka")));
        assertThatThrownBy(() -> resolver.resolve(connector))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("2 source implementations claim source.amps/kafka");
    }
}
