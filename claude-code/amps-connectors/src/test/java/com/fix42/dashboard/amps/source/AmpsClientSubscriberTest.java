package com.fix42.dashboard.amps.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.Message;
import com.fix42.dashboard.amps.TestConnectors;
import com.fix42.dashboard.amps.config.AmpsSourceProperties;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.SourceFormat;
import com.fix42.dashboard.amps.config.UpdateMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The AMPS command a source resolves to. No server involved -- only the command is built. */
class AmpsClientSubscriberTest {

    private static Command command(ConnectorProperties connector) {
        return new AmpsClientSubscriber(connector).buildCommand(connector.getSource());
    }

    @Test
    @DisplayName("a SOW topic replays the state of the world and then subscribes")
    void sowTopicUsesSowAndSubscribe() {
        Command command = command(TestConnectors.fixOrders());
        assertThat(command.getCommand()).isEqualTo(Message.Command.SOWAndSubscribe);
        assertThat(command.getTopic()).isEqualTo("Orders");
        assertThat(command.getOptions()).contains(Message.Options.OOF);
    }

    @Test
    void deltaSubscriptionOnASowTopicUsesSowAndDeltaSubscribe() {
        Command command = command(TestConnectors.nvfixPositions());
        assertThat(command.getCommand()).isEqualTo(Message.Command.SOWAndDeltaSubscribe);
    }

    @Test
    @DisplayName("a journal topic resubscribes from the beginning of the transaction log")
    void journalTopicSubscribesFromTheEpochBookmark() {
        Command command = command(TestConnectors.jsonTrades());
        assertThat(command.getCommand()).isEqualTo(Message.Command.Subscribe);
        assertThat(command.getBookmark()).isEqualTo(Client.Bookmarks.EPOCH);
    }

    @Test
    void bookmarkAliasesResolveToTheAmpsConstants() {
        ConnectorProperties connector = TestConnectors.jsonTrades();
        connector.getSource().setBookmark("now");
        assertThat(command(connector).getBookmark()).isEqualTo(Client.Bookmarks.NOW);

        connector.getSource().setBookmark("most_recent");
        assertThat(command(connector).getBookmark()).isEqualTo(Client.Bookmarks.MOST_RECENT);

        connector.getSource().setBookmark("1|1|");
        assertThat(command(connector).getBookmark()).isEqualTo("1|1|");
    }

    @Test
    void carriesTheConfiguredFilterAndExtraOptions() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getSource().setFilter("/Symbol = 'AAPL'");
        connector.getSource().setOptions("conflation=100ms");

        Command command = command(connector);
        assertThat(command.getFilter()).isEqualTo("/Symbol = 'AAPL'");
        assertThat(command.getOptions()).contains("oof").contains("conflation=100ms");
    }

    @Test
    void buildsTheAmpsUriFromHostPortAndFormat() {
        AmpsSourceProperties source = new AmpsSourceProperties();
        source.setHost("amps-1");
        source.setPort(9007);
        assertThat(source.resolveUri(SourceFormat.FIX)).isEqualTo("tcp://amps-1:9007/amps/fix");
        assertThat(source.resolveUri(SourceFormat.NVFIX)).isEqualTo("tcp://amps-1:9007/amps/nvfix");
        assertThat(source.resolveUri(SourceFormat.JSON)).isEqualTo("tcp://amps-1:9007/amps/json");
    }

    @Test
    void anExplicitUriWinsOverHostAndPort() {
        AmpsSourceProperties source = new AmpsSourceProperties();
        source.setHost("ignored");
        source.setUri("tcps://secure:9443/amps/json");
        assertThat(source.resolveUri(SourceFormat.FIX)).isEqualTo("tcps://secure:9443/amps/json");
    }

    @Test
    void anExplicitMessageTypeOverridesTheFormatDefault() {
        AmpsSourceProperties source = new AmpsSourceProperties();
        source.setMessageType("fix");
        source.setSubscriptionMode(UpdateMode.FULL);
        assertThat(source.resolveUri(SourceFormat.JSON)).isEqualTo("tcp://localhost:9007/amps/fix");
    }
}
