package com.fix42.dashboard.connectors.amps;

import static org.assertj.core.api.Assertions.assertThat;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.JSONMessage;
import com.crankuptheamps.client.Message;
import com.fix42.dashboard.connectors.TestConnectors;
import com.fix42.dashboard.connectors.config.AmpsSourceProperties;
import com.fix42.dashboard.connectors.config.ConnectorProperties;
import com.fix42.dashboard.connectors.config.SourceFormat;
import com.fix42.dashboard.connectors.config.UpdateMode;
import com.fix42.dashboard.connectors.source.RecordHandler;
import com.fix42.dashboard.connectors.source.SourceRecord;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The AMPS command a source resolves to. No server involved -- only the command is built. */
class AmpsRecordSourceTest {

    private static Command command(ConnectorProperties connector) {
        return new AmpsRecordSource(connector).buildCommand(connector.getSource().getAmps());
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
        connector.getSource().getAmps().setBookmark("now");
        assertThat(command(connector).getBookmark()).isEqualTo(Client.Bookmarks.NOW);

        connector.getSource().getAmps().setBookmark("most_recent");
        assertThat(command(connector).getBookmark()).isEqualTo(Client.Bookmarks.MOST_RECENT);

        connector.getSource().getAmps().setBookmark("1|1|");
        assertThat(command(connector).getBookmark()).isEqualTo("1|1|");
    }

    @Test
    void carriesTheConfiguredFilterAndExtraOptions() {
        ConnectorProperties connector = TestConnectors.fixOrders();
        connector.getSource().getAmps().setFilter("/Symbol = 'AAPL'");
        connector.getSource().getAmps().setOptions("conflation=100ms");

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


    // ---- what each received command becomes -------------------------------------------------

    private static Message message(int command, String data, String sowKey) {
        Message message = new JSONMessage(
                StandardCharsets.UTF_8.newEncoder(), StandardCharsets.UTF_8.newDecoder());
        message.setCommand(command);
        if (data != null) {
            message.setData(data);
        }
        if (sowKey != null) {
            message.setSowKey(sowKey);
        }
        return message;
    }

    /** Records what the source hands to the connector, in order. */
    private static final class RecordingHandler implements RecordHandler {
        final List<String> events = new ArrayList<>();

        @Override
        public void onRecord(SourceRecord record) {
            events.add(record.action() + ":" + record.key() + ":" + record.data());
        }

        @Override
        public void onSnapshotBegin() {
            events.add("snapshot-begin");
        }

        @Override
        public void onSnapshotEnd() {
            events.add("snapshot-end");
        }
    }

    @Test
    @DisplayName("group_begin/group_end bracket the SOW replay as the snapshot callbacks")
    void mapsEachCommandToItsCallback() {
        AmpsRecordSource source = new AmpsRecordSource(TestConnectors.fixOrders());
        RecordingHandler handler = new RecordingHandler();

        source.dispatch(message(Message.Command.GroupBegin, null, null), handler);
        source.dispatch(message(Message.Command.SOW, "11=C-1", "SOW-1"), handler);
        source.dispatch(message(Message.Command.GroupEnd, null, null), handler);
        source.dispatch(message(Message.Command.Publish, "11=C-1", "SOW-1"), handler);
        source.dispatch(message(Message.Command.DeltaPublish, "38=200", "SOW-1"), handler);
        source.dispatch(message(Message.Command.OOF, "", "SOW-1"), handler);
        source.dispatch(message(Message.Command.SOWDelete, "", "SOW-1"), handler);
        source.dispatch(message(Message.Command.Ack, null, null), handler);
        source.dispatch(message(Message.Command.Heartbeat, null, null), handler);

        assertThat(handler.events).containsExactly(
                "snapshot-begin",
                "UPSERT:SOW-1:11=C-1",
                "snapshot-end",
                "UPSERT:SOW-1:11=C-1",
                "UPSERT:SOW-1:38=200",
                "DELETE:SOW-1:",
                "DELETE:SOW-1:");
    }
}
