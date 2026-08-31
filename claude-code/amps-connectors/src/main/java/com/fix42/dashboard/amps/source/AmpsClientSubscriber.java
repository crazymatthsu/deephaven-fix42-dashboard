package com.fix42.dashboard.amps.source;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.CompositeMessageParser;
import com.crankuptheamps.client.ConnectionStateListener;
import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import com.crankuptheamps.client.Message;
import com.fix42.dashboard.amps.config.AmpsSourceProperties;
import com.fix42.dashboard.amps.config.ConnectorProperties;
import com.fix42.dashboard.amps.config.SourceFormat;
import com.fix42.dashboard.amps.config.UpdateMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link AmpsSubscriber} backed by the 60East AMPS java client.
 *
 * <p>Uses {@link HAClient}, so a dropped AMPS connection is re-established and the
 * subscription replayed by the client itself -- the connector above does not have to
 * distinguish "AMPS blipped" from "AMPS is gone".
 *
 * <p>The command issued depends on the topic and the subscription mode (doc 07 section 3):
 *
 * <table border="1">
 *   <caption>Command selection</caption>
 *   <tr><th>topic</th><th>mode</th><th>AMPS command</th></tr>
 *   <tr><td>SOW</td><td>FULL</td><td>{@code sow_and_subscribe}</td></tr>
 *   <tr><td>SOW</td><td>DELTA</td><td>{@code sow_and_delta_subscribe}</td></tr>
 *   <tr><td>journal</td><td>FULL</td><td>{@code subscribe} from the {@code epoch} bookmark</td></tr>
 * </table>
 */
public class AmpsClientSubscriber implements AmpsSubscriber {

    private static final Logger log = LoggerFactory.getLogger(AmpsClientSubscriber.class);

    private final ConnectorProperties connector;
    private final AtomicBoolean connected = new AtomicBoolean(false);

    /**
     * Unframes composite messages (4-byte binary length prefix per part), which is why it
     * reads the raw {@code Message} rather than {@code getData()} -- the prefixes do not
     * survive a pass through a string. One instance, reused: the AMPS client delivers this
     * subscription's messages on a single thread.
     */
    private final CompositeMessageParser compositeParser = new CompositeMessageParser();

    private HAClient client;
    private CommandId subscriptionId;

    public AmpsClientSubscriber(ConnectorProperties connector) {
        this.connector = connector;
    }

    @Override
    public void start(RecordHandler handler) throws Exception {
        AmpsSourceProperties source = connector.getSource();
        String uri = source.resolveUri(connector.getFormat());
        String clientName = source.getClientName() == null || source.getClientName().isBlank()
                ? connector.getName()
                : source.getClientName();

        HAClient amps = HAClient.createMemoryBacked(clientName);
        amps.setServerChooser(new DefaultServerChooser().add(uri));
        amps.setTimeout((int) source.getTimeout().toMillis());
        amps.setReconnectDelay((int) source.getReconnectDelay().toMillis());
        amps.addConnectionStateListener(state -> onConnectionState(state, uri));
        amps.setExceptionListener(e -> log.warn("[{}] AMPS client error", connector.getName(), e));

        log.info("[{}] connecting to AMPS {} (topic '{}', {}, {} subscription)",
                connector.getName(), uri, source.getTopic(),
                source.isSow() ? "SOW" : "journal", source.getSubscriptionMode());
        amps.connectAndLogon();
        this.client = amps;

        Command command = buildCommand(source);
        this.subscriptionId = amps.executeAsync(command, message -> dispatch(message, handler));
        log.info("[{}] subscribed to '{}' (sub id {})",
                connector.getName(), source.getTopic(), subscriptionId);
    }

    /**
     * Build the AMPS command for this source. Package-private so the command selection is
     * unit-testable without a server.
     */
    Command buildCommand(AmpsSourceProperties source) {
        Command command = new Command(commandFor(source));
        command.setTopic(source.getTopic());
        command.setTimeout(source.getTimeout().toMillis());
        if (source.getFilter() != null && !source.getFilter().isBlank()) {
            command.setFilter(source.getFilter());
        }
        String options = optionsFor(source);
        if (!options.isEmpty()) {
            command.setOptions(options);
        }
        if (!source.isSow()) {
            // A journal topic has no SOW to replay, so the bookmark is the rehydration
            // mechanism: from `epoch` AMPS replays the whole transaction log.
            command.setBookmark(bookmarkFor(source));
        }
        return command;
    }

    private static int commandFor(AmpsSourceProperties source) {
        if (!source.isSow()) {
            return Message.Command.Subscribe;
        }
        return source.getSubscriptionMode() == UpdateMode.DELTA
                ? Message.Command.SOWAndDeltaSubscribe
                : Message.Command.SOWAndSubscribe;
    }

    private static String optionsFor(AmpsSourceProperties source) {
        List<String> options = new ArrayList<>();
        if (source.isSow()) {
            // Deliver out-of-focus messages so records leaving the SOW (or the filter) can be
            // removed from the keyed table instead of going stale.
            options.add(Message.Options.OOF);
        }
        if (source.getOptions() != null && !source.getOptions().isBlank()) {
            options.add(source.getOptions().trim());
        }
        return String.join(",", options);
    }

    private static String bookmarkFor(AmpsSourceProperties source) {
        String bookmark = source.getBookmark();
        if (bookmark == null || bookmark.isBlank()) {
            return Client.Bookmarks.EPOCH;
        }
        return switch (bookmark.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "epoch", "beginning", "start" -> Client.Bookmarks.EPOCH;
            case "now" -> Client.Bookmarks.NOW;
            case "recent", "most_recent" -> Client.Bookmarks.MOST_RECENT;
            default -> bookmark.trim();
        };
    }

    private void dispatch(Message message, RecordHandler handler) {
        try {
            int command = message.getCommand();
            AmpsRecord.Action action = switch (command) {
                case Message.Command.SOW,
                     Message.Command.Publish,
                     Message.Command.DeltaPublish -> AmpsRecord.Action.UPSERT;
                case Message.Command.OOF, Message.Command.SOWDelete -> AmpsRecord.Action.DELETE;
                // GroupBegin/GroupEnd/Ack/Heartbeat carry no record.
                default -> null;
            };
            if (action == null) {
                return;
            }
            if (connector.getFormat() == SourceFormat.COMPOSITE) {
                int count = compositeParser.parse(message);
                List<String> parts = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    parts.add(compositeParser.getString(i, StandardCharsets.UTF_8));
                }
                handler.onRecord(AmpsRecord.composite(parts, message.getSowKey(), action));
                return;
            }
            handler.onRecord(new AmpsRecord(message.getData(), message.getSowKey(), action));
        } catch (RuntimeException e) {
            log.error("[{}] failed to handle AMPS message", connector.getName(), e);
        }
    }

    private void onConnectionState(int state, String uri) {
        switch (state) {
            case ConnectionStateListener.LoggedOn -> {
                connected.set(true);
                log.info("[{}] AMPS logged on to {}", connector.getName(), uri);
            }
            case ConnectionStateListener.Resubscribed ->
                    log.info("[{}] AMPS resubscribed after reconnect", connector.getName());
            case ConnectionStateListener.Disconnected, ConnectionStateListener.Shutdown -> {
                connected.set(false);
                log.warn("[{}] AMPS disconnected from {}", connector.getName(), uri);
            }
            default -> {
                // Connected / PublishReplayed / HeartbeatInitiated need no action.
            }
        }
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public void close() {
        HAClient amps = this.client;
        this.client = null;
        connected.set(false);
        if (amps == null) {
            return;
        }
        try {
            if (subscriptionId != null) {
                amps.unsubscribe(subscriptionId);
            }
        } catch (Exception e) {
            log.debug("[{}] unsubscribe on close failed", connector.getName(), e);
        } finally {
            subscriptionId = null;
            try {
                amps.close();
            } catch (RuntimeException e) {
                log.debug("[{}] AMPS client close failed", connector.getName(), e);
            }
        }
    }
}
