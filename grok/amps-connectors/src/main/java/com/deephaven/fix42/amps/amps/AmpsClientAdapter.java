package com.deephaven.fix42.amps.amps;

import com.crankuptheamps.client.Client;
import com.crankuptheamps.client.Command;
import com.crankuptheamps.client.CommandId;
import com.crankuptheamps.client.Message;
import com.crankuptheamps.client.exception.AMPSException;
import com.deephaven.fix42.amps.config.AmpsProperties;
import com.deephaven.fix42.amps.config.ConnectorConfigValidator;
import com.deephaven.fix42.amps.config.ConnectorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/** Thin AMPS Java client wrapper. */
public final class AmpsClientAdapter implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AmpsClientAdapter.class);

    private final Client client;
    private CommandId subscriptionId;

    public AmpsClientAdapter(Client client) {
        this.client = client;
    }

    public static AmpsClientAdapter connect(AmpsProperties amps, ConnectorProperties connector) throws AMPSException {
        String uri = ConnectorConfigValidator.resolvedUri(amps, connector);
        Client client = new Client(connector.resolvedClientName());
        log.info("connector {}: connecting AMPS {}", connector.getName(), uri);
        client.connect(uri);
        client.logon();
        return new AmpsClientAdapter(client);
    }

    public void subscribe(SubscriptionSpec spec, Consumer<AmpsInboundMessage> consumer) throws AMPSException {
        Command command = new Command(spec.command()).setTopic(spec.topic());
        if (!spec.filter().isBlank()) {
            command.setFilter(spec.filter());
        }
        if (!spec.bookmark().isBlank()) {
            command.setBookmark(spec.bookmark());
        }
        if (!spec.options().isBlank()) {
            command.setOptions(spec.options());
        }
        if (spec.batchSize() > 0) {
            command.setBatchSize(spec.batchSize());
        }
        log.info("AMPS subscribe {}", spec);
        subscriptionId = client.executeAsync(command, message -> dispatch(message, consumer));
    }

    private static void dispatch(Message message, Consumer<AmpsInboundMessage> consumer) {
        if (message == null) {
            return;
        }
        consumer.accept(new AmpsInboundMessage(
                kindOf(message.getCommand()),
                Integer.toString(message.getCommand()),
                message.getSowKey(),
                message.getData()));
    }

    static AmpsInboundMessage.Kind kindOf(int command) {
        if (command == Message.Command.SOW) {
            return AmpsInboundMessage.Kind.SOW;
        }
        if (command == Message.Command.Publish || command == Message.Command.DeltaPublish) {
            return AmpsInboundMessage.Kind.PUBLISH;
        }
        if (command == Message.Command.OOF) {
            return AmpsInboundMessage.Kind.OOF;
        }
        return AmpsInboundMessage.Kind.OTHER;
    }

    @Override
    public void close() {
        try {
            if (subscriptionId != null) {
                try {
                    client.unsubscribe(subscriptionId);
                } catch (Exception e) {
                    log.debug("unsubscribe failed: {}", e.toString());
                }
            }
            client.disconnect();
        } catch (Exception e) {
            log.debug("AMPS disconnect failed: {}", e.toString());
        } finally {
            try {
                client.close();
            } catch (Exception e) {
                log.debug("AMPS close failed: {}", e.toString());
            }
        }
    }
}
