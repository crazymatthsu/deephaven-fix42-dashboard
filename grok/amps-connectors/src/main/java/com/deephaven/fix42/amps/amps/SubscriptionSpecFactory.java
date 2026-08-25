package com.deephaven.fix42.amps.amps;

import com.deephaven.fix42.amps.config.ConnectorProperties;
import com.deephaven.fix42.amps.config.TopicKind;
import com.deephaven.fix42.amps.config.UpdateMode;

public final class SubscriptionSpecFactory {
    private SubscriptionSpecFactory() {}

    /**
     * @param replayFromBeginning journal only: true when the Deephaven table was just created
     *     (rehydrate); false when the table already existed (connector-only restart).
     */
    public static SubscriptionSpec create(ConnectorProperties connector, boolean replayFromBeginning) {
        String filter = connector.getFilter() == null ? "" : connector.getFilter().trim();
        int batch = Math.max(1, connector.getBatchSize());
        if (connector.getTopicKind() == TopicKind.SOW) {
            String command = connector.getSubscriberMode() == UpdateMode.DELTA
                    ? "sow_and_delta_subscribe"
                    : "sow_and_subscribe";
            return new SubscriptionSpec(command, connector.getTopic(), filter, "", "oof,send_keys", batch);
        }
        String command = connector.getSubscriberMode() == UpdateMode.DELTA ? "delta_subscribe" : "subscribe";
        String bookmark = replayFromBeginning ? SubscriptionSpec.BOOKMARK_EPOCH : SubscriptionSpec.BOOKMARK_NOW;
        return new SubscriptionSpec(command, connector.getTopic(), filter, bookmark, "", batch);
    }
}
