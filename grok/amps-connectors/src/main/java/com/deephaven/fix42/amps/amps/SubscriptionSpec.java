package com.deephaven.fix42.amps.amps;

import java.util.Objects;

/** AMPS command that a connector will issue. */
public final class SubscriptionSpec {
    public static final String BOOKMARK_EPOCH = "0";
    public static final String BOOKMARK_NOW = "0|1|";

    private final String command;
    private final String topic;
    private final String filter;
    private final String bookmark;
    private final String options;
    private final int batchSize;

    public SubscriptionSpec(
            String command, String topic, String filter, String bookmark, String options, int batchSize) {
        this.command = command;
        this.topic = topic;
        this.filter = filter == null ? "" : filter;
        this.bookmark = bookmark == null ? "" : bookmark;
        this.options = options == null ? "" : options;
        this.batchSize = batchSize;
    }

    public String command() {
        return command;
    }

    public String topic() {
        return topic;
    }

    public String filter() {
        return filter;
    }

    public String bookmark() {
        return bookmark;
    }

    public String options() {
        return options;
    }

    public int batchSize() {
        return batchSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SubscriptionSpec that)) {
            return false;
        }
        return batchSize == that.batchSize
                && command.equals(that.command)
                && topic.equals(that.topic)
                && filter.equals(that.filter)
                && bookmark.equals(that.bookmark)
                && options.equals(that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(command, topic, filter, bookmark, options, batchSize);
    }

    @Override
    public String toString() {
        return command + " topic=" + topic + " bookmark=" + bookmark + " options=" + options;
    }
}
