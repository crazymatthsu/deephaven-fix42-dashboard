package com.fix42.dashboard.fixcache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Everything the Deephaven layer publishes for one input message.
 *
 * <p>Port of {@code fix42cache.state_machine.Result}. On an unparseable / unsupported / unresolvable
 * message {@link #error()} is set, {@link #state()} is {@code null} and the row lists are empty;
 * {@link #message()} is still populated whenever the input was parseable enough to audit.
 */
public final class Result {

    private final OrderState state;
    private final List<ExecutionRow> executions;
    private final List<OrderEventRow> events;
    private final MessageRow message;
    private final String error;

    Result(
            OrderState state,
            List<ExecutionRow> executions,
            List<OrderEventRow> events,
            MessageRow message,
            String error) {
        this.state = state;
        this.executions = executions == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(executions));
        this.events = events == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(events));
        this.message = message;
        this.error = error;
    }

    /** A failure with no audit row (the input could not be parsed at all). */
    static Result error(String error) {
        return new Result(null, null, null, null, error);
    }

    /** A failure that still produced an audit row. */
    static Result error(MessageRow message, String error) {
        return new Result(null, null, null, message, error);
    }

    /** The post-message snapshot of the affected chain, or {@code null} on error. */
    public OrderState state() {
        return state;
    }

    /** 0..n execution rows (bust/correct/DK also re-emit the referenced execution). */
    public List<ExecutionRow> executions() {
        return executions;
    }

    /** 0..n lifecycle events. */
    public List<OrderEventRow> events() {
        return events;
    }

    /** The raw-message audit row, or {@code null} when the input was unparseable. */
    public MessageRow message() {
        return message;
    }

    /** The failure reason, or {@code null} when the message was applied. */
    public String error() {
        return error;
    }
}
