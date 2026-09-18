package com.fix42.dashboard.amps.source;

import java.util.List;

/**
 * One message delivered by an AMPS subscription, before decoding.
 *
 * <p>A message of a simple format is one payload string; a message of an AMPS composite
 * type is a list of part payloads, already unframed by the subscriber (the wire framing is
 * binary length prefixes, which do not survive as a single string). Exactly one of
 * {@code data} / {@code parts} is meaningful, selected by the connector's format.
 *
 * @param data the raw payload -- FIX, NVFIX or JSON text; {@code null} for a composite record
 * @param parts the payloads of a composite message's parts, in wire order; {@code null} for a
 *     simple record
 * @param sowKey the AMPS SOW key of the record, or {@code null} on a journal topic
 * @param action whether the message asserts the record or removes it
 */
public record AmpsRecord(String data, List<String> parts, String sowKey, Action action) {

    /** What the message says about the record. */
    public enum Action {
        /** The record exists with this content. */
        UPSERT,
        /** The record left the subscription: a SOW delete, or an out-of-focus message. */
        DELETE
    }

    /** A simple (single-payload) record. */
    public AmpsRecord(String data, String sowKey, Action action) {
        this(data, null, sowKey, action);
    }

    /** An upsert with no SOW key -- the journal-topic shape. */
    public static AmpsRecord of(String data) {
        return new AmpsRecord(data, null, null, Action.UPSERT);
    }

    /** An upsert carrying a SOW key. */
    public static AmpsRecord of(String data, String sowKey) {
        return new AmpsRecord(data, null, sowKey, Action.UPSERT);
    }

    /** A removal of the record identified by {@code data}/{@code sowKey}. */
    public static AmpsRecord delete(String data, String sowKey) {
        return new AmpsRecord(data, null, sowKey, Action.DELETE);
    }

    /** A record of an AMPS composite message type, already split into its parts. */
    public static AmpsRecord composite(List<String> parts, String sowKey, Action action) {
        return new AmpsRecord(null, List.copyOf(parts), sowKey, action);
    }

    /** This record with a different action; content and identity kept. */
    public AmpsRecord withAction(Action newAction) {
        return new AmpsRecord(data, parts, sowKey, newAction);
    }
}
