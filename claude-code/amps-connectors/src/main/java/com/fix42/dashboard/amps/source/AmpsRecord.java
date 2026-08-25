package com.fix42.dashboard.amps.source;

/**
 * One message delivered by an AMPS subscription, before decoding.
 *
 * @param data the raw payload -- FIX, NVFIX or JSON text
 * @param sowKey the AMPS SOW key of the record, or {@code null} on a journal topic
 * @param action whether the message asserts the record or removes it
 */
public record AmpsRecord(String data, String sowKey, Action action) {

    /** What the message says about the record. */
    public enum Action {
        /** The record exists with this content. */
        UPSERT,
        /** The record left the subscription: a SOW delete, or an out-of-focus message. */
        DELETE
    }

    /** An upsert with no SOW key -- the journal-topic shape. */
    public static AmpsRecord of(String data) {
        return new AmpsRecord(data, null, Action.UPSERT);
    }

    /** An upsert carrying a SOW key. */
    public static AmpsRecord of(String data, String sowKey) {
        return new AmpsRecord(data, sowKey, Action.UPSERT);
    }

    /** A removal of the record identified by {@code data}/{@code sowKey}. */
    public static AmpsRecord delete(String data, String sowKey) {
        return new AmpsRecord(data, sowKey, Action.DELETE);
    }
}
