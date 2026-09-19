package com.fix42.dashboard.connectors.source;

import java.util.List;

/**
 * One message delivered by a {@link RecordSource}, before decoding.
 *
 * <p>A message of a simple format is one payload string; a message of a composite type is a
 * list of part payloads, already unframed by the source (AMPS frames them with binary length
 * prefixes, which do not survive as a single string). Exactly one of {@code data} /
 * {@code parts} is meaningful, selected by the connector's format.
 *
 * @param data the raw payload -- FIX, NVFIX or JSON text; {@code null} for a composite record
 * @param parts the payloads of a composite message's parts, in wire order; {@code null} for a
 *     simple record
 * @param key the source's own key for the record (an AMPS SOW key, a Kafka message key), or
 *     {@code null} for a source that does not key its messages
 * @param action whether the message asserts the record or removes it
 */
public record SourceRecord(String data, List<String> parts, String key, Action action) {

    /** What the message says about the record. */
    public enum Action {
        /** The record exists with this content. */
        UPSERT,
        /** The record left the subscription: a SOW delete, an out-of-focus message, a tombstone. */
        DELETE
    }

    /** A simple (single-payload) record. */
    public SourceRecord(String data, String key, Action action) {
        this(data, null, key, action);
    }

    /** An upsert with no source key -- the journal-topic shape. */
    public static SourceRecord of(String data) {
        return new SourceRecord(data, null, null, Action.UPSERT);
    }

    /** An upsert carrying a source key. */
    public static SourceRecord of(String data, String key) {
        return new SourceRecord(data, null, key, Action.UPSERT);
    }

    /** A removal of the record identified by {@code data}/{@code key}. */
    public static SourceRecord delete(String data, String key) {
        return new SourceRecord(data, null, key, Action.DELETE);
    }

    /** A record of a composite message type, already split into its parts. */
    public static SourceRecord composite(List<String> parts, String key, Action action) {
        return new SourceRecord(null, List.copyOf(parts), key, action);
    }

    /** This record with a different action; content and identity kept. */
    public SourceRecord withAction(Action newAction) {
        return new SourceRecord(data, parts, key, newAction);
    }
}
