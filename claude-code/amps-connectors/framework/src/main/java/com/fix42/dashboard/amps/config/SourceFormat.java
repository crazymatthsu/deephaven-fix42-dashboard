package com.fix42.dashboard.amps.config;

/**
 * Wire format of the AMPS message payload.
 *
 * <p>Each value selects a {@code RecordDecoder}; all formats decode into the same shape --
 * a {@code Map<String, String>} keyed by the configured {@code tag} -- so the mapping
 * layer downstream is format agnostic.
 */
public enum SourceFormat {

    /** Tag-number/value pairs, e.g. {@code 11=C-1<SOH>55=AAPL<SOH>}. Tags are FIX tag numbers. */
    FIX,

    /** Name/value pairs in FIX framing, e.g. {@code ClOrdID=C-1<SOH>Symbol=AAPL<SOH>}. */
    NVFIX,

    /** A JSON object; tags are field names, optionally dotted paths into nested objects. */
    JSON,

    /**
     * An AMPS composite message type ({@code composite-local} / {@code composite-global}):
     * one message carrying several length-prefixed parts, each of a constituent format named
     * by {@code composite-parts}. Tags are part-indexed -- {@code 0.orderId} is field
     * {@code orderId} of the first part -- mirroring the {@code /0/orderId} XPaths AMPS
     * itself uses for these types.
     */
    COMPOSITE
}
