package com.fix42.dashboard.amps.config;

/**
 * Wire format of the AMPS message payload.
 *
 * <p>Each value selects a {@code RecordDecoder}; all three decode into the same shape --
 * a {@code Map<String, String>} keyed by the configured {@code tag} -- so the mapping
 * layer downstream is format agnostic.
 */
public enum SourceFormat {

    /** Tag-number/value pairs, e.g. {@code 11=C-1<SOH>55=AAPL<SOH>}. Tags are FIX tag numbers. */
    FIX,

    /** Name/value pairs in FIX framing, e.g. {@code ClOrdID=C-1<SOH>Symbol=AAPL<SOH>}. */
    NVFIX,

    /** A JSON object; tags are field names, optionally dotted paths into nested objects. */
    JSON
}
