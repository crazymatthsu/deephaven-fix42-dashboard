package com.fix42.dashboard.dh;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Everything {@link AmpsRawSource} needs, read from the environment -- port of
 * {@code dh_app.amps_ingest.AmpsConfig}.
 *
 * <p>Deliberately free of any {@code com.crankuptheamps} type so it can be unit tested without the
 * AMPS client on the classpath, exactly as the python original is testable on a bare python.
 */
public final class AmpsConfig {

    /** Comma/whitespace-separated AMPS URIs; several entries make an HA pair. */
    public static final String URI_ENV = "FIX42_AMPS_URI";

    /** AMPS topic; falls back to {@link #TOPIC_FALLBACK_ENV}. */
    public static final String TOPIC_ENV = "FIX42_AMPS_TOPIC";

    /** The shared topic variable, so a deployment need not name its topic twice. */
    public static final String TOPIC_FALLBACK_ENV = "FIX42_TOPIC";

    /** Optional server-side content filter. */
    public static final String FILTER_ENV = "FIX42_AMPS_FILTER";

    /** AMPS client name -- the analogue of the Kafka group id. */
    public static final String CLIENT_NAME_ENV = "FIX42_AMPS_CLIENT_NAME";

    /** Where in the transaction log to start. */
    public static final String BOOKMARK_ENV = "FIX42_AMPS_BOOKMARK";

    /** Bound on rows buffered between update-graph cycles. */
    public static final String MAX_PENDING_ENV = "FIX42_AMPS_MAX_PENDING";

    public static final String DEFAULT_URI = "tcp://amps:9007/amps/fix";
    public static final String DEFAULT_TOPIC = Ingest.DEFAULT_TOPIC;
    public static final String DEFAULT_CLIENT_NAME = "dh-fix42-dashboard";
    public static final String DEFAULT_BOOKMARK = "epoch";

    /** Enough for several seconds of a fast feed; see {@link RawBuffer} on overflow. */
    public static final int DEFAULT_MAX_PENDING = 250_000;

    /**
     * Friendly bookmark names to the constant to read off {@code AMPS.Client.Bookmarks}.
     *
     * <p>Anything not listed here is passed to AMPS verbatim as a literal bookmark.
     */
    public static final Map<String, String> BOOKMARK_ALIASES;

    static {
        Map<String, String> aliases = new LinkedHashMap<>();
        aliases.put("epoch", "EPOCH");
        aliases.put("beginning", "EPOCH");
        aliases.put("now", "NOW");
        aliases.put("most_recent", "MOST_RECENT");
        aliases.put("recent", "MOST_RECENT");
        BOOKMARK_ALIASES = Map.copyOf(aliases);
    }

    /** The column the state machine folds; the only one {@link Fix42Pipeline} reads. */
    public static final String RAW_COLUMN = "RawFix";

    /**
     * Columns of the AMPS {@code fix_raw} blink table.
     *
     * <p>{@code AmpsBookmark} is the transaction-log position -- the AMPS analogue of
     * {@code KafkaOffset}, and the evidence for replay order.
     */
    public static final List<String> COLUMN_NAMES = List.of(RAW_COLUMN, "AmpsBookmark", "IngestTs");

    private final List<String> uris;
    private final String topic;
    private final String filter;
    private final String clientName;
    private final String bookmark;
    private final int maxPending;

    /**
     * Stores the resolved settings.
     *
     * @param uris one or more AMPS URIs
     * @param topic the topic to subscribe to
     * @param filter optional server-side content filter; blank becomes {@code null}
     * @param clientName the AMPS client name
     * @param bookmark an alias or a literal bookmark
     * @param maxPending the {@link RawBuffer} bound
     */
    public AmpsConfig(
            List<String> uris, String topic, String filter, String clientName, String bookmark, int maxPending) {
        this.uris = List.copyOf(uris);
        this.topic = topic;
        this.filter = (filter == null || filter.isEmpty()) ? null : filter;
        this.clientName = clientName;
        this.bookmark = bookmark;
        this.maxPending = maxPending;
    }

    /**
     * Builds a config from {@code env}.
     *
     * <p>The topic falls back to {@link #TOPIC_FALLBACK_ENV} so a deployment that already names its
     * topic once does not have to name it twice.
     */
    public static AmpsConfig fromEnv(Map<String, String> env) {
        List<String> uris = splitUris(env.getOrDefault(URI_ENV, ""));
        if (uris.isEmpty()) {
            uris = List.of(DEFAULT_URI);
        }
        String topic = Ingest.orDefault(env, TOPIC_ENV, Ingest.orDefault(env, TOPIC_FALLBACK_ENV, DEFAULT_TOPIC));
        return new AmpsConfig(
                uris,
                topic,
                env.get(FILTER_ENV),
                Ingest.orDefault(env, CLIENT_NAME_ENV, DEFAULT_CLIENT_NAME),
                Ingest.orDefault(env, BOOKMARK_ENV, DEFAULT_BOOKMARK),
                positiveInt(env.get(MAX_PENDING_ENV), DEFAULT_MAX_PENDING));
    }

    /** {@link #fromEnv(Map)} against the process environment. */
    public static AmpsConfig fromEnv() {
        return fromEnv(System.getenv());
    }

    public List<String> uris() {
        return uris;
    }

    public String topic() {
        return topic;
    }

    /** The server-side filter, or {@code null} when unset. */
    public String filter() {
        return filter;
    }

    public String clientName() {
        return clientName;
    }

    public String bookmark() {
        return bookmark;
    }

    public int maxPending() {
        return maxPending;
    }

    /** One-line summary for the startup banner. */
    public String describe() {
        String summary = String.join(",", uris) + " topic=" + topic + " bookmark=" + bookmark;
        return filter == null ? summary : summary + " filter='" + filter + "'";
    }

    @Override
    public String toString() {
        return "AmpsConfig(" + describe() + ")";
    }

    /**
     * Resolves a configured bookmark name to the string AMPS expects.
     *
     * @param value a {@link #BOOKMARK_ALIASES} key (case and hyphen insensitive) or a literal AMPS
     *     bookmark such as {@code "3|1|"}
     * @param epoch the value of {@code AMPS.Client.Bookmarks.EPOCH}
     * @param now the value of {@code AMPS.Client.Bookmarks.NOW}
     * @param mostRecent the value of {@code AMPS.Client.Bookmarks.MOST_RECENT}
     * @return the bookmark string to hand to {@code bookmarkSubscribe}
     */
    public static String resolveBookmark(String value, String epoch, String now, String mostRecent) {
        String key = (value == null ? "" : value).strip().toLowerCase(Locale.ROOT).replace('-', '_');
        String alias = BOOKMARK_ALIASES.get(key);
        if (alias == null) {
            return value;
        }
        return switch (alias) {
            case "EPOCH" -> epoch;
            case "NOW" -> now;
            default -> mostRecent;
        };
    }

    /** Splits a comma/whitespace separated URI list, dropping empties. */
    static List<String> splitUris(String value) {
        List<String> uris = new ArrayList<>();
        for (String part : value.replace(',', ' ').split("\\s+")) {
            if (!part.isEmpty()) {
                uris.add(part);
            }
        }
        return uris;
    }

    /**
     * Parses a positive int, falling back to {@code fallback} on anything unusable.
     *
     * <p>python's {@code int()} is unbounded and accepts digit-group underscores, and the buffer
     * bound is only ever compared against a list length -- so a value past {@code Integer.MAX_VALUE}
     * saturates rather than falling back, which is what "effectively unbounded" means on this side.
     */
    static int positiveInt(String value, int fallback) {
        if (value == null) {
            return fallback;
        }
        try {
            java.math.BigInteger parsed = new java.math.BigInteger(value.strip().replace("_", ""));
            if (parsed.signum() <= 0) {
                return fallback;
            }
            return parsed.bitLength() > 31 ? Integer.MAX_VALUE : parsed.intValueExact();
        } catch (NumberFormatException | ArithmeticException unusable) {
            return fallback;
        }
    }
}
