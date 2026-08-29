package com.fix42.dashboard.dh;

import io.deephaven.engine.table.Table;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Ingestion: the {@code fix_raw} blink source table (doc 03 section 2.1) -- port of
 * {@code dh_app.ingest}.
 *
 * <p>The source is selectable with {@code FIX42_SOURCE}:
 *
 * <dl>
 *   <dt>{@code kafka} (default)
 *   <dd>{@link KafkaTools#consumeToTable} over the {@code fix42.messages} topic, seeking to the
 *       beginning.
 *   <dt>{@code amps}
 *   <dd>An AMPS bookmark subscription replaying the transaction log from {@code EPOCH}
 *       ({@link AmpsRawSource}).
 * </dl>
 *
 * <p>Both are the same contract, which is the point of the switch: the topic <em>is</em> the
 * journal, so replaying it from the start on every boot makes a restart rebuild the identical cache
 * (doc 03 section 3.3). Neither path retains anything -- {@code fix_raw} is a blink table either
 * way (doc 02 section 2).
 *
 * <p>Downstream is source-agnostic: {@link Fix42Pipeline} reads exactly one column, {@code RawFix},
 * and the rest of the DAG never sees this table at all. The other columns differ per source and are
 * bookkeeping only ({@code KafkaOffset} vs {@code AmpsBookmark} both being "where in the journal
 * this row came from").
 *
 * <p>Neither client is referenced outside {@link #buildFixRaw()}'s two branches: {@link KafkaIngest}
 * and {@link AmpsRawSource} each own their vendor imports, so a Kafka deployment never loads the AMPS
 * client (which is not in the server image) and this class's configuration logic stays testable with
 * neither on the classpath -- the exact arrangement the python module gets from its lazy imports.
 */
public final class Ingest {

    /** Environment variable selecting the source (doc 05 section 4). */
    public static final String SOURCE_ENV = "FIX42_SOURCE";

    /** Environment variable holding the Kafka bootstrap servers (doc 05 section 4). */
    public static final String BOOTSTRAP_ENV = "FIX42_KAFKA_BOOTSTRAP";

    /** Environment variable holding the source topic (doc 05 section 4). */
    public static final String TOPIC_ENV = "FIX42_TOPIC";

    /** Kafka source name. */
    public static final String SOURCE_KAFKA = "kafka";

    /** AMPS transaction-log source name. */
    public static final String SOURCE_AMPS = "amps";

    /** Every accepted {@link #SOURCE_ENV} value. */
    public static final List<String> SOURCES = List.of(SOURCE_KAFKA, SOURCE_AMPS);

    /** The source used when {@link #SOURCE_ENV} is unset. */
    public static final String DEFAULT_SOURCE = SOURCE_KAFKA;

    /** Compose-network default -- the Kafka service name (doc 04 section 7). */
    public static final String DEFAULT_BOOTSTRAP = "kafka:9092";

    /** Project-wide topic convention (doc 00 section 5). */
    public static final String DEFAULT_TOPIC = "fix42.messages";

    /** Consumer group id (doc 03 section 2.1). */
    public static final String DEFAULT_GROUP_ID = "dh-fix42-dashboard";

    private Ingest() {}

    /**
     * The configured source name, normalized to one of {@link #SOURCES}.
     *
     * @param env the environment to read
     * @return {@code "kafka"} or {@code "amps"}
     * @throws IllegalArgumentException if {@link #SOURCE_ENV} is set to anything else. Failing
     *     loudly beats silently falling back to Kafka: a deployment that meant to read AMPS and got
     *     Kafka would look healthy while rebuilding the wrong cache.
     */
    public static String fixSource(Map<String, String> env) {
        String value = orDefault(env, SOURCE_ENV, DEFAULT_SOURCE).strip().toLowerCase(Locale.ROOT);
        if (!SOURCES.contains(value)) {
            throw new IllegalArgumentException(
                    SOURCE_ENV + "='" + value + "' is not a known source; expected one of " + SOURCES);
        }
        return value;
    }

    /** {@link #fixSource(Map)} against the process environment. */
    public static String fixSource() {
        return fixSource(System.getenv());
    }

    /** The configured Kafka bootstrap servers. */
    public static String kafkaBootstrap(Map<String, String> env) {
        return orDefault(env, BOOTSTRAP_ENV, DEFAULT_BOOTSTRAP);
    }

    /** The configured source topic. */
    public static String kafkaTopic(Map<String, String> env) {
        return orDefault(env, TOPIC_ENV, DEFAULT_TOPIC);
    }

    /** One-line summary of where {@code fix_raw} is reading from, for the startup banner. */
    public static String sourceDescription(Map<String, String> env) {
        if (SOURCE_AMPS.equals(fixSource(env))) {
            return "amps: " + AmpsConfig.fromEnv(env).describe();
        }
        return "kafka: " + kafkaBootstrap(env) + " topic=" + kafkaTopic(env) + " (seek to beginning)";
    }

    /** {@link #sourceDescription(Map)} against the process environment. */
    public static String sourceDescription() {
        return sourceDescription(System.getenv());
    }

    /**
     * Builds the {@code fix_raw} blink table from the configured source.
     *
     * @return a blink table carrying {@code RawFix} plus per-source bookkeeping columns
     * @throws IllegalArgumentException if {@link #SOURCE_ENV} names an unknown source
     */
    public static Table buildFixRaw() {
        Map<String, String> env = System.getenv();
        if (SOURCE_AMPS.equals(fixSource(env))) {
            return AmpsRawSource.build(AmpsConfig.fromEnv(env));
        }
        return KafkaIngest.buildFixRaw(kafkaBootstrap(env), kafkaTopic(env), DEFAULT_GROUP_ID);
    }

    /** {@code env.get(key)}, falling back to {@code fallback} when unset or empty. */
    static String orDefault(Map<String, String> env, String key, String fallback) {
        String value = env.get(key);
        return (value == null || value.isEmpty()) ? fallback : value;
    }
}
