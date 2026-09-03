package com.fix42.dashboard.gen;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * CLI entry point of the FIX 4.2 mock order-flow generator.
 *
 * <pre>{@code
 * ./gradlew :fix-mock-generator:run --args="--dry-run --orders 3 --seed 42"
 * ./gradlew :fix-mock-generator:run --args="--seed 42 --orders 12 --rate 200"
 * ./gradlew :fix-mock-generator:run --args="--multi-oms --dry-run --orders 3 --seed 42 --children 2"
 * }</pre>
 *
 * <p>Two modes share the flag set: the default single-tape mode of doc 05 §2.1, and
 * {@code --multi-oms}, which generates the correlated four-hub drop-copy tapes of doc 09 §8.
 * Either mode publishes to Kafka by default or to AMPS with {@code --amps-uri} (doc 10 §10).
 */
public final class GeneratorMain {

    private static final String DEFAULT_BOOTSTRAP = "localhost:19092";
    private static final String DEFAULT_TOPIC = "fix42.messages";
    private static final int DEFAULT_ORDERS = 20;
    private static final double DEFAULT_RATE = 50;

    private GeneratorMain() {}

    /**
     * Parsed command line.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic            destination topic
     * @param orders           chains to generate per batch
     * @param seed             RNG seed; identical seeds produce identical streams
     * @param rate             pacing in messages/second when publishing ({@code <= 0} disables it)
     * @param scenario         {@code all} or a single catalog name
     * @param loop             regenerate batches forever
     * @param listScenarios    print the catalog and exit
     * @param dryRun           print pipe-rendered messages to stdout instead of publishing
     * @param emitExpected     write the expected final per-chain state as JSON to this path
     * @param help             print usage and exit
     * @param multiOms         generate the four correlated multi-OMS tapes instead of one tape
     * @param children         multi-OMS only: maximum fan-out per {@code OMS-B-parent}
     * @param ampsUri          AMPS URI to publish to instead of Kafka, or {@code null} for Kafka
     *                         (doc 10 §10); mutually exclusive with an explicit
     *                         {@code --bootstrap-servers}
     */
    public record Config(
            String bootstrapServers,
            String topic,
            int orders,
            long seed,
            double rate,
            String scenario,
            boolean loop,
            boolean listScenarios,
            boolean dryRun,
            Path emitExpected,
            boolean help,
            boolean multiOms,
            int children,
            String ampsUri) {}

    public static void main(String[] args) {
        int exit;
        try {
            exit = run(parseArgs(args), System.out);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println();
            usage(System.err);
            exit = 2;
        } catch (Exception e) {
            System.err.println("error: " + e);
            exit = 1;
        }
        if (exit != 0) {
            System.exit(exit);
        }
    }

    /** Parses the flag set defined in {@code docs/05-implementation-and-testing.md} §2.1. */
    public static Config parseArgs(String[] args) {
        String bootstrap = DEFAULT_BOOTSTRAP;
        String topic = DEFAULT_TOPIC;
        int orders = DEFAULT_ORDERS;
        long seed = new Random().nextLong();
        double rate = DEFAULT_RATE;
        String scenario = ScenarioCatalog.ALL;
        boolean loop = false;
        boolean listScenarios = false;
        boolean dryRun = false;
        Path emitExpected = null;
        boolean help = false;
        boolean multiOms = false;
        int children = MultiOmsScenarioEngine.DEFAULT_MAX_CHILDREN;
        String ampsUri = null;
        boolean topicGiven = false;
        boolean childrenGiven = false;
        boolean bootstrapGiven = false;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--bootstrap-servers" -> {
                    bootstrap = value(args, ++i, arg);
                    bootstrapGiven = true;
                }
                case "--amps-uri" -> ampsUri = value(args, ++i, arg);
                case "--topic" -> {
                    topic = value(args, ++i, arg);
                    topicGiven = true;
                }
                case "--orders" -> orders = parseInt(value(args, ++i, arg), arg);
                case "--seed" -> seed = parseLong(value(args, ++i, arg), arg);
                case "--rate" -> rate = parseDouble(value(args, ++i, arg), arg);
                case "--scenario" -> scenario = value(args, ++i, arg);
                case "--emit-expected" -> emitExpected = Path.of(value(args, ++i, arg));
                case "--children" -> {
                    children = parseInt(value(args, ++i, arg), arg);
                    childrenGiven = true;
                }
                case "--multi-oms" -> multiOms = true;
                case "--loop" -> loop = true;
                case "--list-scenarios" -> listScenarios = true;
                case "--dry-run" -> dryRun = true;
                case "--help", "-h" -> help = true;
                default -> throw new IllegalArgumentException("unknown flag '" + arg + "'");
            }
        }

        if (orders <= 0) {
            throw new IllegalArgumentException("--orders must be positive, got " + orders);
        }
        if (ampsUri != null) {
            if (ampsUri.isBlank()) {
                throw new IllegalArgumentException("--amps-uri requires a value");
            }
            // One sink per run: leaving the Kafka default in place while publishing to AMPS is
            // fine (it is never read), but an explicit broker means the two flags disagree.
            if (bootstrapGiven) {
                throw new IllegalArgumentException(
                        "--amps-uri and --bootstrap-servers cannot both be given: choose one sink");
            }
        }
        if (multiOms) {
            if (topicGiven) {
                throw new IllegalArgumentException(
                        "--topic is not valid with --multi-oms: the topology fixes one topic per hub ("
                                + String.join(", ", MultiOmsTopology.topics()) + ")");
            }
            if (children < 1) {
                throw new IllegalArgumentException("--children must be at least 1, got " + children);
            }
            if (!MultiOmsScenarioCatalog.ALL.equalsIgnoreCase(scenario)) {
                MultiOmsScenarioCatalog.fromCliName(scenario);
            }
        } else {
            if (childrenGiven) {
                throw new IllegalArgumentException("--children is only valid with --multi-oms");
            }
            if (!ScenarioCatalog.ALL.equalsIgnoreCase(scenario)) {
                ScenarioCatalog.fromCliName(scenario);
            }
        }
        if (loop && emitExpected != null) {
            throw new IllegalArgumentException("--emit-expected cannot be combined with --loop");
        }
        return new Config(bootstrap, topic, orders, seed, rate, scenario,
                loop, listScenarios, dryRun, emitExpected, help, multiOms, children, ampsUri);
    }

    /** Executes the parsed configuration; returns the process exit code. */
    public static int run(Config cfg, PrintStream out) throws IOException {
        if (cfg.help()) {
            usage(out);
            return 0;
        }
        if (cfg.listScenarios()) {
            if (cfg.multiOms()) {
                listMultiOmsScenarios(out);
            } else {
                listScenarios(out);
            }
            return 0;
        }
        return cfg.multiOms() ? runMultiOms(cfg, out) : runSingleTape(cfg, out);
    }

    /** The single-tape mode of doc 05 §2.1: one chain per order, one topic. */
    private static int runSingleTape(Config cfg, PrintStream out) throws IOException {
        ScenarioEngine engine = new ScenarioEngine(cfg.seed());
        long pauseNanos = cfg.rate() > 0 ? (long) (1_000_000_000L / cfg.rate()) : 0;

        if (cfg.dryRun()) {
            ScenarioEngine.GeneratedBatch batch = engine.generate(cfg.orders(), cfg.scenario());
            for (ScenarioEngine.EmittedMessage emitted : batch.messages()) {
                out.println(FixSerializer.renderPipe(FixSerializer.serialize(emitted.message())));
            }
            writeExpected(cfg, batch, out);
            System.err.printf(Locale.ROOT, "generated %d messages across %d chains (seed %d, scenario %s)%n",
                    batch.messages().size(), batch.chains().size(), cfg.seed(), cfg.scenario());
            return 0;
        }

        try (FixPublisher publisher = openPublisher(cfg, cfg.topic())) {
            long batchNo = 0;
            do {
                ScenarioEngine.GeneratedBatch batch = engine.generate(cfg.orders(), cfg.scenario());
                for (ScenarioEngine.EmittedMessage emitted : batch.messages()) {
                    publisher.publish(cfg.topic(), emitted.chainKey(),
                            FixSerializer.serialize(emitted.message()));
                    pace(pauseNanos);
                }
                publisher.flush();
                writeExpected(cfg, batch, out);
                batchNo++;
                System.err.printf(Locale.ROOT,
                        "batch %d: published %d messages across %d chains to %s (%s, seed %d)%n",
                        batchNo, batch.messages().size(), batch.chains().size(),
                        cfg.topic(), sink(cfg), cfg.seed());
            } while (cfg.loop() && !Thread.currentThread().isInterrupted());
        }
        return 0;
    }

    /**
     * Opens the sink this run publishes to: AMPS when {@code --amps-uri} was given, Kafka
     * otherwise (doc 10 §10). {@code defaultTopic} is the destination for messages that do not
     * route themselves; AMPS ignores it, since every {@code publish} names its topic.
     */
    private static FixPublisher openPublisher(Config cfg, String defaultTopic) {
        return cfg.ampsUri() != null
                ? new AmpsFixPublisher(cfg.ampsUri())
                : new KafkaFixPublisher(cfg.bootstrapServers(), defaultTopic);
    }

    /** The sink named in the per-batch log line: the AMPS URI or the Kafka bootstrap servers. */
    private static String sink(Config cfg) {
        return cfg.ampsUri() != null ? cfg.ampsUri() : cfg.bootstrapServers();
    }

    /**
     * The multi-OMS mode of doc 09 §8: four correlated drop-copy tapes per family, each routed to
     * its hub's own topic. {@code --topic} is rejected by {@link #parseArgs} here because the
     * topology fixes the destinations.
     */
    private static int runMultiOms(Config cfg, PrintStream out) throws IOException {
        MultiOmsScenarioEngine engine = new MultiOmsScenarioEngine(cfg.seed(), cfg.children());
        long pauseNanos = cfg.rate() > 0 ? (long) (1_000_000_000L / cfg.rate()) : 0;

        if (cfg.dryRun()) {
            MultiOmsScenarioEngine.GeneratedBatch batch = engine.generate(cfg.orders(), cfg.scenario());
            for (MultiOmsScenarioEngine.EmittedMessage emitted : batch.messages()) {
                out.println(emitted.topic() + "\t"
                        + FixSerializer.renderPipe(FixSerializer.serialize(emitted.message())));
            }
            writeExpectedOms(cfg, batch);
            System.err.printf(Locale.ROOT,
                    "generated %d messages across %d families / %d hub orders "
                            + "(seed %d, scenario %s, children %d)%n",
                    batch.messages().size(), batch.chains().size(), batch.expectedOrders().size(),
                    cfg.seed(), cfg.scenario(), cfg.children());
            return 0;
        }

        try (FixPublisher publisher = openPublisher(cfg, MultiOmsTopology.OMS_A.topic())) {
            long batchNo = 0;
            do {
                MultiOmsScenarioEngine.GeneratedBatch batch = engine.generate(cfg.orders(), cfg.scenario());
                for (MultiOmsScenarioEngine.EmittedMessage emitted : batch.messages()) {
                    publisher.publish(emitted.topic(), emitted.chainKey(),
                            FixSerializer.serialize(emitted.message()));
                    pace(pauseNanos);
                }
                publisher.flush();
                writeExpectedOms(cfg, batch);
                batchNo++;
                System.err.printf(Locale.ROOT,
                        "batch %d: published %d messages across %d families to %s (%s, seed %d)%n",
                        batchNo, batch.messages().size(), batch.chains().size(),
                        String.join(", ", MultiOmsTopology.topics()), sink(cfg), cfg.seed());
            } while (cfg.loop() && !Thread.currentThread().isInterrupted());
        }
        return 0;
    }

    private static void writeExpectedOms(Config cfg, MultiOmsScenarioEngine.GeneratedBatch batch)
            throws IOException {
        if (cfg.emitExpected() == null) {
            return;
        }
        Path target = prepare(cfg.emitExpected());
        List<ExpectedOmsOrder> orders = batch.expectedOrders();
        Files.writeString(target, toOmsJson(orders), StandardCharsets.UTF_8);
        System.err.println("wrote expected state for " + orders.size() + " hub orders across "
                + batch.chains().size() + " families to " + target);
    }

    private static void writeExpected(Config cfg, ScenarioEngine.GeneratedBatch batch, PrintStream out)
            throws IOException {
        if (cfg.emitExpected() == null) {
            return;
        }
        Path target = prepare(cfg.emitExpected());
        Files.writeString(target, toJson(batch.expectedStates()), StandardCharsets.UTF_8);
        System.err.println("wrote expected state for " + batch.chains().size() + " chains to " + target);
    }

    private static Path prepare(Path path) throws IOException {
        Path target = path.toAbsolutePath();
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        return target;
    }

    /** Renders the expected per-chain final states as a JSON array. */
    public static String toJson(List<ExpectedChainState> states) {
        return states.stream()
                .map(ExpectedChainState::toJson)
                .collect(Collectors.joining(",\n  ", "[\n  ", "\n]\n"));
    }

    /** Renders the expected per-hub-order blotter rows as a JSON array (doc 09 §8). */
    public static String toOmsJson(List<ExpectedOmsOrder> orders) {
        return orders.stream()
                .map(ExpectedOmsOrder::toJson)
                .collect(Collectors.joining(",\n  ", "[\n  ", "\n]\n"));
    }

    private static void listScenarios(PrintStream out) {
        out.println("scenario                sequence");
        out.println("----------------------  --------------------------------------------------------------");
        for (ScenarioCatalog scenario : ScenarioCatalog.values()) {
            out.printf(Locale.ROOT, "%-22s  %s%n", scenario.cliName(), scenario.sequence());
        }
        out.printf(Locale.ROOT, "%-22s  weighted mix; every scenario appears at least once%n", ScenarioCatalog.ALL);
    }

    private static void listMultiOmsScenarios(PrintStream out) {
        out.println("multi-OMS topology: "
                + MultiOmsTopology.HUBS.stream()
                        .map(hub -> hub.isRoot()
                                ? hub.name() + " (" + hub.topic() + ")"
                                : hub.name() + " (" + hub.topic() + ", " + hub.linkTag() + "="
                                        + hub.upstream() + ".ClOrdID)")
                        .collect(Collectors.joining(" -> ")));
        out.println();
        out.println("scenario                script");
        out.println("----------------------  --------------------------------------------------------------");
        for (MultiOmsScenarioCatalog scenario : MultiOmsScenarioCatalog.values()) {
            out.printf(Locale.ROOT, "%-22s  %s%n", scenario.cliName(), scenario.script());
        }
        out.printf(Locale.ROOT, "%-22s  weighted mix; every scenario appears at least once%n",
                MultiOmsScenarioCatalog.ALL);
    }

    private static void usage(PrintStream out) {
        out.println("""
                fix-mock-generator — FIX 4.2 mock order flow for the Deephaven dashboard

                Usage: GeneratorMain [flags]

                  --bootstrap-servers <host:port>  Kafka bootstrap servers (default localhost:19092)
                  --topic <name>                   destination topic (default fix42.messages;
                                                   rejected with --multi-oms, which fixes one topic per hub)
                  --amps-uri <uri>                 publish to AMPS instead of Kafka (e.g.
                                                   tcp://localhost:29007/amps/fix; --bootstrap-servers
                                                   must not be given)
                  --orders <n>                     order chains (families with --multi-oms) to generate (default 20)
                  --seed <n>                       RNG seed (default random; identical seeds replay identically)
                  --rate <msgs/sec>                publish pacing (default 50; ignored by --dry-run)
                  --scenario all|<name>            scenario selection (default all)
                  --multi-oms                      generate the correlated 4-hub drop-copy tapes (doc 09 §8)
                  --children <n>                   --multi-oms only: max fan-out per OMS-B-parent (default 3)
                  --loop                           regenerate batches forever
                  --list-scenarios                 print the scenario catalog and exit
                  --dry-run                        print pipe-rendered messages to stdout, no Kafka
                                                   (--multi-oms prefixes each line with "<topic>\\t")
                  --emit-expected <file>           write expected final state as JSON (per chain, or
                                                   per hub order with --multi-oms)
                  -h, --help                       print this help""");
    }

    private static void pace(long pauseNanos) {
        if (pauseNanos <= 0) {
            return;
        }
        try {
            Thread.sleep(pauseNanos / 1_000_000L, (int) (pauseNanos % 1_000_000L));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String value(String[] args, int index, String flag) {
        if (index >= args.length) {
            throw new IllegalArgumentException(flag + " requires a value");
        }
        return args[index];
    }

    private static int parseInt(String raw, String flag) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " expects an integer, got '" + raw + "'");
        }
    }

    private static long parseLong(String raw, String flag) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " expects an integer, got '" + raw + "'");
        }
    }

    private static double parseDouble(String raw, String flag) {
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(flag + " expects a number, got '" + raw + "'");
        }
    }
}
