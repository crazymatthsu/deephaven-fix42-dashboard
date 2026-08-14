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
 * }</pre>
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
            boolean help) {}

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

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--bootstrap-servers" -> bootstrap = value(args, ++i, arg);
                case "--topic" -> topic = value(args, ++i, arg);
                case "--orders" -> orders = parseInt(value(args, ++i, arg), arg);
                case "--seed" -> seed = parseLong(value(args, ++i, arg), arg);
                case "--rate" -> rate = parseDouble(value(args, ++i, arg), arg);
                case "--scenario" -> scenario = value(args, ++i, arg);
                case "--emit-expected" -> emitExpected = Path.of(value(args, ++i, arg));
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
        if (!ScenarioCatalog.ALL.equalsIgnoreCase(scenario)) {
            ScenarioCatalog.fromCliName(scenario);
        }
        if (loop && emitExpected != null) {
            throw new IllegalArgumentException("--emit-expected cannot be combined with --loop");
        }
        return new Config(bootstrap, topic, orders, seed, rate, scenario,
                loop, listScenarios, dryRun, emitExpected, help);
    }

    /** Executes the parsed configuration; returns the process exit code. */
    public static int run(Config cfg, PrintStream out) throws IOException {
        if (cfg.help()) {
            usage(out);
            return 0;
        }
        if (cfg.listScenarios()) {
            listScenarios(out);
            return 0;
        }

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

        try (KafkaFixPublisher publisher = new KafkaFixPublisher(cfg.bootstrapServers(), cfg.topic())) {
            long batchNo = 0;
            do {
                ScenarioEngine.GeneratedBatch batch = engine.generate(cfg.orders(), cfg.scenario());
                for (ScenarioEngine.EmittedMessage emitted : batch.messages()) {
                    publisher.publish(emitted.chainKey(), FixSerializer.serialize(emitted.message()));
                    pace(pauseNanos);
                }
                publisher.flush();
                writeExpected(cfg, batch, out);
                batchNo++;
                System.err.printf(Locale.ROOT,
                        "batch %d: published %d messages across %d chains to %s (%s, seed %d)%n",
                        batchNo, batch.messages().size(), batch.chains().size(),
                        cfg.topic(), cfg.bootstrapServers(), cfg.seed());
            } while (cfg.loop() && !Thread.currentThread().isInterrupted());
        }
        return 0;
    }

    private static void writeExpected(Config cfg, ScenarioEngine.GeneratedBatch batch, PrintStream out)
            throws IOException {
        if (cfg.emitExpected() == null) {
            return;
        }
        Path target = cfg.emitExpected().toAbsolutePath();
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        Files.writeString(target, toJson(batch.expectedStates()), StandardCharsets.UTF_8);
        System.err.println("wrote expected state for " + batch.chains().size() + " chains to " + target);
    }

    /** Renders the expected per-chain final states as a JSON array. */
    public static String toJson(List<ExpectedChainState> states) {
        return states.stream()
                .map(ExpectedChainState::toJson)
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

    private static void usage(PrintStream out) {
        out.println("""
                fix-mock-generator — FIX 4.2 mock order flow for the Deephaven dashboard

                Usage: GeneratorMain [flags]

                  --bootstrap-servers <host:port>  Kafka bootstrap servers (default localhost:19092)
                  --topic <name>                   destination topic (default fix42.messages)
                  --orders <n>                     order chains to generate (default 20)
                  --seed <n>                       RNG seed (default random; identical seeds replay identically)
                  --rate <msgs/sec>                publish pacing (default 50; ignored by --dry-run)
                  --scenario all|<name>            scenario selection (default all)
                  --loop                           regenerate batches forever
                  --list-scenarios                 print the scenario catalog and exit
                  --dry-run                        print pipe-rendered messages to stdout, no Kafka
                  --emit-expected <file>           write expected final per-chain state as JSON
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
