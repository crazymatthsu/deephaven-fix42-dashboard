package com.fix42.dashboard.gen;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** CLI parsing and the Kafka-free execution paths of {@link GeneratorMain}. */
class GeneratorMainTest {

    private static String capture(GeneratorMain.Config cfg) throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(buffer, true, StandardCharsets.UTF_8)) {
            assertEquals(0, GeneratorMain.run(cfg, out));
        }
        return buffer.toString(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("defaults match the documented CLI contract")
    void defaults() {
        GeneratorMain.Config cfg = GeneratorMain.parseArgs(new String[0]);
        assertEquals("localhost:19092", cfg.bootstrapServers());
        assertEquals("fix42.messages", cfg.topic());
        assertEquals(20, cfg.orders());
        assertEquals(50.0, cfg.rate());
        assertEquals(ScenarioCatalog.ALL, cfg.scenario());
        assertFalse(cfg.loop());
        assertFalse(cfg.dryRun());
        assertFalse(cfg.listScenarios());
        assertNull(cfg.emitExpected());
        assertFalse(cfg.multiOms());
        assertEquals(MultiOmsScenarioEngine.DEFAULT_MAX_CHILDREN, cfg.children());
        assertEquals(3, cfg.children());
    }

    @Test
    @DisplayName("all flags parse")
    void allFlagsParse() {
        GeneratorMain.Config cfg = GeneratorMain.parseArgs(new String[] {
            "--bootstrap-servers", "kafka:9092",
            "--topic", "other.topic",
            "--orders", "7",
            "--seed", "42",
            "--rate", "200",
            "--scenario", "fill_bust",
            "--dry-run",
            "--emit-expected", "expected.json",
        });
        assertEquals("kafka:9092", cfg.bootstrapServers());
        assertEquals("other.topic", cfg.topic());
        assertEquals(7, cfg.orders());
        assertEquals(42L, cfg.seed());
        assertEquals(200.0, cfg.rate());
        assertEquals("fill_bust", cfg.scenario());
        assertTrue(cfg.dryRun());
        assertEquals(Path.of("expected.json"), cfg.emitExpected());
    }

    @Test
    @DisplayName("bad flags, bad values and impossible combinations are rejected")
    void invalidArgs() {
        assertThrows(IllegalArgumentException.class, () -> GeneratorMain.parseArgs(new String[] {"--nope"}));
        assertThrows(IllegalArgumentException.class, () -> GeneratorMain.parseArgs(new String[] {"--orders"}));
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--orders", "x"}));
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--orders", "0"}));
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--scenario", "nope"}));
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--loop", "--emit-expected", "e.json"}));
    }

    @Test
    @DisplayName("--seed defaults to a random value so unseeded runs differ")
    void seedDefaultsRandom() {
        long first = GeneratorMain.parseArgs(new String[0]).seed();
        long second = GeneratorMain.parseArgs(new String[0]).seed();
        long third = GeneratorMain.parseArgs(new String[0]).seed();
        assertFalse(first == second && second == third, "unseeded runs should not share a seed");
    }

    @Test
    @DisplayName("--list-scenarios prints every catalog name")
    void listScenarios() throws Exception {
        String output = capture(GeneratorMain.parseArgs(new String[] {"--list-scenarios"}));
        for (ScenarioCatalog scenario : ScenarioCatalog.values()) {
            assertTrue(output.contains(scenario.cliName()), scenario.cliName());
        }
        assertTrue(output.contains(ScenarioCatalog.ALL));
    }

    @Test
    @DisplayName("--help prints usage covering every flag")
    void help() throws Exception {
        String output = capture(GeneratorMain.parseArgs(new String[] {"--help"}));
        for (String flag : List.of("--bootstrap-servers", "--amps-uri", "--topic", "--orders", "--seed",
                "--rate", "--scenario", "--loop", "--list-scenarios", "--dry-run", "--emit-expected")) {
            assertTrue(output.contains(flag), flag);
        }
    }

    @Test
    @DisplayName("--dry-run prints pipe-rendered, framing-valid FIX and never touches Kafka")
    void dryRunOutput() throws Exception {
        String output = capture(GeneratorMain.parseArgs(
                new String[] {"--dry-run", "--orders", "3", "--seed", "42"}));
        List<String> lines = output.lines().filter(l -> !l.isBlank()).toList();
        assertTrue(lines.size() > 3);
        for (String line : lines) {
            assertTrue(line.startsWith("8=FIX.4.2|"), line);
            assertTrue(line.endsWith("|"), line);
            assertEquals(0, line.chars().filter(c -> c == FixTags.SOH).count());
            assertTrue(TestFix.framingValid(line.replace(FixTags.PIPE, FixTags.SOH)), line);
        }
        assertEquals(3, lines.stream()
                .map(l -> TestFix.parse(l).get(FixTags.MSG_TYPE))
                .filter(FixTags.MSG_NEW_ORDER_SINGLE::equals)
                .count());
    }

    @Test
    @DisplayName("--dry-run is deterministic for a given seed")
    void dryRunIsDeterministic() throws Exception {
        String[] args = {"--dry-run", "--orders", "5", "--seed", "1234", "--scenario", "amend_ack"};
        String first = capture(GeneratorMain.parseArgs(args));
        String second = capture(GeneratorMain.parseArgs(args));
        assertEquals(
                first.lines().map(GeneratorMainTest::withoutWallClockFields).toList(),
                second.lines().map(GeneratorMainTest::withoutWallClockFields).toList());
    }

    @Test
    @DisplayName("--emit-expected writes one JSON object per chain with the cache column names")
    void emitExpected(@TempDir Path dir) throws Exception {
        Path target = dir.resolve("nested").resolve("expected.json");
        capture(GeneratorMain.parseArgs(new String[] {
            "--dry-run", "--orders", "12", "--seed", "42", "--emit-expected", target.toString(),
        }));

        String json = Files.readString(target, StandardCharsets.UTF_8);
        assertTrue(json.startsWith("["));
        assertTrue(json.strip().endsWith("]"));
        assertEquals(12, json.split("\"ChainKey\"", -1).length - 1);
        for (String column : List.of("ChainKey", "OrderID", "Scenario", "OrdStatus",
                "CumQty", "LeavesQty", "ClOrdID")) {
            assertTrue(json.contains("\"" + column + "\""), column);
        }
        assertTrue(json.contains("\"ORD-0001\""));
        assertTrue(json.contains("\"FILLED\""));
        assertTrue(json.contains("\"REJECTED\""));
        assertTrue(json.contains("\"CANCELED\""));
    }

    @Test
    @DisplayName("expected-state JSON escapes strings and renders quantities as JSON numbers")
    void expectedJsonShape() {
        String json = GeneratorMain.toJson(List.of(
                new ExpectedChainState("ORD-1", "ORD-1", "new_ack_fill_full", "FILLED", 1000, 0, "C-0001-1")));
        assertTrue(json.contains("\"CumQty\":1000.0"));
        assertTrue(json.contains("\"LeavesQty\":0.0"));
        assertEquals("\"a\\\"b\\\\c\"", ExpectedChainState.jsonString("a\"b\\c"));
    }

    @Test
    @DisplayName("dry-run emits every message of every generated chain")
    void dryRunCoversWholeBatch() throws Exception {
        String output = capture(GeneratorMain.parseArgs(
                new String[] {"--dry-run", "--orders", "10", "--seed", "9"}));
        long lines = output.lines().filter(l -> l.startsWith("8=FIX.4.2|")).count();
        long generated = new ScenarioEngine(9L).generate(10, ScenarioCatalog.ALL).messages().size();
        assertEquals(generated, lines);
    }

    @Test
    @DisplayName("a single-scenario dry run only emits that scenario's skeleton")
    void singleScenarioDryRun() throws Exception {
        String output = capture(GeneratorMain.parseArgs(
                new String[] {"--dry-run", "--orders", "2", "--seed", "5", "--scenario", "new_reject"}));
        List<Map<Integer, String>> messages = output.lines()
                .filter(l -> !l.isBlank())
                .map(TestFix::parse)
                .toList();
        assertEquals(4, messages.size());
        assertEquals(2, messages.stream()
                .filter(m -> FixTags.EXEC_TYPE_REJECTED.equals(m.get(FixTags.EXEC_TYPE)))
                .count());
    }

    // ---------------------------------------------------------------- multi-OMS mode

    @Test
    @DisplayName("--multi-oms and --children parse, and --children defaults to 3")
    void multiOmsFlagsParse() {
        GeneratorMain.Config cfg = GeneratorMain.parseArgs(new String[] {
            "--multi-oms", "--children", "5", "--orders", "4", "--seed", "42",
            "--scenario", "missed_fill", "--dry-run",
        });
        assertTrue(cfg.multiOms());
        assertEquals(5, cfg.children());
        assertEquals("missed_fill", cfg.scenario());
        assertEquals("fix42.messages", cfg.topic(), "the unused default is left alone");

        assertEquals(3, GeneratorMain.parseArgs(new String[] {"--multi-oms"}).children());
    }

    @Test
    @DisplayName("--topic is rejected in multi-OMS mode; the topology fixes the destinations")
    void multiOmsRejectsTopic() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--multi-oms", "--topic", "custom"}));
        assertTrue(thrown.getMessage().contains("--topic"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("fix42.oms-a"), thrown.getMessage());
        // Flag order must not matter.
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--topic", "custom", "--multi-oms"}));
    }

    @Test
    @DisplayName("--children needs --multi-oms and must be at least 1")
    void childrenValidation() {
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--children", "2"}));
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--multi-oms", "--children", "0"}));
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--multi-oms", "--children", "x"}));
    }

    @Test
    @DisplayName("--scenario validates against the mode's own catalog")
    void scenarioValidatesPerMode() {
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--scenario", "clean_fill"}));
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--multi-oms", "--scenario", "fill_bust"}));
        assertEquals("clean_fill", GeneratorMain.parseArgs(
                new String[] {"--multi-oms", "--scenario", "clean_fill"}).scenario());
        assertEquals("fill_bust", GeneratorMain.parseArgs(
                new String[] {"--scenario", "fill_bust"}).scenario());
    }

    @Test
    @DisplayName("--multi-oms --list-scenarios prints the multi-OMS catalog and the topology")
    void listMultiOmsScenarios() throws Exception {
        String output = capture(GeneratorMain.parseArgs(new String[] {"--multi-oms", "--list-scenarios"}));
        for (MultiOmsScenarioCatalog scenario : MultiOmsScenarioCatalog.values()) {
            assertTrue(output.contains(scenario.cliName()), scenario.cliName());
        }
        for (String topic : MultiOmsTopology.topics()) {
            assertTrue(output.contains(topic), topic);
        }
        assertTrue(output.contains("16666") && output.contains("16667") && output.contains("16668"));
        for (ScenarioCatalog single : ScenarioCatalog.values()) {
            assertFalse(output.contains(single.cliName()), "single-tape catalog must not leak: " + single);
        }
    }

    @Test
    @DisplayName("--help covers the multi-OMS flags too")
    void helpCoversMultiOms() throws Exception {
        String output = capture(GeneratorMain.parseArgs(new String[] {"--help"}));
        assertTrue(output.contains("--multi-oms"));
        assertTrue(output.contains("--children"));
    }

    @Test
    @DisplayName("--multi-oms --dry-run prints '<topic>\\t<pipe-rendered message>' per line")
    void multiOmsDryRunOutput() throws Exception {
        String output = capture(GeneratorMain.parseArgs(new String[] {
            "--multi-oms", "--dry-run", "--orders", "3", "--seed", "42", "--children", "2",
        }));
        List<String> lines = output.lines().filter(l -> !l.isBlank()).toList();
        assertTrue(lines.size() > 24);

        Set<String> topics = new HashSet<>();
        for (String line : lines) {
            String[] parts = line.split("\t", -1);
            assertEquals(2, parts.length, "one tab separating topic from message: " + line);
            topics.add(parts[0]);
            assertTrue(MultiOmsTopology.topics().contains(parts[0]), parts[0]);
            assertTrue(parts[1].startsWith("8=FIX.4.2|"), parts[1]);
            assertTrue(parts[1].endsWith("|"), parts[1]);
            assertEquals(0, parts[1].chars().filter(c -> c == FixTags.SOH).count());
            assertTrue(TestFix.framingValid(parts[1].replace(FixTags.PIPE, FixTags.SOH)), parts[1]);
        }
        assertEquals(Set.copyOf(MultiOmsTopology.topics()), topics);

        long roots = lines.stream()
                .map(l -> l.split("\t", -1)[1])
                .map(TestFix::parse)
                .filter(m -> FixTags.MSG_NEW_ORDER_SINGLE.equals(m.get(FixTags.MSG_TYPE))
                        && MultiOmsTopology.OMS_A.name().equals(m.get(FixTags.SENDER_COMP_ID)))
                .count();
        assertEquals(3, roots, "one OMS-A order per requested family");
    }

    @Test
    @DisplayName("--multi-oms --dry-run is deterministic for a given seed")
    void multiOmsDryRunIsDeterministic() throws Exception {
        String[] args = {"--multi-oms", "--dry-run", "--orders", "4", "--seed", "77",
            "--scenario", "partial_route"};
        assertEquals(
                capture(GeneratorMain.parseArgs(args)).lines()
                        .map(GeneratorMainTest::withoutWallClockFields).toList(),
                capture(GeneratorMain.parseArgs(args)).lines()
                        .map(GeneratorMainTest::withoutWallClockFields).toList());
    }

    @Test
    @DisplayName("--multi-oms --emit-expected writes one JSON object per hub order")
    void multiOmsEmitExpected(@TempDir Path dir) throws Exception {
        Path target = dir.resolve("nested").resolve("expected-oms.json");
        capture(GeneratorMain.parseArgs(new String[] {
            "--multi-oms", "--dry-run", "--orders", "12", "--seed", "42", "--children", "3",
            "--emit-expected", target.toString(),
        }));

        String json = Files.readString(target, StandardCharsets.UTF_8);
        assertTrue(json.startsWith("["));
        assertTrue(json.strip().endsWith("]"));
        for (String column : List.of("Oms", "ClOrdID", "OrderID", "ExtOrdID", "GlobalKey", "RootGlobalKey",
                "Scenario", "OrdStatus", "CumQty", "LeavesQty", "AvgPx", "LinkState", "BreakKind")) {
            assertTrue(json.contains("\"" + column + "\":"), column);
        }
        int rows = json.split("\"GlobalKey\":", -1).length - 1;
        assertEquals(rows, new MultiOmsScenarioEngine(42L, 3).generate(12, ScenarioCatalog.ALL)
                .expectedOrders().size());
        for (String hub : List.of("OMS-A", "OMS-B-parent", "OMS-B-child", "OMS-C")) {
            assertTrue(json.contains("\"Oms\":\"" + hub + "\""), hub);
        }
        for (String kind : List.of("NONE", "UNROUTED", "QTY_BREAK", "DANGLING")) {
            assertTrue(json.contains("\"BreakKind\":\"" + kind + "\""), kind);
        }
        assertTrue(json.contains("\"LinkState\":\"ROOT\""));
        assertTrue(json.contains("\"LinkState\":\"LINKED\""));
        assertTrue(json.contains("\"GlobalKey\":\"OMS-A|A-0001\""));
    }

    @Test
    @DisplayName("single-tape mode is untouched: no topic prefix, no multi-OMS scenarios")
    void singleTapeModeUnchanged() throws Exception {
        String output = capture(GeneratorMain.parseArgs(
                new String[] {"--dry-run", "--orders", "3", "--seed", "42"}));
        for (String line : output.lines().filter(l -> !l.isBlank()).toList()) {
            assertTrue(line.startsWith("8=FIX.4.2|"), line);
            assertEquals(0, line.chars().filter(c -> c == '\t').count(), "no topic prefix in single-tape mode");
        }
    }

    // ---------------------------------------------------------------- AMPS sink (--amps-uri)

    @Test
    @DisplayName("--amps-uri parses in both modes and leaves every other flag alone")
    void ampsUriParsed() {
        GeneratorMain.Config cfg = GeneratorMain.parseArgs(new String[] {
            "--amps-uri", "tcp://localhost:29007/amps/fix", "--orders", "4", "--seed", "42",
        });
        assertEquals("tcp://localhost:29007/amps/fix", cfg.ampsUri());
        assertEquals("localhost:19092", cfg.bootstrapServers(), "the unused Kafka default is left alone");
        assertEquals("fix42.messages", cfg.topic());
        assertEquals(4, cfg.orders());

        GeneratorMain.Config multi = GeneratorMain.parseArgs(new String[] {
            "--multi-oms", "--amps-uri", "tcp://amps:9007/amps/fix", "--children", "2",
        });
        assertEquals("tcp://amps:9007/amps/fix", multi.ampsUri());
        assertTrue(multi.multiOms());
        assertEquals(2, multi.children());

        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--amps-uri"}));
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {"--amps-uri", "  "}));
    }

    @Test
    @DisplayName("--amps-uri with an explicit --bootstrap-servers is rejected: one sink per run")
    void ampsUriRejectsExplicitBootstrap() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {
                    "--amps-uri", "tcp://localhost:29007/amps/fix", "--bootstrap-servers", "kafka:9092",
                }));
        assertTrue(thrown.getMessage().contains("--amps-uri"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("--bootstrap-servers"), thrown.getMessage());
        // Flag order must not matter.
        assertThrows(IllegalArgumentException.class,
                () -> GeneratorMain.parseArgs(new String[] {
                    "--bootstrap-servers", "kafka:9092", "--amps-uri", "tcp://localhost:29007/amps/fix",
                }));
        // The Kafka default on its own is not "explicit", so this stays legal.
        assertEquals("tcp://localhost:29007/amps/fix", GeneratorMain.parseArgs(
                new String[] {"--amps-uri", "tcp://localhost:29007/amps/fix"}).ampsUri());
    }

    @Test
    @DisplayName("the default sink is Kafka: ampsUri is null unless --amps-uri is given")
    void defaultIsKafka() {
        assertNull(GeneratorMain.parseArgs(new String[0]).ampsUri());
        assertNull(GeneratorMain.parseArgs(new String[] {"--multi-oms", "--orders", "2"}).ampsUri());
        assertNull(GeneratorMain.parseArgs(
                new String[] {"--bootstrap-servers", "kafka:9092"}).ampsUri());
    }

    /** Blanks the fields that follow the wall clock: 52/60 and the checksum that covers them. */
    private static String withoutWallClockFields(String line) {
        return line.replaceAll("\\|(52|60)=[^|]*", "|$1=T").replaceAll("\\|10=\\d{3}\\|$", "|10=C|");
    }
}
