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
import java.util.List;
import java.util.Map;
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
        for (String flag : List.of("--bootstrap-servers", "--topic", "--orders", "--seed", "--rate",
                "--scenario", "--loop", "--list-scenarios", "--dry-run", "--emit-expected")) {
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

    /** Blanks the fields that follow the wall clock: 52/60 and the checksum that covers them. */
    private static String withoutWallClockFields(String line) {
        return line.replaceAll("\\|(52|60)=[^|]*", "|$1=T").replaceAll("\\|10=\\d{3}\\|$", "|10=C|");
    }
}
