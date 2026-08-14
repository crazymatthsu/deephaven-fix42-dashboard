package com.fix42.dashboard.gen;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * The scenario catalog frozen by {@code docs/05-implementation-and-testing.md} §2.2.
 *
 * <p>The enum constant order is also the round-robin order used by {@code --scenario all} to
 * guarantee that every scenario appears at least once before the weighted remainder is drawn.
 */
public enum ScenarioCatalog {

    NEW_ACK_FILL_FULL("new_ack_fill_full", 25,
            "D -> 8(A) -> 8(0) -> 8(1) x k -> 8(2)"),
    NEW_REJECT("new_reject", 8,
            "D -> 8(150=8, 103 set)"),
    AMEND_ACK("amend_ack", 15,
            "D -> 8(0) -> [8(1)] -> G -> 8(E) -> 8(5, new terms) -> fills to filled"),
    AMEND_REJECT("amend_reject", 8,
            "D -> 8(0) -> G -> 9(434=2, 102, 58)"),
    CANCEL_ACK("cancel_ack", 12,
            "D -> 8(0) -> [8(1)] -> F -> 8(6) -> 8(4)"),
    CANCEL_REJECT("cancel_reject", 8,
            "D -> 8(0) -> 8(1) -> F -> 9(434=1, 39=1)"),
    FILL_BUST("fill_bust", 6,
            "D -> 8(0) -> 8(1) -> 8(20=1, 19=prior ExecID, restated 14/151/6/39)"),
    FILL_CORRECT("fill_correct", 6,
            "D -> 8(0) -> 8(1) -> 8(20=2, 19=prior ExecID, new 31/32, restated snapshots)"),
    DK_TRADE("dk_trade", 6,
            "D -> 8(0) -> 8(1) -> Q(37, 17=that exec, 127)"),
    PARTIAL_THEN_CANCEL("partial_then_cancel", 6,
            "D -> 8(0) -> 8(1) -> F -> 8(6) -> 8(4)");

    /** CLI value meaning "weighted mix of every scenario". */
    public static final String ALL = "all";

    private final String cliName;
    private final int weight;
    private final String sequence;

    ScenarioCatalog(String cliName, int weight, String sequence) {
        this.cliName = cliName;
        this.weight = weight;
        this.sequence = sequence;
    }

    public String cliName() {
        return cliName;
    }

    public int weight() {
        return weight;
    }

    /** Human-readable message skeleton, printed by {@code --list-scenarios}. */
    public String sequence() {
        return sequence;
    }

    public static ScenarioCatalog fromCliName(String name) {
        String needle = name.trim().toLowerCase(Locale.ROOT);
        return Arrays.stream(values())
                .filter(s -> s.cliName.equals(needle))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "unknown scenario '" + name + "'; known: " + ALL + ", " + cliNames()));
    }

    public static String cliNames() {
        return Arrays.stream(values()).map(ScenarioCatalog::cliName).collect(Collectors.joining(", "));
    }
}
