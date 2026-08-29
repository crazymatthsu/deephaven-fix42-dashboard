package com.fix42.dashboard.gen;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * The multi-OMS scenario catalog frozen by {@code docs/09-multi-oms-blotter.md} §8.
 *
 * <p>Each constant scripts one <em>family</em> — an {@code OMS-A} order routed through
 * {@code OMS-B-parent}, fanned out to {@code OMS-B-child} orders and on to {@code OMS-C} — and
 * fixes what the blotter's per-edge reconciliation (§5.4) must conclude about it.
 *
 * <p>The enum constant order is also the round-robin order used by {@code --scenario all}, so a
 * short run still exercises every branch before the weighted remainder is drawn.
 */
public enum MultiOmsScenarioCatalog {

    CLEAN_FILL("clean_fill", 30,
            "full route A->Bp->Bc(xk)->C, filled at every level; all deltas 0"),
    WORKING_FANOUT("working_fanout", 20,
            "full route, partial fills only (still working); leaves consistent at every edge"),
    PARTIAL_ROUTE("partial_route", 15,
            "Bp routes 40-70% to children; routed part fills; UNROUTED at Bp"),
    MISSED_FILL("missed_fill", 15,
            "clean_fill with one execution report omitted from the Bp tape; QTY_BREAK on A and Bp"),
    DANGLING_CHILD("dangling_child", 10,
            "clean_fill plus an extra OMS-C order whose 16668 names an id no tape defines"),
    LATE_PARENT("late_parent", 10,
            "clean_fill content with the whole Bp tape emitted after every Bc/C message");

    /** CLI value meaning "weighted mix of every scenario"; the same token as the single-tape catalog. */
    public static final String ALL = ScenarioCatalog.ALL;

    private final String cliName;
    private final int weight;
    private final String script;

    MultiOmsScenarioCatalog(String cliName, int weight, String script) {
        this.cliName = cliName;
        this.weight = weight;
        this.script = script;
    }

    public String cliName() {
        return cliName;
    }

    public int weight() {
        return weight;
    }

    /** Human-readable family skeleton, printed by {@code --multi-oms --list-scenarios}. */
    public String script() {
        return script;
    }

    public static MultiOmsScenarioCatalog fromCliName(String name) {
        String needle = name.trim().toLowerCase(Locale.ROOT);
        return Arrays.stream(values())
                .filter(s -> s.cliName.equals(needle))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "unknown multi-OMS scenario '" + name + "'; known: " + ALL + ", " + cliNames()));
    }

    public static String cliNames() {
        return Arrays.stream(values()).map(MultiOmsScenarioCatalog::cliName).collect(Collectors.joining(", "));
    }
}
