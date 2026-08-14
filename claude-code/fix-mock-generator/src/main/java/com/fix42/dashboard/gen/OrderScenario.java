package com.fix42.dashboard.gen;

import java.util.List;

/**
 * One order chain's scripted message sequence.
 *
 * <p>The chain key is the venue {@code 37 OrderID}: it is the Kafka record key for every message of
 * the chain, so all of them land in one partition and per-order ordering is preserved
 * ({@code docs/00-overview.md} §5).
 *
 * @param scenario catalog entry that produced the chain
 * @param chainKey venue OrderID, e.g. {@code ORD-0007}
 * @param steps    messages in strict per-chain order, each with its think-time gap
 * @param expected final cache state the chain must produce
 */
public record OrderScenario(
        ScenarioCatalog scenario,
        String chainKey,
        List<Step> steps,
        ExpectedChainState expected) {

    public OrderScenario {
        steps = List.copyOf(steps);
    }

    /**
     * A single scripted message plus the simulated gap before it reaches the wire.
     *
     * @param message      the FIX message (header slots 34/52/60 still unstamped)
     * @param thinkMillis  simulated delay before this message, used to advance the virtual clock
     */
    public record Step(FixMessage message, long thinkMillis) {}

    public int messageCount() {
        return steps.size();
    }
}
