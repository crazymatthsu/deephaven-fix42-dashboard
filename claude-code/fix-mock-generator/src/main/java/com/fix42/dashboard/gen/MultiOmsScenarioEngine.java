package com.fix42.dashboard.gen;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Deterministic, seeded generator of <em>correlated</em> FIX 4.2 drop-copy tapes for the four-hub
 * topology of {@code docs/09-multi-oms-blotter.md} §3.
 *
 * <p>One "family" is an {@code OMS-A} order routed to {@code OMS-B-parent}, fanned out to {@code k}
 * {@code OMS-B-child} orders and on to one {@code OMS-C} order per child. Every hub keeps its own
 * {@code 11}/{@code 37}/{@code 17} space and emits its own self-consistent doc 01 lifecycle; the
 * hubs are tied together only by the link tag on each downstream {@code D}
 * ({@code 16666}/{@code 16667}/{@code 16668}), which names the upstream order's {@code ClOrdID}.
 *
 * <p>Two properties make the blotter's per-edge reconciliation (doc 09 §5.4) exact rather than
 * approximate:
 *
 * <ul>
 *   <li>no {@code D} carries {@code 37 OrderID}, so each hub's {@code OrderKey} is its {@code D}'s
 *       ClOrdID and {@code GlobalKey} is {@code <Oms>|<that ClOrdID>};</li>
 *   <li>every fill executes at the family's limit price, so {@code AvgPx} is identical at every
 *       level and the notional rollup nets to zero without tolerance games.</li>
 * </ul>
 *
 * <p>One execution propagates to all four tapes as that tape's <em>own</em> ExecutionReport, with
 * its own ExecID and its own absolute {@code 14}/{@code 151}/{@code 6} snapshot. Messages are
 * emitted in causal order — an upstream {@code D}/ack precedes the downstream {@code D} that
 * references it, and a fill travels downstream-tape-first — except in
 * {@link MultiOmsScenarioCatalog#LATE_PARENT}, which defers a whole tape on purpose.
 */
public final class MultiOmsScenarioEngine {

    /** Default {@code --children}: maximum fan-out drawn per {@code OMS-B-parent}. */
    public static final int DEFAULT_MAX_CHILDREN = 3;

    /** Families progressing concurrently in the interleaved stream. */
    public static final int MAX_LIVE_CHAINS = 4;

    private static final DateTimeFormatter FIX_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private static final String[] SYMBOLS = {"AAPL", "MSFT", "NVDA", "AMZN", "TSLA", "META", "GOOGL", "JPM"};
    private static final double[] REF_PX = {228.50, 415.20, 137.80, 186.40, 251.10, 585.30, 172.90, 241.60};
    private static final String[] ACCOUNTS = {"ACC-1", "ACC-2", "ACC-3", "ACC-4", "ACC-5"};
    private static final String[] MARKETS = {"XNAS", "XNYS", "ARCX", "BATS", "EDGX"};
    private static final String[] SIDES = {"1", "1", "1", "2", "2", "5"};
    private static final String[] TIFS = {"0", "0", "0", "0", "1", "3", "4"};

    /** Round lot; every quantity in this mode is a whole number of these. */
    private static final long LOT = 100;

    private static final int MIN_LOTS = 4;
    private static final int MAX_LOTS = 50;
    private static final int MAX_FILL_CHUNKS = 3;

    /** {@code partial_route} routes a seeded fraction of the parent order to its children. */
    private static final double PARTIAL_ROUTE_MIN = 0.40;
    private static final double PARTIAL_ROUTE_MAX = 0.70;

    private final Random random;
    private final int maxChildren;
    private final Instant baseTime;
    private final String timePlaceholder;

    private final Map<String, Long> execSeqByHub = new HashMap<>();
    private final Map<String, Long> msgSeqByHub = new HashMap<>();
    private Instant clock;
    private int chainSeq;

    public MultiOmsScenarioEngine(long seed, int maxChildren) {
        this(seed, maxChildren, Instant.now().truncatedTo(ChronoUnit.SECONDS));
    }

    public MultiOmsScenarioEngine(long seed, int maxChildren, Instant baseTime) {
        if (maxChildren < 1) {
            throw new IllegalArgumentException("maxChildren must be at least 1, got " + maxChildren);
        }
        this.random = new Random(seed);
        this.maxChildren = maxChildren;
        this.baseTime = baseTime;
        this.clock = baseTime;
        this.timePlaceholder = FIX_TIME.format(baseTime);
    }

    /**
     * One message ready for the wire, routed to its hub's topic.
     *
     * @param topic       the hub topic this message is published to
     * @param oms         hub name; also the message's {@code 49 SenderCompID}
     * @param chainKey    Kafka record key: the ClOrdID of the {@code D} that opened this hub order
     * @param scenario    catalog entry that produced the family
     * @param message     the FIX message (header slots 34/52/60 still unstamped before streaming)
     * @param thinkMillis simulated delay before this message, used to advance the virtual clock
     */
    public record EmittedMessage(String topic, String oms, String chainKey,
            MultiOmsScenarioCatalog scenario, FixMessage message, long thinkMillis) {}

    /**
     * One scripted family: its messages in strict per-family order and the blotter rows they must
     * produce, one per hub-order.
     */
    public record MultiOmsChain(MultiOmsScenarioCatalog scenario, String rootGlobalKey,
            List<EmittedMessage> messages, List<ExpectedOmsOrder> orders) {

        public MultiOmsChain {
            messages = List.copyOf(messages);
            orders = List.copyOf(orders);
        }
    }

    /** One generated batch: the family scripts plus their interleaved, stamped message stream. */
    public record GeneratedBatch(List<MultiOmsChain> chains, List<EmittedMessage> messages) {

        /** Every hub-order's expected blotter row, families in generation order. */
        public List<ExpectedOmsOrder> expectedOrders() {
            return chains.stream().flatMap(chain -> chain.orders().stream()).toList();
        }
    }

    /** Scripts {@code count} families for {@code selector} and returns the interleaved stream. */
    public GeneratedBatch generate(int count, String selector) {
        List<MultiOmsChain> chains = buildChains(count, selector);
        List<EmittedMessage> stream = interleave(chains);
        stamp(stream);
        return new GeneratedBatch(chains, stream);
    }

    /** Scripts {@code count} independent families; {@code selector} is a catalog name or {@code all}. */
    public List<MultiOmsChain> buildChains(int count, String selector) {
        if (count <= 0) {
            throw new IllegalArgumentException("orders must be positive, got " + count);
        }
        List<MultiOmsChain> chains = new ArrayList<>(count);
        for (MultiOmsScenarioCatalog scenario : plan(count, selector)) {
            chains.add(script(scenario));
        }
        return chains;
    }

    private List<MultiOmsScenarioCatalog> plan(int count, String selector) {
        List<MultiOmsScenarioCatalog> plan = new ArrayList<>(count);
        if (!MultiOmsScenarioCatalog.ALL.equalsIgnoreCase(selector)) {
            MultiOmsScenarioCatalog only = MultiOmsScenarioCatalog.fromCliName(selector);
            for (int i = 0; i < count; i++) {
                plan.add(only);
            }
            return plan;
        }
        // Cover the catalog first so a short run still exercises every branch, then weight the rest.
        for (MultiOmsScenarioCatalog scenario : MultiOmsScenarioCatalog.values()) {
            if (plan.size() == count) {
                return plan;
            }
            plan.add(scenario);
        }
        int total = 0;
        for (MultiOmsScenarioCatalog scenario : MultiOmsScenarioCatalog.values()) {
            total += scenario.weight();
        }
        while (plan.size() < count) {
            int draw = random.nextInt(total);
            for (MultiOmsScenarioCatalog scenario : MultiOmsScenarioCatalog.values()) {
                draw -= scenario.weight();
                if (draw < 0) {
                    plan.add(scenario);
                    break;
                }
            }
        }
        return plan;
    }

    // ---------------------------------------------------------------- scripting

    private MultiOmsChain script(MultiOmsScenarioCatalog scenario) {
        int no = ++chainSeq;

        int symbolIdx = random.nextInt(SYMBOLS.length);
        double refPx = round2(REF_PX[symbolIdx] * (1 + (random.nextDouble() - 0.5) * 0.04));
        Family family = new Family(
                pick(ACCOUNTS),
                SYMBOLS[symbolIdx],
                pick(SIDES),
                pick(TIFS),
                round2(refPx * (1 + (random.nextDouble() - 0.5) * 0.01)));
        long totalLots = MIN_LOTS + random.nextInt(MAX_LOTS - MIN_LOTS + 1);
        int drawnChildren = 1 + random.nextInt(maxChildren);

        long routedLots = totalLots;
        if (scenario == MultiOmsScenarioCatalog.PARTIAL_ROUTE) {
            double fraction = PARTIAL_ROUTE_MIN + random.nextDouble() * (PARTIAL_ROUTE_MAX - PARTIAL_ROUTE_MIN);
            routedLots = Math.max(1, Math.min(totalLots - 1, Math.round(totalLots * fraction)));
        }
        // working_fanout leaves every child working, so each needs room for a strictly partial fill.
        long minChildLots = scenario == MultiOmsScenarioCatalog.WORKING_FANOUT ? 2 : 1;
        long[] childLots = splitLots(routedLots, drawnChildren, minChildLots);

        HubOrder root = new HubOrder(MultiOmsTopology.OMS_A, family,
                "A-%04d".formatted(no), "OA-%04d".formatted(no), "", totalLots * LOT);
        HubOrder parent = new HubOrder(MultiOmsTopology.OMS_B_PARENT, family,
                "BP-%04d".formatted(no), "OBP-%04d".formatted(no), root.clOrdId, totalLots * LOT);
        parent.linkTo(root);

        List<HubOrder> children = new ArrayList<>(childLots.length);
        List<HubOrder> venueOrders = new ArrayList<>(childLots.length);
        for (int j = 0; j < childLots.length; j++) {
            long qty = childLots[j] * LOT;
            HubOrder child = new HubOrder(MultiOmsTopology.OMS_B_CHILD, family,
                    "BC-%04d-%d".formatted(no, j + 1), "OBC-%04d-%d".formatted(no, j + 1),
                    parent.clOrdId, qty);
            child.linkTo(parent);
            HubOrder venue = new HubOrder(MultiOmsTopology.OMS_C, family,
                    "C-%04d-%d".formatted(no, j + 1), "OC-%04d-%d".formatted(no, j + 1),
                    child.clOrdId, qty);
            venue.linkTo(child);
            children.add(child);
            venueOrders.add(venue);
        }

        // An extra OMS-C order whose link value no tape ever defines: LinkState DANGLING forever.
        HubOrder orphan = null;
        if (scenario == MultiOmsScenarioCatalog.DANGLING_CHILD) {
            int index = childLots.length + 1;
            long lots = 1 + random.nextInt(5);
            orphan = new HubOrder(MultiOmsTopology.OMS_C, family,
                    "C-%04d-%d".formatted(no, index), "OC-%04d-%d".formatted(no, index),
                    "MISSING-%04d".formatted(no), lots * LOT);
        }

        List<PlannedStep> scripted = new ArrayList<>();
        open(scripted, root);
        open(scripted, parent);
        for (HubOrder child : children) {
            open(scripted, child);
        }
        for (HubOrder venue : venueOrders) {
            open(scripted, venue);
        }
        if (orphan != null) {
            open(scripted, orphan);
        }

        for (FillEvent event : fillSchedule(childLots, scenario)) {
            long shares = event.lots() * LOT;
            String market = pick(MARKETS);
            // The execution happens at OMS-C and is reported up the chain, one tape at a time.
            scripted.add(fill(venueOrders.get(event.childIndex()), shares, market, think(150, 1400)));
            scripted.add(fill(children.get(event.childIndex()), shares, market, think(5, 40)));
            scripted.add(fill(parent, shares, market, think(5, 40)));
            scripted.add(fill(root, shares, market, think(5, 40)));
        }

        if (scenario == MultiOmsScenarioCatalog.MISSED_FILL) {
            dropLastMessageOf(scripted, parent);
        }
        List<PlannedStep> steps = scenario == MultiOmsScenarioCatalog.LATE_PARENT
                ? deferTapeOf(scripted, parent)
                : scripted;

        adoptReportedState(steps);

        List<HubOrder> exported = new ArrayList<>();
        exported.add(root);
        exported.add(parent);
        for (int j = 0; j < children.size(); j++) {
            exported.add(children.get(j));
            exported.add(venueOrders.get(j));
        }
        if (orphan != null) {
            exported.add(orphan);
        }

        List<EmittedMessage> messages = steps.stream()
                .map(step -> new EmittedMessage(step.order().hub.topic(), step.order().hub.name(),
                        step.order().clOrdId, scenario, step.message(), step.thinkMillis()))
                .toList();
        List<ExpectedOmsOrder> orders = exported.stream()
                .map(order -> order.expected(scenario))
                .toList();
        return new MultiOmsChain(scenario, root.globalKey(), messages, orders);
    }

    /** A single execution against one child branch. */
    private record FillEvent(int childIndex, long lots) {}

    /**
     * Chunks each child's executed quantity and round-robins the chunks across children, so every
     * tape sees monotone {@code CumQty} while several branches trade concurrently.
     */
    private List<FillEvent> fillSchedule(long[] childLots, MultiOmsScenarioCatalog scenario) {
        // Every child contributes at least one execution, so a fan-out already gives the parent tape
        // two or more reports. A single child with a single chunk would not: withholding that one
        // report would leave OMS-B-parent sitting at its ack instead of partially filled, which is
        // not the break missed_fill is meant to inject. Force a second chunk in exactly that case.
        int minChunks = scenario == MultiOmsScenarioCatalog.MISSED_FILL && childLots.length == 1 ? 2 : 1;

        List<Deque<Long>> pending = new ArrayList<>(childLots.length);
        for (int j = 0; j < childLots.length; j++) {
            long lots = childLots[j];
            long executed = scenario == MultiOmsScenarioCatalog.WORKING_FANOUT
                    ? 1 + random.nextInt((int) (lots - 1))   // strictly partial: still working
                    : lots;
            int parts = Math.max(j == 0 ? minChunks : 1, 1 + random.nextInt(MAX_FILL_CHUNKS));
            Deque<Long> chunks = new ArrayDeque<>();
            for (long chunk : splitLots(executed, parts, 1)) {
                chunks.add(chunk);
            }
            pending.add(chunks);
        }
        List<FillEvent> schedule = new ArrayList<>();
        boolean progressed = true;
        while (progressed) {
            progressed = false;
            for (int j = 0; j < pending.size(); j++) {
                Long chunk = pending.get(j).poll();
                if (chunk != null) {
                    schedule.add(new FillEvent(j, chunk));
                    progressed = true;
                }
            }
        }
        return schedule;
    }

    /** Removes the last message {@code order}'s tape would have carried (a {@code missed_fill} gap). */
    private static void dropLastMessageOf(List<PlannedStep> steps, HubOrder order) {
        for (int i = steps.size() - 1; i >= 0; i--) {
            if (steps.get(i).order() == order) {
                steps.remove(i);
                return;
            }
        }
    }

    /** Moves every message of {@code order}'s tape after all the others, preserving per-tape order. */
    private static List<PlannedStep> deferTapeOf(List<PlannedStep> steps, HubOrder order) {
        List<PlannedStep> rest = new ArrayList<>(steps.size());
        List<PlannedStep> deferred = new ArrayList<>();
        for (PlannedStep step : steps) {
            (step.order() == order ? deferred : rest).add(step);
        }
        rest.addAll(deferred);
        return rest;
    }

    /**
     * Reads each hub order's terminal state back off its own tape, so the export always describes
     * what was actually emitted — including the report {@code missed_fill} withholds.
     */
    private static void adoptReportedState(List<PlannedStep> steps) {
        Map<HubOrder, FixMessage> lastExec = new LinkedHashMap<>();
        for (PlannedStep step : steps) {
            if (FixTags.MSG_EXECUTION_REPORT.equals(step.message().msgType())) {
                lastExec.put(step.order(), step.message());
            }
        }
        lastExec.forEach((order, message) -> {
            order.reportedCumQty = Double.parseDouble(message.get(FixTags.CUM_QTY));
            order.reportedLeavesQty = Double.parseDouble(message.get(FixTags.LEAVES_QTY));
            order.reportedAvgPx = Double.parseDouble(message.get(FixTags.AVG_PX));
            order.reportedStatus = message.get(FixTags.ORD_STATUS);
        });
    }

    // ---------------------------------------------------------------- messages

    /** Appends the order's {@code D} and its {@code 150=0} ack. */
    private void open(List<PlannedStep> steps, HubOrder order) {
        steps.add(new PlannedStep(order, newOrderSingle(order), think(80, 400)));
        steps.add(new PlannedStep(order, ack(order), think(20, 160)));
    }

    private FixMessage header(HubOrder order, String msgType) {
        return FixMessage.create(msgType)
                .set(FixTags.SENDER_COMP_ID, order.hub.name())
                .set(FixTags.TARGET_COMP_ID, FixTags.TARGET_DROP_COPY)
                .set(FixTags.MSG_SEQ_NUM, 1)
                .set(FixTags.SENDING_TIME, timePlaceholder);
    }

    /**
     * The order request. It deliberately carries no {@code 37 OrderID} — the hub has not assigned
     * one yet — so the cache keys the chain by this {@code 11 ClOrdID} (doc 01 §3 step 4), which is
     * what makes {@code GlobalKey} predictable from the export.
     */
    private FixMessage newOrderSingle(HubOrder order) {
        Family f = order.family;
        FixMessage m = header(order, FixTags.MSG_NEW_ORDER_SINGLE)
                .set(FixTags.CL_ORD_ID, order.clOrdId)
                .set(FixTags.ACCOUNT, f.account())
                .set(FixTags.HANDL_INST, "1")
                .set(FixTags.SYMBOL, f.symbol())
                .set(FixTags.SIDE, f.side())
                .set(FixTags.TRANSACT_TIME, timePlaceholder)
                .setQty(FixTags.ORDER_QTY, order.orderQty)
                .set(FixTags.ORD_TYPE, FixTags.ORD_TYPE_LIMIT)
                .setPrice(FixTags.PRICE, f.price())
                .set(FixTags.TIME_IN_FORCE, f.timeInForce());
        if (!order.hub.isRoot()) {
            m.set(order.hub.linkTag(), order.extOrdId);
        }
        return m;
    }

    private FixMessage ack(HubOrder order) {
        return buildExec(order, FixTags.EXEC_TYPE_NEW, FixTags.ORD_STATUS_NEW, null, 0, null);
    }

    /** One execution on this tape: absolute snapshots, always at the family's limit price. */
    private PlannedStep fill(HubOrder order, long shares, String market, long thinkMillis) {
        order.cumQty += shares;
        order.avgPx = order.family.price();
        boolean complete = order.cumQty >= order.orderQty;
        FixMessage message = buildExec(order,
                complete ? FixTags.EXEC_TYPE_FILL : FixTags.EXEC_TYPE_PARTIAL_FILL,
                complete ? FixTags.ORD_STATUS_FILLED : FixTags.ORD_STATUS_PARTIALLY_FILLED,
                market, shares, order.family.price());
        return new PlannedStep(order, message, thinkMillis);
    }

    /** Lays down an ExecutionReport body in FIX 4.2 field order. */
    private FixMessage buildExec(HubOrder order, String execType, String ordStatus,
            String market, double lastShares, Double lastPx) {
        Family f = order.family;
        FixMessage m = header(order, FixTags.MSG_EXECUTION_REPORT)
                .set(FixTags.ORDER_ID, order.orderId)
                .set(FixTags.CL_ORD_ID, order.clOrdId)
                .set(FixTags.EXEC_ID, nextExecId(order.hub))
                .set(FixTags.EXEC_TRANS_TYPE, FixTags.EXEC_TRANS_NEW)
                .set(FixTags.EXEC_TYPE, execType)
                .set(FixTags.ORD_STATUS, ordStatus)
                .set(FixTags.ACCOUNT, f.account())
                .set(FixTags.SYMBOL, f.symbol())
                .set(FixTags.SIDE, f.side())
                .setQty(FixTags.ORDER_QTY, order.orderQty)
                .set(FixTags.ORD_TYPE, FixTags.ORD_TYPE_LIMIT)
                .setPrice(FixTags.PRICE, f.price())
                .set(FixTags.TIME_IN_FORCE, f.timeInForce());
        if (lastPx != null) {
            m.setQty(FixTags.LAST_SHARES, lastShares)
                    .setPrice(FixTags.LAST_PX, lastPx)
                    .set(FixTags.LAST_MKT, market);
        }
        return m.setQty(FixTags.LEAVES_QTY, order.orderQty - order.cumQty)
                .setQty(FixTags.CUM_QTY, order.cumQty)
                .setPrice(FixTags.AVG_PX, order.avgPx)
                .set(FixTags.TRANSACT_TIME, timePlaceholder);
    }

    // ---------------------------------------------------------------- streaming

    /**
     * Round-robin interleaves the families across a window of at most {@link #MAX_LIVE_CHAINS} live
     * families; per-family order — and therefore per-tape order — is strictly preserved.
     */
    private static List<EmittedMessage> interleave(List<MultiOmsChain> chains) {
        List<EmittedMessage> out = new ArrayList<>();
        Deque<MultiOmsChain> waiting = new ArrayDeque<>(chains);
        List<Cursor> live = new ArrayList<>();
        while (live.size() < MAX_LIVE_CHAINS && !waiting.isEmpty()) {
            live.add(new Cursor(waiting.poll()));
        }
        while (!live.isEmpty()) {
            for (int i = 0; i < live.size(); ) {
                Cursor cursor = live.get(i);
                out.add(cursor.next());
                if (cursor.exhausted()) {
                    live.remove(i);
                    if (!waiting.isEmpty()) {
                        live.add(new Cursor(waiting.poll()));
                    }
                } else {
                    i++;
                }
            }
        }
        return out;
    }

    /** Stamps {@code 34} per hub session and {@code 52}/{@code 60} monotonically in wire order. */
    private void stamp(List<EmittedMessage> stream) {
        for (EmittedMessage emitted : stream) {
            clock = clock.plusMillis(Math.max(1L, emitted.thinkMillis()));
            String now = FIX_TIME.format(clock);
            FixMessage message = emitted.message();
            message.set(FixTags.MSG_SEQ_NUM, msgSeqByHub.merge(emitted.oms(), 1L, Long::sum));
            message.set(FixTags.SENDING_TIME, now);
            if (message.has(FixTags.TRANSACT_TIME)) {
                message.set(FixTags.TRANSACT_TIME, now);
            }
        }
    }

    /** Cursor over one family's messages, used by the round-robin interleaver. */
    private static final class Cursor {
        private final MultiOmsChain chain;
        private int index;

        Cursor(MultiOmsChain chain) {
            this.chain = chain;
        }

        EmittedMessage next() {
            return chain.messages().get(index++);
        }

        boolean exhausted() {
            return index >= chain.messages().size();
        }
    }

    // ---------------------------------------------------------------- family state

    /** Terms shared by every hub order of one family: the same economic flow seen four times. */
    private record Family(String account, String symbol, String side, String timeInForce, double price) {}

    /** One scripted message together with the hub order whose tape carries it. */
    private record PlannedStep(HubOrder order, FixMessage message, long thinkMillis) {}

    /** Book-keeping for one hub's order while its tape is being written. */
    private static final class HubOrder {

        private final MultiOmsTopology.Hub hub;
        private final Family family;
        private final String clOrdId;
        private final String orderId;
        private final String extOrdId;
        private final long orderQty;
        private final List<HubOrder> children = new ArrayList<>();

        private HubOrder parent;
        private long cumQty;
        private double avgPx;

        private double reportedCumQty;
        private double reportedLeavesQty;
        private double reportedAvgPx;
        private String reportedStatus = FixTags.ORD_STATUS_PENDING_NEW;

        HubOrder(MultiOmsTopology.Hub hub, Family family,
                String clOrdId, String orderId, String extOrdId, long orderQty) {
            this.hub = hub;
            this.family = family;
            this.clOrdId = clOrdId;
            this.orderId = orderId;
            this.extOrdId = extOrdId;
            this.orderQty = orderQty;
            this.reportedLeavesQty = orderQty;
        }

        void linkTo(HubOrder upstream) {
            this.parent = upstream;
            upstream.children.add(this);
        }

        String globalKey() {
            return MultiOmsTopology.globalKey(hub.name(), clOrdId);
        }

        /** Doc 09 §5.3: a link value that resolves is {@code LINKED}, one that does not dangles. */
        String linkState() {
            if (extOrdId.isEmpty()) {
                return hub.isRoot() ? ExpectedOmsOrder.LINK_ROOT : "NO_LINK";
            }
            return parent != null ? ExpectedOmsOrder.LINK_LINKED : ExpectedOmsOrder.LINK_DANGLING;
        }

        /** A {@code DANGLING} order is its own root until (unless) its parent appears. */
        String rootGlobalKey() {
            HubOrder order = this;
            while (order.parent != null) {
                order = order.parent;
            }
            return order.globalKey();
        }

        /** This order's blotter row, with {@code BreakKind} from the doc 09 §5.4 edge math. */
        ExpectedOmsOrder expected(MultiOmsScenarioCatalog scenario) {
            double childCumQty = 0;
            double childLeavesQty = 0;
            double childNotional = 0;
            for (HubOrder child : children) {
                childCumQty += child.reportedCumQty;
                childLeavesQty += child.reportedLeavesQty;
                childNotional += child.reportedAvgPx * child.reportedCumQty;
            }
            String linkState = linkState();
            String breakKind = ExpectedOmsOrder.breakKind(linkState, !children.isEmpty(),
                    reportedCumQty - childCumQty,
                    reportedAvgPx * reportedCumQty - childNotional,
                    reportedLeavesQty - childLeavesQty);
            return new ExpectedOmsOrder(hub.name(), clOrdId, orderId, extOrdId,
                    globalKey(), rootGlobalKey(), scenario.cliName(),
                    FixTags.ordStatusName(reportedStatus),
                    reportedCumQty, reportedLeavesQty, reportedAvgPx, linkState, breakKind);
        }
    }

    // ---------------------------------------------------------------- helpers

    private String nextExecId(MultiOmsTopology.Hub hub) {
        return execPrefix(hub) + "%06d".formatted(execSeqByHub.merge(hub.name(), 1L, Long::sum));
    }

    /** Each hub owns a private, monotone ExecID space; the prefix keeps them readable and disjoint. */
    private static String execPrefix(MultiOmsTopology.Hub hub) {
        if (hub.equals(MultiOmsTopology.OMS_A)) {
            return "EA-";
        }
        if (hub.equals(MultiOmsTopology.OMS_B_PARENT)) {
            return "EBP-";
        }
        if (hub.equals(MultiOmsTopology.OMS_B_CHILD)) {
            return "EBC-";
        }
        return "EC-";
    }

    /**
     * Splits {@code totalLots} into at most {@code parts} chunks of at least {@code minPer} lots
     * each, summing exactly to the total. {@code parts} is clamped down when the total cannot feed
     * that many minimum-sized chunks.
     */
    private long[] splitLots(long totalLots, int parts, long minPer) {
        int n = (int) Math.max(1, Math.min(parts, totalLots / minPer));
        long[] out = new long[n];
        long remaining = totalLots;
        for (int i = 0; i < n - 1; i++) {
            long max = remaining - (long) (n - 1 - i) * minPer;
            long take = minPer + (long) (random.nextDouble() * (Math.max(0, max - minPer) / 2.0 + 1));
            out[i] = Math.min(take, max);
            remaining -= out[i];
        }
        out[n - 1] = remaining;
        return out;
    }

    private long think(int minMillis, int maxMillis) {
        return minMillis + random.nextInt(maxMillis - minMillis);
    }

    private <T> T pick(T[] pool) {
        return pool[random.nextInt(pool.length)];
    }

    private static double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
