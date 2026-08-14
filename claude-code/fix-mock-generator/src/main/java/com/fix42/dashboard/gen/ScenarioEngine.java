package com.fix42.dashboard.gen;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;

/**
 * Deterministic, seeded generator of FIX 4.2 order-chain scripts.
 *
 * <p>The engine plays the venue side: it owns the {@code 37 OrderID} / {@code 17 ExecID} counters
 * and all absolute {@code 14 CumQty} / {@code 151 LeavesQty} / {@code 6 AvgPx} arithmetic, exactly
 * as a matching engine would, so the Deephaven state machine can adopt the snapshots verbatim
 * ({@code docs/01-fix42-messages-and-state-machine.md} §5).
 *
 * <p>Chains are scripted independently, then round-robin interleaved across a window of "live"
 * chains so the dashboard shows many orders progressing at once; per-chain order is strictly
 * preserved. {@code 34 MsgSeqNum} (per session direction), {@code 52 SendingTime} and
 * {@code 60 TransactTime} are stamped after interleaving so they are monotone in wire order.
 *
 * <p>Everything is drawn from a single seeded {@link Random}: the same seed yields the same
 * messages and the same keys.
 */
public final class ScenarioEngine {

    /** Maximum number of chains progressing concurrently in the interleaved stream. */
    public static final int MAX_LIVE_CHAINS = 6;

    private static final DateTimeFormatter FIX_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private static final String[] SYMBOLS = {"AAPL", "MSFT", "NVDA", "AMZN", "TSLA", "META", "GOOGL", "JPM"};
    private static final double[] REF_PX = {228.50, 415.20, 137.80, 186.40, 251.10, 585.30, 172.90, 241.60};
    private static final String[] ACCOUNTS = {"ACC-1", "ACC-2", "ACC-3", "ACC-4", "ACC-5"};
    private static final String[] MARKETS = {"XNAS", "XNYS", "ARCX", "BATS", "EDGX"};
    private static final String[] SIDES = {"1", "1", "1", "2", "2", "5"};
    private static final String[] TIFS = {"0", "0", "0", "0", "1", "3", "4"};

    private static final String[][] ORD_REJ_REASONS = {
        {"1", "unknown symbol"},
        {"2", "exchange closed"},
        {"3", "order exceeds limit"},
        {"4", "too late to enter"},
        {"11", "unsupported order characteristic"},
    };

    private static final String[][] CXL_REJ_REASONS = {
        {"0", "too late to cancel"},
        {"1", "unknown order"},
        {"2", "broker option"},
        {"3", "order already in pending cancel or pending replace status"},
    };

    private static final String[][] DK_REASONS = {
        {"A", "unknown symbol"},
        {"B", "wrong side"},
        {"C", "quantity exceeds order"},
        {"D", "no matching order"},
        {"E", "price exceeds limit"},
        {"F", "calculation difference"},
        {"Z", "other"},
    };

    private final Random random;
    private final Instant baseTime;
    private final String timePlaceholder;

    private int chainSeq;
    private long execSeq;
    private Instant clock;
    private long clientSeqNum = 1;
    private long venueSeqNum = 1;

    public ScenarioEngine(long seed) {
        this(seed, Instant.now().truncatedTo(ChronoUnit.SECONDS));
    }

    public ScenarioEngine(long seed, Instant baseTime) {
        this.random = new Random(seed);
        this.baseTime = baseTime;
        this.clock = baseTime;
        this.timePlaceholder = FIX_TIME.format(baseTime);
    }

    /** A message ready for the wire, tagged with the chain it belongs to. */
    public record EmittedMessage(String chainKey, ScenarioCatalog scenario, FixMessage message, long thinkMillis) {}

    /** One generated batch: the chain scripts plus their interleaved, stamped message stream. */
    public record GeneratedBatch(List<OrderScenario> chains, List<EmittedMessage> messages) {

        public List<ExpectedChainState> expectedStates() {
            return chains.stream().map(OrderScenario::expected).toList();
        }
    }

    /** Scripts {@code count} chains for {@code selector} and returns the interleaved stream. */
    public GeneratedBatch generate(int count, String selector) {
        List<OrderScenario> chains = buildChains(count, selector);
        return new GeneratedBatch(chains, stream(chains));
    }

    /** Scripts {@code count} independent order chains; {@code selector} is a catalog name or {@code all}. */
    public List<OrderScenario> buildChains(int count, String selector) {
        if (count <= 0) {
            throw new IllegalArgumentException("orders must be positive, got " + count);
        }
        List<ScenarioCatalog> plan = plan(count, selector);
        List<OrderScenario> chains = new ArrayList<>(count);
        for (ScenarioCatalog scenario : plan) {
            chains.add(script(scenario));
        }
        return chains;
    }

    /**
     * Round-robin interleaves the chains across a window of at most {@link #MAX_LIVE_CHAINS} live
     * chains and stamps sequence numbers and timestamps in wire order.
     */
    public List<EmittedMessage> stream(List<OrderScenario> chains) {
        List<EmittedMessage> out = new ArrayList<>();
        Deque<OrderScenario> pending = new ArrayDeque<>(chains);
        List<Cursor> live = new ArrayList<>();
        while (live.size() < MAX_LIVE_CHAINS && !pending.isEmpty()) {
            live.add(new Cursor(pending.poll()));
        }
        while (!live.isEmpty()) {
            for (int i = 0; i < live.size(); ) {
                Cursor cursor = live.get(i);
                OrderScenario.Step step = cursor.next();
                out.add(new EmittedMessage(
                        cursor.chain.chainKey(), cursor.chain.scenario(), step.message(), step.thinkMillis()));
                if (cursor.exhausted()) {
                    live.remove(i);
                    if (!pending.isEmpty()) {
                        live.add(new Cursor(pending.poll()));
                    }
                } else {
                    i++;
                }
            }
        }
        stamp(out);
        return out;
    }

    private void stamp(List<EmittedMessage> stream) {
        for (EmittedMessage emitted : stream) {
            clock = clock.plusMillis(Math.max(1L, emitted.thinkMillis()));
            String now = FIX_TIME.format(clock);
            FixMessage message = emitted.message();
            boolean fromVenue = FixTags.SENDER_VENUE.equals(message.get(FixTags.SENDER_COMP_ID));
            message.set(FixTags.MSG_SEQ_NUM, fromVenue ? venueSeqNum++ : clientSeqNum++);
            message.set(FixTags.SENDING_TIME, now);
            if (message.has(FixTags.TRANSACT_TIME)) {
                message.set(FixTags.TRANSACT_TIME, now);
            }
        }
    }

    /** Instant the engine's virtual clock started from; useful for reproducible tests. */
    public Instant baseTime() {
        return baseTime;
    }

    private List<ScenarioCatalog> plan(int count, String selector) {
        List<ScenarioCatalog> plan = new ArrayList<>(count);
        if (!ScenarioCatalog.ALL.equalsIgnoreCase(selector)) {
            ScenarioCatalog only = ScenarioCatalog.fromCliName(selector);
            for (int i = 0; i < count; i++) {
                plan.add(only);
            }
            return plan;
        }
        // Cover the catalog first so a short run still exercises every branch, then weight the rest.
        for (ScenarioCatalog scenario : ScenarioCatalog.values()) {
            if (plan.size() == count) {
                return plan;
            }
            plan.add(scenario);
        }
        int total = 0;
        for (ScenarioCatalog scenario : ScenarioCatalog.values()) {
            total += scenario.weight();
        }
        while (plan.size() < count) {
            int draw = random.nextInt(total);
            for (ScenarioCatalog scenario : ScenarioCatalog.values()) {
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

    private OrderScenario script(ScenarioCatalog scenario) {
        Chain chain = newChain(scenario);
        switch (scenario) {
            case NEW_ACK_FILL_FULL -> scriptNewAckFillFull(chain);
            case NEW_REJECT -> scriptNewReject(chain);
            case AMEND_ACK -> scriptAmendAck(chain);
            case AMEND_REJECT -> scriptAmendReject(chain);
            case CANCEL_ACK -> scriptCancelAck(chain);
            case CANCEL_REJECT -> scriptCancelReject(chain);
            case FILL_BUST -> scriptFillBust(chain);
            case FILL_CORRECT -> scriptFillCorrect(chain);
            case DK_TRADE -> scriptDkTrade(chain);
            case PARTIAL_THEN_CANCEL -> scriptPartialThenCancel(chain);
        }
        return new OrderScenario(scenario, chain.orderId, chain.steps, chain.expected());
    }

    private void scriptNewAckFillFull(Chain c) {
        c.add(newOrderSingle(c), think(80, 400));
        c.add(pendingNew(c), think(15, 90));
        c.add(ack(c), think(20, 160));
        long[] chunks = splitQty(c.orderQty, 2 + random.nextInt(3));
        for (long chunk : chunks) {
            c.add(fill(c, chunk), think(150, 1400));
        }
    }

    private void scriptNewReject(Chain c) {
        c.add(newOrderSingle(c), think(80, 400));
        String[] reason = pick(ORD_REJ_REASONS);
        c.add(buildExec(c, new ExecSpec()
                .execType(FixTags.EXEC_TYPE_REJECTED)
                .ordStatus(FixTags.ORD_STATUS_REJECTED)
                .clOrdId(c.currentClOrdId)
                .ordRejReason(reason[0])
                .snapshot(0, 0, 0)
                .text("rejected: " + reason[1])), think(30, 200));
    }

    private void scriptAmendAck(Chain c) {
        c.add(newOrderSingle(c), think(80, 400));
        c.add(ack(c), think(20, 160));
        if (random.nextBoolean()) {
            c.add(fill(c, splitQty(c.orderQty, 3)[0]), think(200, 1200));
        }
        long newQty = amendedQty(c);
        double newPrice = amendedPrice(c);
        c.add(cancelReplaceRequest(c, newQty, newPrice), think(400, 2500));
        c.add(pendingReplace(c), think(20, 120));
        c.add(replaced(c), think(30, 200));
        long remaining = c.orderQty - c.cumQty;
        for (long chunk : splitQty(remaining, 1 + random.nextInt(2))) {
            c.add(fill(c, chunk), think(150, 1400));
        }
    }

    private void scriptAmendReject(Chain c) {
        c.add(newOrderSingle(c), think(80, 400));
        c.add(ack(c), think(20, 160));
        c.add(cancelReplaceRequest(c, amendedQty(c), amendedPrice(c)), think(400, 2500));
        c.add(cancelReject(c, FixTags.CXL_REJ_RESPONSE_TO_REPLACE, c.ordStatus), think(30, 250));
    }

    private void scriptCancelAck(Chain c) {
        c.add(newOrderSingle(c), think(80, 400));
        c.add(ack(c), think(20, 160));
        if (random.nextBoolean()) {
            c.add(fill(c, splitQty(c.orderQty, 3)[0]), think(200, 1200));
        }
        c.add(cancelRequest(c), think(400, 2500));
        c.add(pendingCancel(c), think(20, 120));
        c.add(canceled(c), think(30, 220));
    }

    private void scriptCancelReject(Chain c) {
        c.add(newOrderSingle(c), think(80, 400));
        c.add(ack(c), think(20, 160));
        c.add(fill(c, splitQty(c.orderQty, 3)[0]), think(200, 1200));
        c.add(cancelRequest(c), think(400, 2500));
        c.add(cancelReject(c, FixTags.CXL_REJ_RESPONSE_TO_CANCEL, c.ordStatus), think(30, 250));
    }

    private void scriptFillBust(Chain c) {
        c.add(newOrderSingle(c), think(80, 400));
        c.add(ack(c), think(20, 160));
        c.add(fill(c, splitQty(c.orderQty, 3)[0]), think(200, 1200));
        c.add(bust(c), think(600, 3000));
    }

    private void scriptFillCorrect(Chain c) {
        c.add(newOrderSingle(c), think(80, 400));
        c.add(ack(c), think(20, 160));
        c.add(fill(c, splitQty(c.orderQty, 3)[0]), think(200, 1200));
        c.add(correct(c), think(600, 3000));
    }

    private void scriptDkTrade(Chain c) {
        c.add(newOrderSingle(c), think(80, 400));
        c.add(ack(c), think(20, 160));
        c.add(fill(c, splitQty(c.orderQty, 3)[0]), think(200, 1200));
        c.add(dontKnowTrade(c), think(500, 2500));
    }

    private void scriptPartialThenCancel(Chain c) {
        c.add(newOrderSingle(c), think(80, 400));
        c.add(ack(c), think(20, 160));
        c.add(fill(c, splitQty(c.orderQty, 3)[0]), think(200, 1200));
        c.add(cancelRequest(c), think(400, 2500));
        c.add(pendingCancel(c), think(20, 120));
        c.add(canceled(c), think(30, 220));
    }

    // ---------------------------------------------------------------- messages

    private FixMessage header(String msgType, boolean fromVenue) {
        return FixMessage.create(msgType)
                .set(FixTags.SENDER_COMP_ID, fromVenue ? FixTags.SENDER_VENUE : FixTags.SENDER_CLIENT)
                .set(FixTags.TARGET_COMP_ID, fromVenue ? FixTags.SENDER_CLIENT : FixTags.SENDER_VENUE)
                .set(FixTags.MSG_SEQ_NUM, 1)
                .set(FixTags.SENDING_TIME, timePlaceholder);
    }

    private FixMessage newOrderSingle(Chain c) {
        FixMessage m = header(FixTags.MSG_NEW_ORDER_SINGLE, false)
                .set(FixTags.CL_ORD_ID, c.currentClOrdId)
                .set(FixTags.ACCOUNT, c.account)
                .set(FixTags.HANDL_INST, "1")
                .set(FixTags.SYMBOL, c.symbol)
                .set(FixTags.SIDE, c.side)
                .set(FixTags.TRANSACT_TIME, timePlaceholder)
                .setQty(FixTags.ORDER_QTY, c.orderQty)
                .set(FixTags.ORD_TYPE, c.ordType);
        if (c.isLimit()) {
            m.setPrice(FixTags.PRICE, c.price);
        }
        return m.set(FixTags.TIME_IN_FORCE, c.timeInForce);
    }

    private FixMessage cancelReplaceRequest(Chain c, long newQty, double newPrice) {
        String requestId = c.nextClOrdId();
        FixMessage m = header(FixTags.MSG_CANCEL_REPLACE_REQUEST, false)
                .set(FixTags.ORDER_ID, c.orderId)
                .set(FixTags.ORIG_CL_ORD_ID, c.currentClOrdId)
                .set(FixTags.CL_ORD_ID, requestId)
                .set(FixTags.ACCOUNT, c.account)
                .set(FixTags.HANDL_INST, "1")
                .set(FixTags.SYMBOL, c.symbol)
                .set(FixTags.SIDE, c.side)
                .set(FixTags.TRANSACT_TIME, timePlaceholder)
                .setQty(FixTags.ORDER_QTY, newQty)
                .set(FixTags.ORD_TYPE, c.ordType);
        if (c.isLimit()) {
            m.setPrice(FixTags.PRICE, newPrice);
        }
        m.set(FixTags.TIME_IN_FORCE, c.timeInForce);
        c.pendingClOrdId = requestId;
        c.stagedQty = newQty;
        c.stagedPrice = newPrice;
        return m;
    }

    private FixMessage cancelRequest(Chain c) {
        String requestId = c.nextClOrdId();
        FixMessage m = header(FixTags.MSG_CANCEL_REQUEST, false)
                .set(FixTags.ORDER_ID, c.orderId)
                .set(FixTags.ORIG_CL_ORD_ID, c.currentClOrdId)
                .set(FixTags.CL_ORD_ID, requestId)
                .set(FixTags.ACCOUNT, c.account)
                .set(FixTags.SYMBOL, c.symbol)
                .set(FixTags.SIDE, c.side)
                .set(FixTags.TRANSACT_TIME, timePlaceholder)
                .setQty(FixTags.ORDER_QTY, c.orderQty);
        c.pendingClOrdId = requestId;
        return m;
    }

    private FixMessage cancelReject(Chain c, String responseTo, String ordStatus) {
        String[] reason = pick(CXL_REJ_REASONS);
        FixMessage m = header(FixTags.MSG_CANCEL_REJECT, true)
                .set(FixTags.ORDER_ID, c.orderId)
                .set(FixTags.CL_ORD_ID, c.pendingClOrdId)
                .set(FixTags.ORIG_CL_ORD_ID, c.currentClOrdId)
                .set(FixTags.ORD_STATUS, ordStatus)
                .set(FixTags.ACCOUNT, c.account)
                .set(FixTags.TRANSACT_TIME, timePlaceholder)
                .set(FixTags.CXL_REJ_RESPONSE_TO, responseTo)
                .set(FixTags.CXL_REJ_REASON, reason[0])
                .set(FixTags.TEXT, "reject: " + reason[1]);
        c.ordStatus = ordStatus;
        c.pendingClOrdId = null;
        c.stagedQty = 0;
        return m;
    }

    private FixMessage dontKnowTrade(Chain c) {
        String[] reason = pick(DK_REASONS);
        return header(FixTags.MSG_DONT_KNOW_TRADE, false)
                .set(FixTags.ORDER_ID, c.orderId)
                .set(FixTags.EXEC_ID, c.lastFillExecId)
                .set(FixTags.DK_REASON, reason[0])
                .set(FixTags.SYMBOL, c.symbol)
                .set(FixTags.SIDE, c.side)
                .setQty(FixTags.ORDER_QTY, c.orderQty)
                .setQty(FixTags.LAST_SHARES, c.lastShares)
                .setPrice(FixTags.LAST_PX, c.lastPx)
                .set(FixTags.TRANSACT_TIME, timePlaceholder)
                .set(FixTags.TEXT, "DK: " + reason[1]);
    }

    private FixMessage pendingNew(Chain c) {
        return buildExec(c, new ExecSpec()
                .execType(FixTags.EXEC_TYPE_PENDING_NEW)
                .ordStatus(FixTags.ORD_STATUS_PENDING_NEW)
                .clOrdId(c.currentClOrdId)
                .snapshot(0, c.orderQty, 0));
    }

    private FixMessage ack(Chain c) {
        return buildExec(c, new ExecSpec()
                .execType(FixTags.EXEC_TYPE_NEW)
                .ordStatus(FixTags.ORD_STATUS_NEW)
                .clOrdId(c.currentClOrdId)
                .snapshot(0, c.orderQty, 0));
    }

    private FixMessage fill(Chain c, long shares) {
        double px = tradePrice(c);
        c.cumQty += shares;
        c.notional += shares * px;
        c.avgPx = c.notional / c.cumQty;
        c.lastShares = shares;
        c.lastPx = px;
        c.lastMkt = pick(MARKETS);
        boolean complete = c.cumQty >= c.orderQty;
        FixMessage m = buildExec(c, new ExecSpec()
                .execType(complete ? FixTags.EXEC_TYPE_FILL : FixTags.EXEC_TYPE_PARTIAL_FILL)
                .ordStatus(complete ? FixTags.ORD_STATUS_FILLED : FixTags.ORD_STATUS_PARTIALLY_FILLED)
                .clOrdId(c.currentClOrdId)
                .fill(shares, px, c.lastMkt)
                .snapshot(c.cumQty, c.orderQty - c.cumQty, c.avgPx));
        c.lastFillExecId = m.get(FixTags.EXEC_ID);
        return m;
    }

    private FixMessage pendingReplace(Chain c) {
        return buildExec(c, new ExecSpec()
                .execType(FixTags.EXEC_TYPE_PENDING_REPLACE)
                .ordStatus(FixTags.ORD_STATUS_PENDING_REPLACE)
                .clOrdId(c.pendingClOrdId)
                .origClOrdId(c.currentClOrdId)
                .snapshot(c.cumQty, c.orderQty - c.cumQty, c.avgPx));
    }

    /** Applies the staged {@code G} terms and rotates the current {@code ClOrdID}. */
    private FixMessage replaced(Chain c) {
        String priorClOrdId = c.currentClOrdId;
        c.orderQty = c.stagedQty;
        c.price = c.stagedPrice;
        c.currentClOrdId = c.pendingClOrdId;
        c.pendingClOrdId = null;
        c.stagedQty = 0;
        String status = c.cumQty > 0 ? FixTags.ORD_STATUS_PARTIALLY_FILLED : FixTags.ORD_STATUS_NEW;
        return buildExec(c, new ExecSpec()
                .execType(FixTags.EXEC_TYPE_REPLACED)
                .ordStatus(status)
                .clOrdId(c.currentClOrdId)
                .origClOrdId(priorClOrdId)
                .snapshot(c.cumQty, c.orderQty - c.cumQty, c.avgPx));
    }

    private FixMessage pendingCancel(Chain c) {
        return buildExec(c, new ExecSpec()
                .execType(FixTags.EXEC_TYPE_PENDING_CANCEL)
                .ordStatus(FixTags.ORD_STATUS_PENDING_CANCEL)
                .clOrdId(c.pendingClOrdId)
                .origClOrdId(c.currentClOrdId)
                .snapshot(c.cumQty, c.orderQty - c.cumQty, c.avgPx));
    }

    private FixMessage canceled(Chain c) {
        FixMessage m = buildExec(c, new ExecSpec()
                .execType(FixTags.EXEC_TYPE_CANCELED)
                .ordStatus(FixTags.ORD_STATUS_CANCELED)
                .clOrdId(c.pendingClOrdId)
                .origClOrdId(c.currentClOrdId)
                .snapshot(c.cumQty, 0, c.avgPx));
        c.pendingClOrdId = null;
        return m;
    }

    /** Trade bust: {@code 20=1} + {@code 19}, with absolute snapshots restated after removal. */
    private FixMessage bust(Chain c) {
        double bustedShares = c.lastShares;
        double bustedPx = c.lastPx;
        c.cumQty -= (long) bustedShares;
        c.notional -= bustedShares * bustedPx;
        c.avgPx = c.cumQty > 0 ? c.notional / c.cumQty : 0;
        return buildExec(c, new ExecSpec()
                .execTransType(FixTags.EXEC_TRANS_CANCEL)
                .execRefId(c.lastFillExecId)
                .execType(restatedExecType(c))
                .ordStatus(restatedOrdStatus(c))
                .clOrdId(c.currentClOrdId)
                .fill(bustedShares, bustedPx, c.lastMkt)
                .snapshot(c.cumQty, c.orderQty - c.cumQty, c.avgPx)
                .text("trade bust"));
    }

    /** Trade correct: {@code 20=2} + {@code 19}; price is restated, quantity is unchanged. */
    private FixMessage correct(Chain c) {
        double correctedPx = round2(c.lastPx * (1 + (random.nextBoolean() ? 1 : -1) * (0.001 + random.nextDouble() * 0.004)));
        c.notional += c.lastShares * (correctedPx - c.lastPx);
        c.lastPx = correctedPx;
        c.avgPx = c.cumQty > 0 ? c.notional / c.cumQty : 0;
        return buildExec(c, new ExecSpec()
                .execTransType(FixTags.EXEC_TRANS_CORRECT)
                .execRefId(c.lastFillExecId)
                .execType(restatedExecType(c))
                .ordStatus(restatedOrdStatus(c))
                .clOrdId(c.currentClOrdId)
                .fill(c.lastShares, correctedPx, c.lastMkt)
                .snapshot(c.cumQty, c.orderQty - c.cumQty, c.avgPx)
                .text("trade correct"));
    }

    private static String restatedOrdStatus(Chain c) {
        if (c.cumQty <= 0) {
            return FixTags.ORD_STATUS_NEW;
        }
        return c.cumQty >= c.orderQty ? FixTags.ORD_STATUS_FILLED : FixTags.ORD_STATUS_PARTIALLY_FILLED;
    }

    private static String restatedExecType(Chain c) {
        if (c.cumQty <= 0) {
            return FixTags.EXEC_TYPE_NEW;
        }
        return c.cumQty >= c.orderQty ? FixTags.EXEC_TYPE_FILL : FixTags.EXEC_TYPE_PARTIAL_FILL;
    }

    /** Lays down an ExecutionReport body in FIX 4.2 field order. */
    private FixMessage buildExec(Chain c, ExecSpec spec) {
        FixMessage m = header(FixTags.MSG_EXECUTION_REPORT, true)
                .set(FixTags.ORDER_ID, c.orderId)
                .set(FixTags.CL_ORD_ID, spec.clOrdId);
        if (spec.origClOrdId != null) {
            m.set(FixTags.ORIG_CL_ORD_ID, spec.origClOrdId);
        }
        m.set(FixTags.EXEC_ID, nextExecId())
                .set(FixTags.EXEC_TRANS_TYPE, spec.execTransType);
        if (spec.execRefId != null) {
            m.set(FixTags.EXEC_REF_ID, spec.execRefId);
        }
        m.set(FixTags.EXEC_TYPE, spec.execType)
                .set(FixTags.ORD_STATUS, spec.ordStatus);
        if (spec.ordRejReason != null) {
            m.set(FixTags.ORD_REJ_REASON, spec.ordRejReason);
        }
        m.set(FixTags.ACCOUNT, c.account)
                .set(FixTags.SYMBOL, c.symbol)
                .set(FixTags.SIDE, c.side)
                .setQty(FixTags.ORDER_QTY, c.orderQty)
                .set(FixTags.ORD_TYPE, c.ordType);
        if (c.isLimit()) {
            m.setPrice(FixTags.PRICE, c.price);
        }
        m.set(FixTags.TIME_IN_FORCE, c.timeInForce);
        if (spec.lastShares != null) {
            m.setQty(FixTags.LAST_SHARES, spec.lastShares)
                    .setPrice(FixTags.LAST_PX, spec.lastPx)
                    .set(FixTags.LAST_MKT, spec.lastMkt);
        }
        m.setQty(FixTags.LEAVES_QTY, spec.leavesQty)
                .setQty(FixTags.CUM_QTY, spec.cumQty)
                .setPrice(FixTags.AVG_PX, spec.avgPx)
                .set(FixTags.TRANSACT_TIME, timePlaceholder);
        if (spec.text != null) {
            m.set(FixTags.TEXT, spec.text);
        }
        c.ordStatus = spec.ordStatus;
        c.reportedCumQty = spec.cumQty;
        c.reportedLeavesQty = spec.leavesQty;
        return m;
    }

    /** Mutable argument holder for {@link #buildExec}; keeps call sites readable. */
    private static final class ExecSpec {
        private String execTransType = FixTags.EXEC_TRANS_NEW;
        private String execRefId;
        private String execType;
        private String ordStatus;
        private String clOrdId;
        private String origClOrdId;
        private String ordRejReason;
        private Double lastShares;
        private double lastPx;
        private String lastMkt;
        private double cumQty;
        private double leavesQty;
        private double avgPx;
        private String text;

        ExecSpec execTransType(String v) {
            this.execTransType = v;
            return this;
        }

        ExecSpec execRefId(String v) {
            this.execRefId = v;
            return this;
        }

        ExecSpec execType(String v) {
            this.execType = v;
            return this;
        }

        ExecSpec ordStatus(String v) {
            this.ordStatus = v;
            return this;
        }

        ExecSpec clOrdId(String v) {
            this.clOrdId = v;
            return this;
        }

        ExecSpec origClOrdId(String v) {
            this.origClOrdId = v;
            return this;
        }

        ExecSpec ordRejReason(String v) {
            this.ordRejReason = v;
            return this;
        }

        ExecSpec fill(double shares, double px, String mkt) {
            this.lastShares = shares;
            this.lastPx = px;
            this.lastMkt = mkt;
            return this;
        }

        ExecSpec snapshot(double cumQty, double leavesQty, double avgPx) {
            this.cumQty = cumQty;
            this.leavesQty = leavesQty;
            this.avgPx = avgPx;
            return this;
        }

        ExecSpec text(String v) {
            this.text = v;
            return this;
        }
    }

    // ---------------------------------------------------------------- chain state

    /** Venue-side bookkeeping for one order chain while its script is being written. */
    private static final class Chain {
        private final ScenarioCatalog scenario;
        private final String orderId;
        private final String clOrdIdPrefix;
        private final List<OrderScenario.Step> steps = new ArrayList<>();

        private String account;
        private String symbol;
        private String side;
        private String ordType;
        private String timeInForce;
        private long orderQty;
        private double price;
        private double refPx;

        private long cumQty;
        private double notional;
        private double avgPx;
        private double lastShares;
        private double lastPx;
        private String lastMkt = "";
        private String lastFillExecId = "";

        private int clOrdSeq;
        private String currentClOrdId;
        private String pendingClOrdId;
        private long stagedQty;
        private double stagedPrice;

        private String ordStatus = FixTags.ORD_STATUS_PENDING_NEW;
        private double reportedCumQty;
        private double reportedLeavesQty;

        Chain(ScenarioCatalog scenario, int chainNo) {
            this.scenario = scenario;
            this.orderId = "ORD-%04d".formatted(chainNo);
            this.clOrdIdPrefix = "C-%04d-".formatted(chainNo);
            this.currentClOrdId = nextClOrdId();
        }

        String nextClOrdId() {
            return clOrdIdPrefix + (++clOrdSeq);
        }

        boolean isLimit() {
            return "2".equals(ordType);
        }

        void add(FixMessage message, long thinkMillis) {
            steps.add(new OrderScenario.Step(message, thinkMillis));
        }

        ExpectedChainState expected() {
            return new ExpectedChainState(
                    orderId,
                    orderId,
                    scenario.cliName(),
                    FixTags.ordStatusName(ordStatus),
                    reportedCumQty,
                    reportedLeavesQty,
                    currentClOrdId);
        }
    }

    private Chain newChain(ScenarioCatalog scenario) {
        Chain c = new Chain(scenario, ++chainSeq);
        int symbolIdx = random.nextInt(SYMBOLS.length);
        c.symbol = SYMBOLS[symbolIdx];
        c.refPx = round2(REF_PX[symbolIdx] * (1 + (random.nextDouble() - 0.5) * 0.04));
        c.account = pick(ACCOUNTS);
        c.side = pick(SIDES);
        c.ordType = random.nextInt(5) == 0 ? "1" : "2";
        c.timeInForce = pick(TIFS);
        c.orderQty = (2 + random.nextInt(49)) * 100L;
        c.price = c.isLimit() ? round2(c.refPx * (1 + (random.nextDouble() - 0.5) * 0.01)) : 0;
        c.reportedLeavesQty = c.orderQty;
        return c;
    }

    // ---------------------------------------------------------------- helpers

    private String nextExecId() {
        return "EXEC-%06d".formatted(++execSeq);
    }

    private long amendedQty(Chain c) {
        long floor = Math.max(100L, c.cumQty + 100L);
        long delta = (1 + random.nextInt(10)) * 100L;
        long candidate = random.nextBoolean() ? c.orderQty + delta : c.orderQty - delta;
        return Math.max(floor, candidate);
    }

    private double amendedPrice(Chain c) {
        if (!c.isLimit()) {
            return 0;
        }
        return round2(c.price * (1 + (random.nextDouble() - 0.5) * 0.02));
    }

    private double tradePrice(Chain c) {
        double improvement = random.nextDouble() * 0.002;
        if (c.isLimit()) {
            boolean buying = "1".equals(c.side);
            return round2(buying ? c.price * (1 - improvement) : c.price * (1 + improvement));
        }
        return round2(c.refPx * (1 + (random.nextDouble() - 0.5) * 0.006));
    }

    /** Splits a round-lot quantity into {@code parts} round-lot chunks that sum exactly to it. */
    private long[] splitQty(long total, int parts) {
        long lots = Math.max(1, total / 100);
        int n = (int) Math.max(1, Math.min(parts, lots));
        long[] out = new long[n];
        long remaining = lots;
        for (int i = 0; i < n - 1; i++) {
            long max = remaining - (n - 1 - i);
            long take = 1 + (long) (random.nextDouble() * Math.max(1, max / 2.0));
            take = Math.min(take, max);
            out[i] = take * 100;
            remaining -= take;
        }
        out[n - 1] = remaining * 100;
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

    /** Cursor over one chain's steps, used by the round-robin interleaver. */
    private static final class Cursor {
        private final OrderScenario chain;
        private int index;

        Cursor(OrderScenario chain) {
            this.chain = chain;
        }

        OrderScenario.Step next() {
            return chain.steps().get(index++);
        }

        boolean exhausted() {
            return index >= chain.steps().size();
        }
    }
}
