package com.fix42.dashboard.fixcache;

import com.fix42.dashboard.fixcache.FixEnums.CxlRejResponseTo;
import com.fix42.dashboard.fixcache.FixEnums.ExecTransType;
import com.fix42.dashboard.fixcache.FixEnums.ExecType;
import com.fix42.dashboard.fixcache.FixEnums.OrdStatus;
import com.fix42.dashboard.fixcache.FixEnums.OrdType;
import com.fix42.dashboard.fixcache.FixEnums.Side;
import com.fix42.dashboard.fixcache.FixEnums.TimeInForce;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The FIX 4.2 order state machine (docs 01 sections 3/5/6/7, doc 05 section 3).
 *
 * <p>Java port of {@code fix42cache.state_machine.OrderStateMachine}: pure JDK, <b>no Deephaven
 * imports</b> -- the Deephaven layer feeds raw strings in and publishes the returned rows.
 *
 * <p>The machine is a stateful fold over the message stream:
 *
 * <ul>
 *   <li>identity resolution (section 3) keeps one stable {@code OrderKey} per order chain and binds
 *       every identifier seen (37/11/41/17) to it, idempotently;
 *   <li>transitions (section 5) apply venue truth: {@code OrdStatus} always comes from tag 39 on
 *       execution reports and 14/151/6 are adopted as absolute snapshots;
 *   <li>every message yields an audit row, a post-message state snapshot, and 0..n execution/event
 *       rows (section 6).
 * </ul>
 *
 * <p>It is single-threaded by design: Deephaven delivers update-graph rows serially.
 */
public final class OrderStateMachine {

    /** The message types this project handles (doc 01, "In scope"). */
    public static final Set<String> HANDLED_MSG_TYPES =
            Set.of("D", "G", "F", "8", "9", "Q");

    /** Sentinel a venue may send for tag 37 on a {@code 9} whose target was never acked. */
    private static final String ORDER_ID_NONE = "NONE";

    /** 150 ExecType -&gt; EventType (doc 01 section 6); ExecTransType 1/2 override this. */
    private static final Map<ExecType, String> EXEC_TYPE_EVENTS = new EnumMap<>(ExecType.class);

    static {
        EXEC_TYPE_EVENTS.put(ExecType.NEW, EventType.NEW_ACK);
        EXEC_TYPE_EVENTS.put(ExecType.PENDING_NEW, EventType.PENDING_NEW);
        EXEC_TYPE_EVENTS.put(ExecType.PARTIAL_FILL, EventType.PARTIAL_FILL);
        EXEC_TYPE_EVENTS.put(ExecType.FILL, EventType.FULL_FILL);
        EXEC_TYPE_EVENTS.put(ExecType.CANCELED, EventType.CANCEL_ACK);
        EXEC_TYPE_EVENTS.put(ExecType.REPLACED, EventType.AMEND_ACK);
        EXEC_TYPE_EVENTS.put(ExecType.PENDING_CANCEL, EventType.PENDING_CANCEL);
        EXEC_TYPE_EVENTS.put(ExecType.PENDING_REPLACE, EventType.PENDING_AMEND);
        EXEC_TYPE_EVENTS.put(ExecType.REJECTED, EventType.NEW_REJECT);
        EXEC_TYPE_EVENTS.put(ExecType.RESTATED, EventType.RESTATED);
        EXEC_TYPE_EVENTS.put(ExecType.EXPIRED, EventType.EXPIRED);
        EXEC_TYPE_EVENTS.put(ExecType.DONE_FOR_DAY, EventType.DONE_FOR_DAY);
    }

    private final Supplier<Instant> now;
    private final Map<String, Chain> chains = new LinkedHashMap<>();

    /** Identifier -&gt; OrderKey binding tables (doc 01 section 3). */
    private final Map<String, String> keyByOrderId = new HashMap<>();

    private final Map<String, String> keyByClOrdId = new HashMap<>();
    private final Map<String, String> keyByExecId = new HashMap<>();

    /** Creates a machine reading the ingest clock from {@link Clock#systemUTC()}. */
    public OrderStateMachine() {
        this(Instant::now);
    }

    /**
     * Creates a machine with an injectable ingest clock (the analogue of python's {@code now_fn}).
     *
     * @param now called once per processed message for {@code IngestTs}/{@code LastUpdateTs}
     */
    public OrderStateMachine(Supplier<Instant> now) {
        this.now = now;
    }

    // ------------------------------------------------------------------ public API

    /** Parses and applies one raw FIX message. Never throws. */
    public Result process(String raw) {
        Map<Integer, String> fields;
        try {
            fields = FixParser.parseFix(raw);
        } catch (RuntimeException | StackOverflowError exc) { // parseFix is total; defensive
            return Result.error("unparseable: " + exceptionType(exc) + ": " + exc.getMessage());
        }
        if (fields.isEmpty()) {
            return Result.error("unparseable: no FIX fields found");
        }
        return processFields(fields, raw);
    }

    /** Applies one already-parsed message, re-rendering the fields for the audit row. Never throws. */
    public Result processFields(Map<Integer, String> fields) {
        return processFields(fields, null);
    }

    /**
     * Applies one already-parsed message. Never throws.
     *
     * @param fields the parsed message
     * @param raw the original wire string for the audit row; when {@code null} the fields are
     *     re-rendered pipe-delimited
     * @return the resulting rows, or a {@link Result} carrying only an error
     */
    public Result processFields(Map<Integer, String> fields, String raw) {
        try {
            return applyFields(fields, raw);
        } catch (RuntimeException | StackOverflowError exc) {
            // StackOverflowError is caught alongside RuntimeException on purpose. python's analogue
            // is RecursionError, which IS an Exception and so is swallowed by the python original's
            // blanket except -- and an Error escaping here would sail past the Deephaven listener's
            // catch and kill the stream permanently rather than failing one message.
            return Result.error("internal error: " + exceptionType(exc) + ": " + exc.getMessage());
        }
    }

    /** Looks up an order chain by venue OrderID (tag 37). */
    public OrderState getByOrderId(String orderId) {
        return snapshot(keyByOrderId.get(orderId));
    }

    /** Looks up an order chain by any ClOrdID it has ever carried (11/41). */
    public OrderState getByClOrdId(String clOrdId) {
        return snapshot(keyByClOrdId.get(clOrdId));
    }

    /** Looks up an order chain by any ExecID bound to it (tag 17). */
    public OrderState getByExecId(String execId) {
        return snapshot(keyByExecId.get(execId));
    }

    /** Looks up an order chain by its OrderKey. */
    public OrderState getByKey(String orderKey) {
        return snapshot(orderKey);
    }

    /** All order chains for an account (tag 1), in chain-creation order. */
    public List<OrderState> findByAccount(String account) {
        List<OrderState> found = new ArrayList<>();
        for (Chain chain : chains.values()) {
            if (chain.state.account.equals(account)) {
                found.add(chain.state.copy());
            }
        }
        return found;
    }

    /** All order chains for a symbol (tag 55), in chain-creation order. */
    public List<OrderState> findBySymbol(String symbol) {
        List<OrderState> found = new ArrayList<>();
        for (Chain chain : chains.values()) {
            if (chain.state.symbol.equals(symbol)) {
                found.add(chain.state.copy());
            }
        }
        return found;
    }

    /** Number of distinct order chains held. */
    public int orderCount() {
        return chains.size();
    }

    /** Snapshots of every order chain held (test/debug convenience). */
    public List<OrderState> snapshotAll() {
        List<OrderState> all = new ArrayList<>(chains.size());
        for (Chain chain : chains.values()) {
            all.add(chain.state.copy());
        }
        return all;
    }

    /** Live view of the ClOrdID -&gt; OrderKey binding table (tests only). */
    public Map<String, String> keyByClOrdId() {
        return Collections.unmodifiableMap(keyByClOrdId);
    }

    /** Live view of the OrderID -&gt; OrderKey binding table (tests only). */
    public Map<String, String> keyByOrderId() {
        return Collections.unmodifiableMap(keyByOrderId);
    }

    /** Live view of the ExecID -&gt; OrderKey binding table (tests only). */
    public Map<String, String> keyByExecId() {
        return Collections.unmodifiableMap(keyByExecId);
    }

    // ------------------------------------------------------------------ dispatch

    private Result applyFields(Map<Integer, String> f, String raw) {
        Instant ingestTs = now.get();
        String rawText = raw != null ? raw : FixParser.renderFields(f);
        String msgType = text(f, FixTags.MSG_TYPE);

        if (!HANDLED_MSG_TYPES.contains(msgType)) {
            // Audit it, attribute it if we can, but change nothing.
            String key = resolve(f, false);
            String error = msgType.isEmpty()
                    ? "missing MsgType (tag 35)"
                    : "unsupported MsgType: " + PyRepr.of(msgType);
            return Result.error(MessageRow.fromFields(f, key, rawText, ingestTs), error);
        }

        String key = resolve(f, true);
        if (key.isEmpty()) {
            return Result.error(
                    MessageRow.fromFields(f, "", rawText, ingestTs),
                    "unresolvable: no OrderID/ClOrdID/OrigClOrdID binding");
        }

        bind(key, f);
        Chain chain = chains.get(key);
        boolean created = chain == null;
        if (chain == null) {
            chain = new Chain(new OrderState(key, ingestTs));
            chains.put(key, chain);
        }

        OrderState state = chain.state;
        state.msgCount += 1;
        state.lastMsgType = msgType;
        state.lastUpdateTs = ingestTs;
        if (state.firstSeenTs == null) {
            state.firstSeenTs = ingestTs;
        }

        String orderId = orderIdOf(f);
        if (!orderId.isEmpty()) {
            state.orderId = orderId;
        }
        String origClOrdId = text(f, FixTags.ORIG_CL_ORD_ID);
        if (!origClOrdId.isEmpty()) {
            state.origClOrdId = origClOrdId;
        }

        Emitted emitted = switch (msgType) {
            case "D" -> handleNewOrder(f, chain, created, ingestTs);
            case "G" -> handleReplaceRequest(f, chain, created, ingestTs);
            case "F" -> handleCancelRequest(f, chain, created, ingestTs);
            case "8" -> handleExecutionReport(f, chain, created, ingestTs);
            case "9" -> handleCancelReject(f, chain, ingestTs);
            default -> handleDontKnowTrade(f, chain, ingestTs); // "Q"
        };

        return new Result(
                state.copy(),
                emitted.executions,
                emitted.events,
                MessageRow.fromFields(f, key, rawText, ingestTs),
                null);
    }

    // ------------------------------------------------------------------ identity resolution

    /**
     * Resolves the OrderKey for a message.
     *
     * <p>Priority: 37 OrderID, then 11 ClOrdID, then 41 OrigClOrdID, then (for {@code Q}, doc 01
     * section 5.6) 17 ExecID. When nothing is bound yet a new chain is keyed by OrderID if present,
     * else ClOrdID, else OrigClOrdID -- an ExecID is never itself an OrderKey.
     *
     * @return the key, or {@code ""} when the message carries no usable identifier
     */
    private String resolve(Map<Integer, String> f, boolean allowCreate) {
        String orderId = orderIdOf(f);
        String clOrdId = text(f, FixTags.CL_ORD_ID);
        String origClOrdId = text(f, FixTags.ORIG_CL_ORD_ID);
        String execId = text(f, FixTags.EXEC_ID);

        if (!orderId.isEmpty() && keyByOrderId.containsKey(orderId)) {
            return keyByOrderId.get(orderId);
        }
        if (!clOrdId.isEmpty() && keyByClOrdId.containsKey(clOrdId)) {
            return keyByClOrdId.get(clOrdId);
        }
        if (!origClOrdId.isEmpty() && keyByClOrdId.containsKey(origClOrdId)) {
            return keyByClOrdId.get(origClOrdId);
        }
        if (!execId.isEmpty() && keyByExecId.containsKey(execId)) {
            return keyByExecId.get(execId);
        }

        if (!allowCreate) {
            return "";
        }
        if (!orderId.isEmpty()) {
            return orderId;
        }
        if (!clOrdId.isEmpty()) {
            return clOrdId;
        }
        return origClOrdId;
    }

    /** Binds every identifier present to {@code key} (idempotent; first binding wins). */
    private void bind(String key, Map<Integer, String> f) {
        String orderId = orderIdOf(f);
        if (!orderId.isEmpty()) {
            keyByOrderId.putIfAbsent(orderId, key);
        }
        for (String clOrdId : List.of(text(f, FixTags.CL_ORD_ID), text(f, FixTags.ORIG_CL_ORD_ID))) {
            if (!clOrdId.isEmpty()) {
                keyByClOrdId.putIfAbsent(clOrdId, key);
            }
        }
        String execId = text(f, FixTags.EXEC_ID);
        if (!execId.isEmpty()) {
            keyByExecId.putIfAbsent(execId, key);
        }
    }

    private OrderState snapshot(String key) {
        Chain chain = (key == null || key.isEmpty()) ? null : chains.get(key);
        return chain != null ? chain.state.copy() : null;
    }

    // ------------------------------------------------------------------ shared state helpers

    /**
     * Copies order terms from a message onto the state.
     *
     * <p>With {@code onlyIfEmpty} set, existing values are preserved -- used when a late {@code D}
     * merges into a chain the venue already created (edge case 9).
     */
    private void applyTerms(OrderState state, Map<Integer, String> f, boolean onlyIfEmpty) {
        String account = text(f, FixTags.ACCOUNT);
        if (!account.isEmpty() && (!onlyIfEmpty || state.account.isEmpty())) {
            state.account = account;
        }
        String symbol = text(f, FixTags.SYMBOL);
        if (!symbol.isEmpty() && (!onlyIfEmpty || state.symbol.isEmpty())) {
            state.symbol = symbol;
        }
        Side side = enumOf(f, FixTags.SIDE, Side::fromFix);
        if (side != null && (!onlyIfEmpty || state.side == null)) {
            state.side = side;
        }
        OrdType ordType = enumOf(f, FixTags.ORD_TYPE, OrdType::fromFix);
        if (ordType != null && (!onlyIfEmpty || state.ordType == null)) {
            state.ordType = ordType;
        }
        TimeInForce tif = enumOf(f, FixTags.TIME_IN_FORCE, TimeInForce::fromFix);
        if (tif != null && (!onlyIfEmpty || state.timeInForce == null)) {
            state.timeInForce = tif;
        }
        Double orderQty = number(f, FixTags.ORDER_QTY);
        if (orderQty != null && (!onlyIfEmpty || state.orderQty == 0.0)) {
            state.orderQty = orderQty;
        }
        Double price = number(f, FixTags.PRICE);
        if (price != null && (!onlyIfEmpty || state.price == 0.0)) {
            state.price = price;
        }
        Double stopPx = number(f, FixTags.STOP_PX);
        if (stopPx != null && (!onlyIfEmpty || state.stopPx == 0.0)) {
            state.stopPx = stopPx;
        }
    }

    /**
     * Records an identity-carrying ClOrdID in the chain history.
     *
     * <p>Only the original {@code D} id and ids adopted by a {@code 150=5} rotation belong here --
     * cancel-request ids never become the order's identity.
     */
    private void noteClOrdId(OrderState state, String clOrdId) {
        if (!clOrdId.isEmpty() && !state.clOrdIdChain.contains(clOrdId)) {
            state.clOrdIdChain.add(clOrdId);
        }
    }

    /** Derives {@code PendingAction}/{@code PendingClOrdID} from in-flight requests. */
    private void recomputePending(Chain chain) {
        OrderState state = chain.state;
        if (!chain.pending.isEmpty()) {
            PendingRequest latest = chain.pending.get(chain.pending.size() - 1);
            state.pendingAction = latest.action;
            state.pendingClOrdId = latest.clOrdId;
        } else if (chain.newPending) {
            state.pendingAction = PendingAction.NEW;
            state.pendingClOrdId = "";
        } else {
            state.pendingAction = PendingAction.NONE;
            state.pendingClOrdId = "";
        }
    }

    /** Removes one in-flight request, preferring an exact ClOrdID match (newest first). */
    private PendingRequest clearPending(Chain chain, String action, String clOrdId) {
        if (!clOrdId.isEmpty()) {
            for (int i = chain.pending.size() - 1; i >= 0; i--) {
                PendingRequest request = chain.pending.get(i);
                if (request.action.equals(action) && request.clOrdId.equals(clOrdId)) {
                    return chain.pending.remove(i);
                }
            }
        }
        for (int i = chain.pending.size() - 1; i >= 0; i--) {
            if (chain.pending.get(i).action.equals(action)) {
                return chain.pending.remove(i);
            }
        }
        return null;
    }

    /** Snapshots the prior status and records an in-flight {@code F}/{@code G}. */
    private void registerRequest(Chain chain, String action, String clOrdId, StagedTerms terms) {
        chain.priorStatus.put(clOrdId, chain.state.ordStatus);
        chain.pending.add(new PendingRequest(action, clOrdId, terms));
    }

    // ------------------------------------------------------------------ 35=D NewOrderSingle

    /** Rule 1; edge case 9. */
    private Emitted handleNewOrder(
            Map<Integer, String> f, Chain chain, boolean created, Instant ingestTs) {
        OrderState state = chain.state;
        String clOrdId = text(f, FixTags.CL_ORD_ID);

        if (created) {
            state.clOrdId = clOrdId;
            state.rootClOrdId = clOrdId;
            noteClOrdId(state, clOrdId);
            applyTerms(state, f, false);
            state.ordStatus = OrdStatus.PENDING_NEW;
            state.cumQty = 0.0;
            state.leavesQty = state.orderQty;
            state.avgPx = 0.0;
            chain.newPending = true;
        } else {
            // Late `D` for a chain the venue already opened: fill gaps only, never clobber venue
            // state (edge case 9).
            if (state.clOrdId.isEmpty()) {
                state.clOrdId = clOrdId;
            }
            if (state.rootClOrdId.isEmpty()) {
                state.rootClOrdId = clOrdId;
            }
            noteClOrdId(state, clOrdId);
            applyTerms(state, f, true);
        }
        recomputePending(chain);

        // python's `x or y` treats 0.0 as falsy, so a literal 38=0 falls back to the state value.
        String detail = "new order request: "
                + Rows.name(state.side)
                + " "
                + PyNum.num(orZero(number(f, FixTags.ORDER_QTY), state.orderQty))
                + " "
                + state.symbol
                + " "
                + Rows.name(state.ordType)
                + priceSuffix(orZero(number(f, FixTags.PRICE), state.price));

        OrderEventRow event = buildEvent(
                chain,
                f,
                EventType.NEW_REQUEST,
                "D",
                number(f, FixTags.ORDER_QTY),
                number(f, FixTags.PRICE),
                detail,
                ingestTs,
                clOrdId.isEmpty() ? state.clOrdId : clOrdId);
        return Emitted.of(List.of(), List.of(event));
    }

    // ------------------------------------------------------------------ 35=G OrderCancelReplaceRequest

    /** Rule 3. */
    private Emitted handleReplaceRequest(
            Map<Integer, String> f, Chain chain, boolean created, Instant ingestTs) {
        OrderState state = chain.state;
        String clOrdId = text(f, FixTags.CL_ORD_ID);
        String origClOrdId = text(f, FixTags.ORIG_CL_ORD_ID);

        if (created) {
            // Mid-stream start: the order we are amending was never seen.
            state.clOrdId = origClOrdId.isEmpty() ? clOrdId : origClOrdId;
            state.rootClOrdId = state.clOrdId;
            noteClOrdId(state, state.clOrdId);
            applyTerms(state, f, false);
        }

        StagedTerms terms = new StagedTerms(
                number(f, FixTags.ORDER_QTY),
                number(f, FixTags.PRICE),
                number(f, FixTags.STOP_PX),
                enumOf(f, FixTags.TIME_IN_FORCE, TimeInForce::fromFix));
        registerRequest(chain, PendingAction.REPLACE, clOrdId, terms);
        state.ordStatus = OrdStatus.PENDING_REPLACE;
        recomputePending(chain);

        double proposedQty = terms.orderQty != null ? terms.orderQty : state.orderQty;
        double proposedPrice = terms.price != null ? terms.price : state.price;
        String detail = "amend request "
                + (origClOrdId.isEmpty() ? state.clOrdId : origClOrdId)
                + " -> "
                + clOrdId
                + ": qty "
                + PyNum.num(state.orderQty)
                + " -> "
                + PyNum.num(proposedQty)
                + ", price "
                + PyNum.num(state.price)
                + " -> "
                + PyNum.num(proposedPrice);

        OrderEventRow event = buildEvent(
                chain,
                f,
                EventType.AMEND_REQUEST,
                "G",
                proposedQty,
                proposedPrice,
                detail,
                ingestTs,
                clOrdId);
        return Emitted.of(List.of(), List.of(event));
    }

    // ------------------------------------------------------------------ 35=F OrderCancelRequest

    /** Rule 4. */
    private Emitted handleCancelRequest(
            Map<Integer, String> f, Chain chain, boolean created, Instant ingestTs) {
        OrderState state = chain.state;
        String clOrdId = text(f, FixTags.CL_ORD_ID);
        String origClOrdId = text(f, FixTags.ORIG_CL_ORD_ID);

        if (created) {
            state.clOrdId = origClOrdId.isEmpty() ? clOrdId : origClOrdId;
            state.rootClOrdId = state.clOrdId;
            noteClOrdId(state, state.clOrdId);
            applyTerms(state, f, false);
        }

        registerRequest(chain, PendingAction.CANCEL, clOrdId, null);
        state.ordStatus = OrdStatus.PENDING_CANCEL;
        recomputePending(chain);

        String detail = "cancel request "
                + (origClOrdId.isEmpty() ? state.clOrdId : origClOrdId)
                + " -> "
                + clOrdId
                + ": leaves "
                + PyNum.num(state.leavesQty);

        OrderEventRow event = buildEvent(
                chain,
                f,
                EventType.CANCEL_REQUEST,
                "F",
                number(f, FixTags.ORDER_QTY),
                number(f, FixTags.PRICE),
                detail,
                ingestTs,
                clOrdId);
        return Emitted.of(List.of(), List.of(event));
    }

    // ------------------------------------------------------------------ 35=8 ExecutionReport

    /** Rule 2; edge cases 3, 6, 7, 9, 10, 11, 12. */
    private Emitted handleExecutionReport(
            Map<Integer, String> f, Chain chain, boolean created, Instant ingestTs) {
        OrderState state = chain.state;
        String execId = text(f, FixTags.EXEC_ID);
        String execRefId = text(f, FixTags.EXEC_REF_ID);
        ExecTransType transType = enumOf(f, FixTags.EXEC_TRANS_TYPE, ExecTransType::fromFix);
        ExecType execType = enumOf(f, FixTags.EXEC_TYPE, ExecType::fromFix);
        OrdStatus status = enumOf(f, FixTags.ORD_STATUS, OrdStatus::fromFix);

        boolean isBust = transType == ExecTransType.CANCEL;
        boolean isCorrect = transType == ExecTransType.CORRECT;
        boolean isNewTrans = transType == null || transType == ExecTransType.NEW;

        Double cumQty = number(f, FixTags.CUM_QTY);
        boolean duplicate = !execId.isEmpty() && chain.execIds.contains(execId);
        // Stale guard (rule 2, edge case 12): a non-bust/correct report whose CumQty went backwards
        // carries no economic truth.
        boolean stale = !duplicate && !isBust && !isCorrect && cumQty != null && cumQty < state.cumQty;

        if (created) {
            // Audit feeds can start mid-stream: seed identity from the report.
            String clOrdId = text(f, FixTags.CL_ORD_ID);
            state.clOrdId = clOrdId;
            state.rootClOrdId = clOrdId;
            noteClOrdId(state, clOrdId);
        }
        // Venue reports echo the order terms: fill gaps only, never clobber the client's terms
        // (staged `G` terms are applied by the 150=5 branch).
        applyTerms(state, f, true);

        if (duplicate) {
            // Replay guard (edge case 3): bind + count only, no economics and no new lifecycle
            // event; re-emit the stored row so the executions table keeps this ExecID's current
            // disposition.
            ExecutionRow stored = chain.execRows.get(execId);
            ExecutionRow row;
            if (stored != null) {
                row = stored.copy();
                row.ingestTs = ingestTs;
            } else {
                row = buildExecRow(chain, f, execId, execRefId, transType, execType, status, isNewTrans, ingestTs);
            }
            return Emitted.of(List.of(row), List.of());
        }

        if (!execId.isEmpty()) {
            chain.execIds.add(execId);
            state.execCount = chain.execIds.size();
        }

        if (status != null) {
            state.ordStatus = status;
        }
        if (execType != null) {
            state.lastExecType = execType;
        }
        if (f.containsKey(FixTags.TEXT)) {
            state.text = f.get(FixTags.TEXT);
        }
        if (f.containsKey(FixTags.ORD_REJ_REASON)) {
            state.ordRejReason = f.get(FixTags.ORD_REJ_REASON);
        }

        if (!stale) {
            Double leavesQty = number(f, FixTags.LEAVES_QTY);
            Double avgPx = number(f, FixTags.AVG_PX);
            if (cumQty != null) {
                state.cumQty = cumQty;
            }
            if (leavesQty != null) {
                state.leavesQty = leavesQty;
            }
            if (avgPx != null) {
                state.avgPx = avgPx;
            }
            if (isNewTrans && (execType == ExecType.PARTIAL_FILL || execType == ExecType.FILL)) {
                Double lastShares = number(f, FixTags.LAST_SHARES);
                Double lastPx = number(f, FixTags.LAST_PX);
                String lastMkt = text(f, FixTags.LAST_MKT);
                if (lastShares != null) {
                    state.lastShares = lastShares;
                }
                if (lastPx != null) {
                    state.lastPx = lastPx;
                }
                if (!lastMkt.isEmpty()) {
                    state.lastMkt = lastMkt;
                }
            }
            resolvePendingFromExec(chain, f, execType, status);
        }

        ExecutionRow primary =
                buildExecRow(chain, f, execId, execRefId, transType, execType, status, isNewTrans, ingestTs);
        if (!execId.isEmpty()) {
            chain.execRows.put(execId, primary);
        }
        List<ExecutionRow> executions = new ArrayList<>();
        executions.add(primary);

        if ((isBust || isCorrect) && !execRefId.isEmpty()) {
            executions.add(reemitReference(
                    chain,
                    execRefId,
                    isBust ? FillStatus.BUSTED : FillStatus.CORRECTED,
                    ingestTs,
                    FixParser.parseTransactTime(f.get(FixTags.TRANSACT_TIME)),
                    isCorrect ? f : null));
        }

        OrderEventRow event = execEvent(chain, f, transType, execType, ingestTs);
        return Emitted.of(executions, List.of(event));
    }

    /** Clears/applies pending requests that this report resolves (rule 2). */
    private void resolvePendingFromExec(
            Chain chain, Map<Integer, String> f, ExecType execType, OrdStatus status) {
        OrderState state = chain.state;
        String clOrdId = text(f, FixTags.CL_ORD_ID);

        // Any venue response other than PendingNew resolves the local `D` wait.
        if (execType != ExecType.PENDING_NEW) {
            chain.newPending = false;
        }

        if (execType == ExecType.CANCELED || status == OrdStatus.CANCELED) {
            clearPending(chain, PendingAction.CANCEL, clOrdId);
        }

        if (execType == ExecType.REPLACED) {
            PendingRequest request = clearPending(chain, PendingAction.REPLACE, clOrdId);
            if (request != null && request.terms != null) {
                StagedTerms terms = request.terms;
                if (terms.orderQty != null) {
                    state.orderQty = terms.orderQty;
                }
                if (terms.price != null) {
                    state.price = terms.price;
                }
                if (terms.stopPx != null) {
                    state.stopPx = terms.stopPx;
                }
                if (terms.timeInForce != null) {
                    state.timeInForce = terms.timeInForce;
                }
            }
            String newClOrdId = !clOrdId.isEmpty() ? clOrdId : (request != null ? request.clOrdId : "");
            if (!newClOrdId.isEmpty()) {
                state.clOrdId = newClOrdId;
                noteClOrdId(state, newClOrdId);
            }
        }

        recomputePending(chain);
    }

    /** Builds the executions row for the report itself. */
    private ExecutionRow buildExecRow(
            Chain chain,
            Map<Integer, String> f,
            String execId,
            String execRefId,
            ExecTransType transType,
            ExecType execType,
            OrdStatus status,
            boolean isNewTrans,
            Instant ingestTs) {
        OrderState state = chain.state;
        Double cumQty = number(f, FixTags.CUM_QTY);
        Double leavesQty = number(f, FixTags.LEAVES_QTY);
        Double avgPx = number(f, FixTags.AVG_PX);
        Double lastShares = number(f, FixTags.LAST_SHARES);
        Double lastPx = number(f, FixTags.LAST_PX);
        String clOrdId = text(f, FixTags.CL_ORD_ID);

        ExecutionRow row = new ExecutionRow(state.orderKey);
        row.orderId = state.orderId;
        row.clOrdId = clOrdId.isEmpty() ? state.clOrdId : clOrdId;
        row.execId = execId;
        row.execRefId = execRefId;
        row.execTransType = transType;
        row.execType = execType;
        row.ordStatus = status != null ? status : state.ordStatus;
        row.lastShares = lastShares != null ? lastShares : 0.0;
        row.lastPx = lastPx != null ? lastPx : 0.0;
        row.cumQty = cumQty != null ? cumQty : state.cumQty;
        row.leavesQty = leavesQty != null ? leavesQty : state.leavesQty;
        row.avgPx = avgPx != null ? avgPx : state.avgPx;
        row.lastMkt = text(f, FixTags.LAST_MKT);
        row.text = text(f, FixTags.TEXT);
        row.isFill = isNewTrans && (execType == ExecType.PARTIAL_FILL || execType == ExecType.FILL);
        row.fillStatus = FillStatus.NORMAL;
        row.transactTime = FixParser.parseTransactTime(f.get(FixTags.TRANSACT_TIME));
        row.ingestTs = ingestTs;
        return row;
    }

    /**
     * Re-emits a referenced execution with its new disposition.
     *
     * <p>Same {@code ExecID}, updated {@code FillStatus} and current order values, so a downstream
     * {@code last_by(ExecID)} shows the execution's current truth. A reference to an ExecID this
     * machine never saw (mid-stream start) is synthesised so the disposition is still visible.
     */
    private ExecutionRow reemitReference(
            Chain chain,
            String refExecId,
            String disposition,
            Instant ingestTs,
            Instant transactTime,
            Map<Integer, String> correctedFrom) {
        OrderState state = chain.state;
        ExecutionRow stored = chain.execRows.get(refExecId);
        if (stored == null) {
            stored = new ExecutionRow(state.orderKey);
            stored.orderId = state.orderId;
            stored.clOrdId = state.clOrdId;
            stored.execId = refExecId;
        }
        ExecutionRow row = stored.copy();
        row.orderKey = state.orderKey;
        row.orderId = state.orderId;
        row.clOrdId = state.clOrdId;
        row.ordStatus = state.ordStatus;
        row.cumQty = state.cumQty;
        row.leavesQty = state.leavesQty;
        row.avgPx = state.avgPx;
        row.fillStatus = disposition;
        row.transactTime = transactTime != null ? transactTime : stored.transactTime;
        row.ingestTs = ingestTs;

        if (correctedFrom != null) {
            // The correcting report's 32/31/30 become the execution's values.
            Double lastShares = number(correctedFrom, FixTags.LAST_SHARES);
            Double lastPx = number(correctedFrom, FixTags.LAST_PX);
            String lastMkt = text(correctedFrom, FixTags.LAST_MKT);
            String correctedText = text(correctedFrom, FixTags.TEXT);
            if (lastShares != null) {
                row.lastShares = lastShares;
            }
            if (lastPx != null) {
                row.lastPx = lastPx;
            }
            if (!lastMkt.isEmpty()) {
                row.lastMkt = lastMkt;
            }
            if (!correctedText.isEmpty()) {
                row.text = correctedText;
            }
        }
        chain.execRows.put(refExecId, row);
        return row;
    }

    /** Derives the lifecycle event for an execution report (doc 01 section 6). */
    private OrderEventRow execEvent(
            Chain chain,
            Map<Integer, String> f,
            ExecTransType transType,
            ExecType execType,
            Instant ingestTs) {
        OrderState state = chain.state;
        String execId = text(f, FixTags.EXEC_ID);
        String execRefId = text(f, FixTags.EXEC_REF_ID);
        Double lastShares = number(f, FixTags.LAST_SHARES);
        Double lastPx = number(f, FixTags.LAST_PX);

        String eventType;
        String detail;
        if (transType == ExecTransType.CANCEL) {
            eventType = EventType.FILL_BUST;
            detail = "bust of "
                    + (execRefId.isEmpty() ? "?" : execRefId)
                    + ": restated cum "
                    + PyNum.num(state.cumQty)
                    + ", leaves "
                    + PyNum.num(state.leavesQty)
                    + ", avg "
                    + PyNum.num(state.avgPx);
        } else if (transType == ExecTransType.CORRECT) {
            eventType = EventType.FILL_CORRECT;
            detail = "correct of "
                    + (execRefId.isEmpty() ? "?" : execRefId)
                    + ": "
                    + PyNum.num(orZero(lastShares))
                    + " @ "
                    + PyNum.num(orZero(lastPx))
                    + ", restated cum "
                    + PyNum.num(state.cumQty)
                    + ", avg "
                    + PyNum.num(state.avgPx);
        } else {
            eventType = execType != null
                    ? EXEC_TYPE_EVENTS.getOrDefault(execType, EventType.STATUS)
                    : EventType.STATUS;
            if (EventType.PARTIAL_FILL.equals(eventType) || EventType.FULL_FILL.equals(eventType)) {
                detail = "fill "
                        + PyNum.num(orZero(lastShares))
                        + " @ "
                        + PyNum.num(orZero(lastPx))
                        + " (cum "
                        + PyNum.num(state.cumQty)
                        + ", leaves "
                        + PyNum.num(state.leavesQty)
                        + ", avg "
                        + PyNum.num(state.avgPx)
                        + ")";
            } else if (EventType.NEW_REJECT.equals(eventType)) {
                String reason = text(f, FixTags.TEXT);
                detail = "reject: "
                        + (reason.isEmpty() ? "order rejected" : reason)
                        + " (103="
                        + text(f, FixTags.ORD_REJ_REASON)
                        + ")";
            } else if (EventType.AMEND_ACK.equals(eventType)) {
                detail = "amend confirmed: qty "
                        + PyNum.num(state.orderQty)
                        + ", price "
                        + PyNum.num(state.price)
                        + ", ClOrdID "
                        + state.clOrdId;
            } else if (EventType.CANCEL_ACK.equals(eventType)) {
                detail = "cancel confirmed: cum "
                        + PyNum.num(state.cumQty)
                        + ", leaves "
                        + PyNum.num(state.leavesQty);
            } else if (EventType.NEW_ACK.equals(eventType)) {
                detail = "ack: OrderID " + (state.orderId.isEmpty() ? "?" : state.orderId);
            } else {
                detail = "exec report "
                        + (execId.isEmpty() ? "?" : execId)
                        + ": ExecType="
                        + Rows.name(execType)
                        + ", OrdStatus="
                        + Rows.name(state.ordStatus);
            }
        }

        String clOrdId = text(f, FixTags.CL_ORD_ID);
        return buildEvent(
                chain,
                f,
                eventType,
                "8",
                state.orderQty,
                state.price,
                detail,
                ingestTs,
                clOrdId.isEmpty() ? state.clOrdId : clOrdId);
    }

    // ------------------------------------------------------------------ 35=9 OrderCancelReject

    /** Rule 5; edge case 4. */
    private Emitted handleCancelReject(Map<Integer, String> f, Chain chain, Instant ingestTs) {
        OrderState state = chain.state;
        String clOrdId = text(f, FixTags.CL_ORD_ID);
        CxlRejResponseTo responseTo =
                enumOf(f, FixTags.CXL_REJ_RESPONSE_TO, CxlRejResponseTo::fromFix);

        String action;
        if (responseTo == CxlRejResponseTo.ORDER_CANCEL_REQUEST) {
            action = PendingAction.CANCEL;
        } else if (responseTo == CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST) {
            action = PendingAction.REPLACE;
        } else {
            // 434 absent/unknown: fall back to whichever request this ClOrdID was.
            action = null;
            for (int i = chain.pending.size() - 1; i >= 0; i--) {
                if (chain.pending.get(i).clOrdId.equals(clOrdId)) {
                    action = chain.pending.get(i).action;
                    break;
                }
            }
        }

        if (action != null) {
            // Removing the request also discards its staged terms.
            clearPending(chain, action, clOrdId);
        }

        // Revert to the per-request snapshot; a venue-supplied 39 wins over it.
        OrdStatus venueStatus = enumOf(f, FixTags.ORD_STATUS, OrdStatus::fromFix);
        if (venueStatus != null) {
            state.ordStatus = venueStatus;
        } else if (chain.priorStatus.containsKey(clOrdId)) {
            state.ordStatus = chain.priorStatus.get(clOrdId);
        }

        if (f.containsKey(FixTags.CXL_REJ_REASON)) {
            state.cxlRejReason = f.get(FixTags.CXL_REJ_REASON);
        }
        if (f.containsKey(FixTags.TEXT)) {
            state.text = f.get(FixTags.TEXT);
        }
        recomputePending(chain);

        String eventType = PendingAction.REPLACE.equals(action)
                ? EventType.AMEND_REJECT
                : EventType.CANCEL_REJECT;
        String reason = text(f, FixTags.TEXT);
        String detail = "reject: "
                + (reason.isEmpty() ? "request rejected" : reason)
                + " (102="
                + text(f, FixTags.CXL_REJ_REASON)
                + ")";

        OrderEventRow event = buildEvent(
                chain, f, eventType, "9", state.orderQty, state.price, detail, ingestTs, clOrdId);
        return Emitted.of(List.of(), List.of(event));
    }

    // ------------------------------------------------------------------ 35=Q DontKnowTrade

    /** Rule 6; edge case 8. */
    private Emitted handleDontKnowTrade(Map<Integer, String> f, Chain chain, Instant ingestTs) {
        OrderState state = chain.state;
        String execId = text(f, FixTags.EXEC_ID);
        if (f.containsKey(FixTags.DK_REASON)) {
            state.dkReason = f.get(FixTags.DK_REASON);
        }
        if (f.containsKey(FixTags.TEXT)) {
            state.text = f.get(FixTags.TEXT);
        }

        // No economic change; the referenced execution is re-emitted as DK'd.
        ExecutionRow row = reemitReference(
                chain,
                execId,
                FillStatus.DK,
                ingestTs,
                FixParser.parseTransactTime(f.get(FixTags.TRANSACT_TIME)),
                null);
        String detail = "DK trade "
                + (execId.isEmpty() ? "?" : execId)
                + " (127="
                + text(f, FixTags.DK_REASON)
                + ")";
        OrderEventRow event = buildEvent(
                chain,
                f,
                EventType.DK_TRADE,
                "Q",
                state.orderQty,
                state.price,
                detail,
                ingestTs,
                state.clOrdId);
        return Emitted.of(List.of(row), List.of(event));
    }

    // ------------------------------------------------------------------ events

    private OrderEventRow buildEvent(
            Chain chain,
            Map<Integer, String> f,
            String eventType,
            String msgType,
            Double orderQty,
            Double price,
            String detail,
            Instant ingestTs,
            String clOrdId) {
        OrderState state = chain.state;
        OrderEventRow event = new OrderEventRow(state.orderKey);
        event.clOrdId = clOrdId;
        event.origClOrdId = text(f, FixTags.ORIG_CL_ORD_ID);
        event.orderId = state.orderId;
        event.eventType = eventType;
        event.msgType = msgType;
        event.ordStatus = state.ordStatus;
        event.orderQty = orderQty != null ? orderQty : state.orderQty;
        event.price = price != null ? price : state.price;
        event.detail = detail;
        event.transactTime = FixParser.parseTransactTime(f.get(FixTags.TRANSACT_TIME));
        event.ingestTs = ingestTs;
        return event;
    }

    // ------------------------------------------------------------------ field helpers

    /** Reads a string tag, defaulting to {@code ""}. */
    private static String text(Map<Integer, String> fields, int tag) {
        String value = fields.get(tag);
        return value != null ? value : "";
    }

    /** Reads a numeric tag; {@code null} when absent or unparseable. */
    private static Double number(Map<Integer, String> fields, int tag) {
        String raw = fields.get(tag);
        if (raw == null) {
            return null;
        }
        try {
            return Double.valueOf(PyFloat.parse(PyDigits.strip(raw)));
        } catch (NumberFormatException notANumber) {
            return null;
        }
    }

    /** Maps a tag through its enum, or {@code null} when the tag is absent. */
    private static <E extends Enum<E>> E enumOf(
            Map<Integer, String> fields, int tag, java.util.function.Function<String, E> mapper) {
        String raw = fields.get(tag);
        return raw == null ? null : mapper.apply(raw);
    }

    /** Tag 37, treating the {@code NONE} sentinel (doc 01 section 2) as absent. */
    private static String orderIdOf(Map<Integer, String> fields) {
        String value = text(fields, FixTags.ORDER_ID);
        return ORDER_ID_NONE.equals(value) ? "" : value;
    }

    /**
     * python's {@code type(exc).__name__} for the {@code internal error:} prefix.
     *
     * <p>A {@link PyException} carries the python type it stands in for; anything else is a genuine
     * Java failure and reports its own class name.
     */
    private static String exceptionType(Throwable exc) {
        return exc instanceof PyException py ? py.pyType() : exc.getClass().getSimpleName();
    }

    /** python's {@code value or 0.0}. */
    private static double orZero(Double value) {
        return value != null ? value : 0.0;
    }

    /** python's {@code candidate or fallback} for floats: {@code None} <em>and</em> 0.0 fall back. */
    private static double orZero(Double candidate, double fallback) {
        return (candidate != null && candidate != 0.0) ? candidate : fallback;
    }

    /** The {@code " @ <price>"} tail of a NEW_REQUEST detail, empty when the price is 0/absent. */
    private static String priceSuffix(double price) {
        return price != 0.0 ? " @ " + PyNum.num(price) : "";
    }

    // ------------------------------------------------------------------ internal bookkeeping

    /** Terms proposed by a {@code G} -- applied only when a {@code 150=5} confirms it. */
    private static final class StagedTerms {
        final Double orderQty;
        final Double price;
        final Double stopPx;
        final TimeInForce timeInForce;

        StagedTerms(Double orderQty, Double price, Double stopPx, TimeInForce timeInForce) {
            this.orderQty = orderQty;
            this.price = price;
            this.stopPx = stopPx;
            this.timeInForce = timeInForce;
        }
    }

    /** An in-flight {@code F}/{@code G} request awaiting an {@code 8} or {@code 9}. */
    private static final class PendingRequest {
        final String action;
        final String clOrdId;
        final StagedTerms terms;

        PendingRequest(String action, String clOrdId, StagedTerms terms) {
            this.action = action;
            this.clOrdId = clOrdId;
            this.terms = terms;
        }
    }

    /** All machine state for one order chain. */
    private static final class Chain {
        final OrderState state;
        /** ExecIDs seen as tag 17 on a {@code 35=8} -- drives dedupe and {@code ExecCount}. */
        final Set<String> execIds = new LinkedHashSet<>();
        /** ExecID -&gt; latest emitted row, so bust/correct/DK can re-emit it. */
        final Map<String, ExecutionRow> execRows = new LinkedHashMap<>();
        /** In-flight F/G requests, oldest first. */
        final List<PendingRequest> pending = new ArrayList<>();
        /** Request ClOrdID -&gt; OrdStatus captured when that request was sent (section 5.5). */
        final Map<String, OrdStatus> priorStatus = new HashMap<>();
        /** A {@code D} is outstanding until the venue responds with a non-PendingNew {@code 8}. */
        boolean newPending;

        Chain(OrderState state) {
            this.state = state;
        }
    }

    /** The two row lists a handler produces. */
    private record Emitted(List<ExecutionRow> executions, List<OrderEventRow> events) {
        static Emitted of(List<ExecutionRow> executions, List<OrderEventRow> events) {
            return new Emitted(executions, events);
        }
    }
}
