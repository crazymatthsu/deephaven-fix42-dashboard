package com.deephaven.fix42.oms;

import com.deephaven.fix42.codec.FixConstants;
import com.deephaven.fix42.codec.FixMessage;
import com.deephaven.fix42.codec.FixParser;
import com.deephaven.fix42.codec.Tags;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class InMemoryOmsCache implements OmsCache {
    private final CacheConfig config;
    private final FixParser parser;
    private final OrderStateUpdater updater;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<String, OrderState> orders = new LinkedHashMap<>();
    private final Map<String, String> clOrdIdIndex = new HashMap<>();
    private final Map<String, String> orderIdIndex = new HashMap<>();
    private final Map<String, String> execIdIndex = new HashMap<>();
    private final Map<String, Set<String>> accountIndex = new HashMap<>();
    private final Map<String, Set<String>> symbolIndex = new HashMap<>();
    private final Map<String, Set<String>> parentIndex = new HashMap<>();
    private final Map<String, Deque<String>> history = new HashMap<>();

    public InMemoryOmsCache() {
        this(CacheConfig.defaults());
    }

    public InMemoryOmsCache(CacheConfig config) {
        this.config = config;
        this.parser = new FixParser(config.parserConfig());
        this.updater = new OrderStateUpdater(config);
    }

    @Override
    public ProcessResult ingest(String rawFix) {
        FixMessage msg = parser.parse(rawFix);
        String type = msg.msgType();
        if (type.isEmpty()) {
            throw new UnidentifiableOrderException("missing MsgType (35)");
        }
        if (!isSupported(type)) {
            throw new UnsupportedMessageTypeException(type);
        }
        lock.writeLock().lock();
        try {
            return ingestLocked(msg);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private ProcessResult ingestLocked(FixMessage msg) {
        String type = msg.msgType();
        Resolve resolve = resolve(msg, type);
        boolean created = resolve.created;
        String previousKey = "";
        OrderState state = resolve.state;

        if (resolve.rekeyTo != null && !resolve.rekeyTo.equals(state.getOrderKey())) {
            previousKey = state.getOrderKey();
            rekey(state, resolve.rekeyTo);
        }

        bindIdentifiers(state, msg);
        boolean applied = updater.apply(state, msg);
        if (applied) {
            state.bumpVersion();
        }
        reindexSecondary(state, previousKey);
        appendHistory(state.getOrderKey(), msg.raw());

        return new ProcessResult(
                state.getOrderKey(),
                previousKey,
                state.copy(),
                created,
                applied,
                false,
                msg.raw(),
                type);
    }

    private Resolve resolve(FixMessage msg, String type) {
        String orderId = FixValues.str(msg, Tags.ORDER_ID);
        if ("NONE".equalsIgnoreCase(orderId)) {
            orderId = "";
        }
        String clOrdId = FixValues.str(msg, Tags.CL_ORD_ID);
        String origClOrdId = FixValues.str(msg, Tags.ORIG_CL_ORD_ID);
        String execId = FixValues.str(msg, Tags.EXEC_ID);
        String execRefId = FixValues.str(msg, Tags.EXEC_REF_ID);

        String key = null;
        if (!orderId.isEmpty() && orderIdIndex.containsKey(orderId)) {
            key = orderIdIndex.get(orderId);
        } else if (!clOrdId.isEmpty() && clOrdIdIndex.containsKey(clOrdId)) {
            key = clOrdIdIndex.get(clOrdId);
        } else if (!origClOrdId.isEmpty() && clOrdIdIndex.containsKey(origClOrdId)) {
            key = clOrdIdIndex.get(origClOrdId);
        } else if (!execId.isEmpty() && execIdIndex.containsKey(execId)) {
            key = execIdIndex.get(execId);
        } else if (!execRefId.isEmpty() && execIdIndex.containsKey(execRefId)) {
            key = execIdIndex.get(execRefId);
        }

        if (key != null) {
            OrderState existing = orders.get(key);
            String rekeyTo = null;
            if (!orderId.isEmpty() && !orderId.equals(existing.getOrderKey())) {
                if (!orderIdIndex.containsKey(orderId) || orderId.equals(existing.getOrderId())
                        || existing.getOrderId().isEmpty()) {
                    rekeyTo = orderId;
                }
            }
            return new Resolve(existing, false, rekeyTo);
        }

        if (clOrdId.isEmpty() && orderId.isEmpty()) {
            if (FixConstants.MSG_ORDER_STATUS_REQUEST.equals(type)) {
                throw new UnidentifiableOrderException("status request has no ClOrdID or OrderID");
            }
            throw new UnidentifiableOrderException("message has no ClOrdID (11) or OrderID (37)");
        }
        String newKey = !orderId.isEmpty() ? orderId : clOrdId;
        OrderState created = new OrderState();
        created.setOrderKey(newKey);
        if (!clOrdId.isEmpty()) {
            created.setClOrdId(clOrdId);
        }
        orders.put(newKey, created);
        return new Resolve(created, true, null);
    }

    private void rekey(OrderState state, String newKey) {
        String oldKey = state.getOrderKey();
        if (oldKey.equals(newKey)) {
            return;
        }
        orders.remove(oldKey);
        state.setOrderKey(newKey);
        if (state.getOrderId().isEmpty()) {
            state.setOrderId(newKey);
        }
        orders.put(newKey, state);
        remapIndex(clOrdIdIndex, oldKey, newKey);
        remapIndex(orderIdIndex, oldKey, newKey);
        remapIndex(execIdIndex, oldKey, newKey);
        remapMulti(accountIndex, oldKey, newKey);
        remapMulti(symbolIndex, oldKey, newKey);
        remapMulti(parentIndex, oldKey, newKey);
        Deque<String> tape = history.remove(oldKey);
        if (tape != null) {
            history.put(newKey, tape);
        }
        for (OrderState other : orders.values()) {
            if (oldKey.equals(other.getParentOrderId())) {
                other.setParentOrderId(newKey);
            }
            List<String> children = other.getChildOrderKeys();
            int idx = children.indexOf(oldKey);
            if (idx >= 0) {
                children.set(idx, newKey);
            }
        }
    }

    private void bindIdentifiers(OrderState state, FixMessage msg) {
        String key = state.getOrderKey();
        bind(clOrdIdIndex, FixValues.str(msg, Tags.CL_ORD_ID), key);
        bind(clOrdIdIndex, FixValues.str(msg, Tags.ORIG_CL_ORD_ID), key);
        if (!state.getClOrdId().isEmpty()) {
            bind(clOrdIdIndex, state.getClOrdId(), key);
        }
        for (String hist : state.getClOrdIdHistory()) {
            bind(clOrdIdIndex, hist, key);
        }
        String orderId = FixValues.str(msg, Tags.ORDER_ID);
        if (!orderId.isEmpty() && !"NONE".equalsIgnoreCase(orderId)) {
            bind(orderIdIndex, orderId, key);
        }
        if (!state.getOrderId().isEmpty()) {
            bind(orderIdIndex, state.getOrderId(), key);
        }
        bind(execIdIndex, FixValues.str(msg, Tags.EXEC_ID), key);
        bind(execIdIndex, FixValues.str(msg, Tags.EXEC_REF_ID), key);
    }

    private void reindexSecondary(OrderState state, String previousKey) {
        if (!previousKey.isEmpty()) {
            removeFromMulti(accountIndex, previousKey);
            removeFromMulti(symbolIndex, previousKey);
        }
        removeFromMulti(accountIndex, state.getOrderKey());
        removeFromMulti(symbolIndex, state.getOrderKey());
        if (!state.getAccount().isEmpty()) {
            accountIndex.computeIfAbsent(state.getAccount(), k -> new LinkedHashSet<>()).add(state.getOrderKey());
        }
        if (!state.getSymbol().isEmpty()) {
            symbolIndex.computeIfAbsent(state.getSymbol(), k -> new LinkedHashSet<>()).add(state.getOrderKey());
        }
        if (!state.getParentOrderId().isEmpty() || !state.getParentClOrdId().isEmpty()) {
            String parentRef = !state.getParentOrderId().isEmpty() ? state.getParentOrderId() : state.getParentClOrdId();
            parentIndex.computeIfAbsent(parentRef, k -> new LinkedHashSet<>()).add(state.getOrderKey());
            OrderState parent = orders.get(parentRef);
            if (parent == null) {
                String mapped = clOrdIdIndex.get(parentRef);
                if (mapped != null) {
                    parent = orders.get(mapped);
                }
            }
            if (parent != null && !parent.getChildOrderKeys().contains(state.getOrderKey())) {
                parent.getChildOrderKeys().add(state.getOrderKey());
            }
        }
    }

    private void appendHistory(String orderKey, String raw) {
        if (config.historyLimit() <= 0) {
            return;
        }
        Deque<String> tape = history.computeIfAbsent(orderKey, k -> new ArrayDeque<>());
        tape.addLast(raw);
        while (tape.size() > config.historyLimit()) {
            tape.removeFirst();
        }
    }

    private static boolean isSupported(String type) {
        return FixConstants.MSG_NEW_ORDER_SINGLE.equals(type)
                || FixConstants.MSG_EXECUTION_REPORT.equals(type)
                || FixConstants.MSG_ORDER_CANCEL_REJECT.equals(type)
                || FixConstants.MSG_ORDER_CANCEL_REQUEST.equals(type)
                || FixConstants.MSG_ORDER_CANCEL_REPLACE.equals(type)
                || FixConstants.MSG_ORDER_STATUS_REQUEST.equals(type)
                || FixConstants.MSG_DONT_KNOW_TRADE.equals(type);
    }

    private static void bind(Map<String, String> index, String id, String key) {
        if (id != null && !id.isEmpty() && !"NONE".equalsIgnoreCase(id)) {
            index.put(id, key);
        }
    }

    private static void remapIndex(Map<String, String> index, String oldKey, String newKey) {
        for (Map.Entry<String, String> e : index.entrySet()) {
            if (oldKey.equals(e.getValue())) {
                e.setValue(newKey);
            }
        }
    }

    private static void remapMulti(Map<String, Set<String>> index, String oldKey, String newKey) {
        for (Set<String> keys : index.values()) {
            if (keys.remove(oldKey)) {
                keys.add(newKey);
            }
        }
    }

    private static void removeFromMulti(Map<String, Set<String>> index, String key) {
        for (Set<String> keys : index.values()) {
            keys.remove(key);
        }
    }

    @Override
    public Optional<OrderState> get(String orderKey) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(orders.get(orderKey)).map(OrderState::copy);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<OrderState> getByClOrdId(String clOrdId) {
        lock.readLock().lock();
        try {
            String key = clOrdIdIndex.get(clOrdId);
            return key == null ? Optional.empty() : Optional.of(orders.get(key).copy());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<OrderState> getByOrderId(String orderId) {
        lock.readLock().lock();
        try {
            String key = orderIdIndex.get(orderId);
            if (key == null) {
                OrderState direct = orders.get(orderId);
                return direct == null ? Optional.empty() : Optional.of(direct.copy());
            }
            return Optional.of(orders.get(key).copy());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<OrderState> getByExecId(String execId) {
        lock.readLock().lock();
        try {
            String key = execIdIndex.get(execId);
            return key == null ? Optional.empty() : Optional.of(orders.get(key).copy());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<OrderState> findByAccount(String account) {
        lock.readLock().lock();
        try {
            return copyAll(accountIndex.getOrDefault(account, Set.of()));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<OrderState> findBySymbol(String symbol) {
        lock.readLock().lock();
        try {
            return copyAll(symbolIndex.getOrDefault(symbol, Set.of()));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<OrderState> getChildren(String parentOrderId) {
        lock.readLock().lock();
        try {
            Set<String> keys = new LinkedHashSet<>(parentIndex.getOrDefault(parentOrderId, Set.of()));
            String mapped = clOrdIdIndex.get(parentOrderId);
            if (mapped != null) {
                keys.addAll(parentIndex.getOrDefault(mapped, Set.of()));
            }
            return copyAll(keys);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<OrderState> getParent(String childOrderKey) {
        lock.readLock().lock();
        try {
            OrderState child = orders.get(childOrderKey);
            if (child == null) {
                String mapped = clOrdIdIndex.get(childOrderKey);
                child = mapped == null ? null : orders.get(mapped);
            }
            if (child == null) {
                return Optional.empty();
            }
            String parentRef = !child.getParentOrderId().isEmpty() ? child.getParentOrderId() : child.getParentClOrdId();
            if (parentRef.isEmpty()) {
                return Optional.empty();
            }
            OrderState parent = orders.get(parentRef);
            if (parent == null) {
                String mapped = clOrdIdIndex.get(parentRef);
                parent = mapped == null ? null : orders.get(mapped);
            }
            return parent == null ? Optional.empty() : Optional.of(parent.copy());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ChildRollup rollup(String parentOrderId) {
        List<OrderState> children = getChildren(parentOrderId);
        double oq = 0;
        double cq = 0;
        double lq = 0;
        for (OrderState child : children) {
            oq += child.getOrderQty();
            cq += child.getCumQty();
            lq += child.getLeavesQty();
        }
        return new ChildRollup(children.size(), oq, cq, lq);
    }

    @Override
    public List<String> getHistory(String orderKey) {
        lock.readLock().lock();
        try {
            Deque<String> tape = history.get(orderKey);
            return tape == null ? List.of() : List.copyOf(tape);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Collection<OrderState> snapshot() {
        lock.readLock().lock();
        try {
            List<OrderState> copy = new ArrayList<>(orders.size());
            for (OrderState state : orders.values()) {
                copy.add(state.copy());
            }
            return Collections.unmodifiableList(copy);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return orders.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private List<OrderState> copyAll(Set<String> keys) {
        List<OrderState> out = new ArrayList<>();
        for (String key : keys) {
            OrderState state = orders.get(key);
            if (state != null) {
                out.add(state.copy());
            }
        }
        return out;
    }

    private record Resolve(OrderState state, boolean created, String rekeyTo) {}
}
