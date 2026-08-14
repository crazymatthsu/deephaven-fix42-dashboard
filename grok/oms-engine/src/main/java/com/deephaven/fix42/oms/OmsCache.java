package com.deephaven.fix42.oms;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface OmsCache {
    ProcessResult ingest(String rawFix);

    Optional<OrderState> get(String orderKey);

    Optional<OrderState> getByClOrdId(String clOrdId);

    Optional<OrderState> getByOrderId(String orderId);

    Optional<OrderState> getByExecId(String execId);

    List<OrderState> findByAccount(String account);

    List<OrderState> findBySymbol(String symbol);

    List<OrderState> getChildren(String parentOrderId);

    Optional<OrderState> getParent(String childOrderKey);

    ChildRollup rollup(String parentOrderId);

    List<String> getHistory(String orderKey);

    Collection<OrderState> snapshot();

    int size();
}
