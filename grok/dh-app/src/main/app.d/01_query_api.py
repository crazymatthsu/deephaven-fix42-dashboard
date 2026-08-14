"""Query helpers bound in Application Mode.

Java OmsCache is the correctness API. These functions wrap it so a Code
Studio user can look up latest state without writing where-clauses.
Live-table equivalents stay in the DAG (orders_latest, clord_index, …).
"""

from deephaven import new_table
from deephaven.column import string_col, double_col, bool_col, int_col


def _state_table(states):
    if not states:
        return new_table(
            [
                string_col("OrderKey", []),
                string_col("ClOrdID", []),
                string_col("OrderID", []),
                string_col("Account", []),
                string_col("Symbol", []),
                string_col("OrdStatus", []),
                double_col("OrderQty", []),
                double_col("CumQty", []),
                double_col("LeavesQty", []),
                double_col("AvgPx", []),
                double_col("Price", []),
                bool_col("PendingCancel", []),
                bool_col("PendingReplace", []),
                bool_col("DkTrade", []),
                int_col("Version", []),
            ]
        )
    return new_table(
        [
            string_col("OrderKey", [s.getOrderKey() for s in states]),
            string_col("ClOrdID", [s.getClOrdId() for s in states]),
            string_col("OrderID", [s.getOrderId() for s in states]),
            string_col("Account", [s.getAccount() for s in states]),
            string_col("Symbol", [s.getSymbol() for s in states]),
            string_col("OrdStatus", [s.getOrdStatus() for s in states]),
            double_col("OrderQty", [float(s.getOrderQty()) for s in states]),
            double_col("CumQty", [float(s.getCumQty()) for s in states]),
            double_col("LeavesQty", [float(s.getLeavesQty()) for s in states]),
            double_col("AvgPx", [float(s.getAvgPx()) for s in states]),
            double_col("Price", [float(s.getPrice()) for s in states]),
            bool_col("PendingCancel", [bool(s.isPendingCancel()) for s in states]),
            bool_col("PendingReplace", [bool(s.isPendingReplace()) for s in states]),
            bool_col("DkTrade", [bool(s.isDkTrade()) for s in states]),
            int_col("Version", [int(s.getVersion()) for s in states]),
        ]
    )


def _one(optional):
    return _state_table([] if optional is None else [optional])


def get_by_cl_ord_id(cl_ord_id: str):
    found = oms_cache.getByClOrdId(cl_ord_id)
    return _one(None if found.isEmpty() else found.get())


def get_by_order_id(order_id: str):
    found = oms_cache.getByOrderId(order_id)
    return _one(None if found.isEmpty() else found.get())


def get_by_exec_id(exec_id: str):
    found = oms_cache.getByExecId(exec_id)
    return _one(None if found.isEmpty() else found.get())


def get_order(order_key: str):
    found = oms_cache.get(order_key)
    return _one(None if found.isEmpty() else found.get())


def find_by_account(account: str):
    return _state_table(list(oms_cache.findByAccount(account)))


def find_by_symbol(symbol: str):
    return _state_table(list(oms_cache.findBySymbol(symbol)))


def get_children(parent_order_id: str):
    return _state_table(list(oms_cache.getChildren(parent_order_id)))


def get_history(order_key: str):
    rows = list(oms_cache.getHistory(order_key))
    return new_table([string_col("RawFix", rows)])


def resolve_order_key(token: str) -> str:
    """Resolve ClOrdID, OrderID, ExecID, or OrderKey to OrderKey."""
    if not token:
        return ""
    for lookup in (oms_cache.get, oms_cache.getByClOrdId, oms_cache.getByOrderId, oms_cache.getByExecId):
        found = lookup(token)
        if not found.isEmpty():
            return found.get().getOrderKey()
    return token
