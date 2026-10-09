"""Native tables and the per-level adapters -- doc 14 section 3.

This is the only file a real deployment rewrites. Replace :func:`native_tables` with your
own Raptor / algo / router tables (Kafka, a JDBC poll, a parquet snapshot, a remote URI --
anything that is a Deephaven table) and edit the four ``view``s in :func:`adapters` so
each level comes out with ``config.CANONICAL_COLUMNS``. Nothing downstream changes.

The adapter rules that matter:

* **role split** -- the algo server and the router keep parents and children in one table;
  a row with no parent id is a parent. Use whatever flag your system really has.
* **LinkId** -- the id this order carries *for its parent*: the external order id for a
  cross-system hop (algo parent -> Raptor, router parent -> algo child or Raptor), the
  parent order id inside one system (algo child -> algo parent, venue order -> router
  parent).
* **AltId** -- a second id downstream systems may reference (here Raptor's ``ClOrdID``,
  which DMA flow stamps on the router parent while the algo server stores ``OrderId``).
* **Side** -- one coding across systems, or every child is a ``MISMATCH``.
* **Moniker** -- root level only; descendants inherit it.

:func:`order_tree_recon.mockdata.canonical_rows` is the python twin of these views.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence

from deephaven import new_table
from deephaven.column import double_col, long_col, string_col
from deephaven.constants import NULL_DOUBLE
from deephaven.table import Table

from order_tree_recon.mockdata import MockData

__all__ = ["native_tables", "adapters", "RAPTOR_SIDE"]

#: Raptor's FIX tag-54 codes decoded to the names the algo server and router use.
RAPTOR_SIDE = (
    "Side = Side == `1` ? `BUY` : Side == `2` ? `SELL` : Side == `5` ? `SELL_SHORT` : Side"
)

_RAPTOR_COLUMNS = (
    "OrderId", "ClOrdID", "Moniker", "Symbol", "Side", "OrderQty", "CumQty", "LeavesQty", "AvgPx", "OrdStatus",
)
_ALGO_COLUMNS = (
    "AlgoOrderId", "ParentAlgoOrderId", "RaptorOrderId", "Strategy", "Symbol", "Side",
    "Qty", "Filled", "Open", "AvgPrice", "State",
)
_ROUTER_COLUMNS = (
    "RouterOrderId", "ParentRouterOrderId", "UpstreamOrderId", "Venue", "Symbol", "Side",
    "Qty", "CumQty", "LeavesQty", "AvgPx", "Status",
)
_LONG_COLUMNS = frozenset({"OrderQty", "CumQty", "LeavesQty", "Qty", "Filled", "Open"})
_DOUBLE_COLUMNS = frozenset({"AvgPx", "AvgPrice"})


def _table(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> Table:
    cols = []
    for name in columns:
        values = [row[name] for row in rows]
        if name in _LONG_COLUMNS:
            cols.append(long_col(name, [int(v) for v in values]))
        elif name in _DOUBLE_COLUMNS:
            cols.append(double_col(name, [NULL_DOUBLE if v is None else float(v) for v in values]))
        else:
            cols.append(string_col(name, [str(v) for v in values]))
    return new_table(cols)


def native_tables(data: MockData) -> Dict[str, Table]:
    """The three mock system tables, in their native shapes."""
    return {
        "raptor_orders": _table(data.raptor, _RAPTOR_COLUMNS),
        "algo_orders": _table(data.algo, _ALGO_COLUMNS),
        "router_orders": _table(data.router, _ROUTER_COLUMNS),
    }


def adapters(raptor: Table, algo: Table, router: Table) -> Dict[str, Table]:
    """Map the native tables onto one canonical table per default-topology level."""
    algo_parent = "ParentAlgoOrderId == ``"
    router_parent = "ParentRouterOrderId == ``"
    algo_common: List[str] = [
        "OrderId = AlgoOrderId",
        "AltId = ``",
        "Moniker = ``",
        "Symbol",
        "Side",
        "OrderQty = Qty",
        "CumQty = Filled",
        "LeavesQty = Open",
        "AvgPx = AvgPrice",
        "Status = State",
        "Destination = Strategy",
    ]
    router_common: List[str] = [
        "OrderId = RouterOrderId",
        "AltId = ``",
        "Moniker = ``",
        "Symbol",
        "Side",
        "OrderQty = Qty",
        "CumQty",
        "LeavesQty",
        "AvgPx",
        "Status",
        "Destination = Venue",
    ]
    return {
        "CLIENT": raptor.update_view([RAPTOR_SIDE]).view(
            [
                "OrderId",
                "AltId = ClOrdID",
                "LinkId = ``",
                "Moniker",
                "Symbol",
                "Side",
                "OrderQty",
                "CumQty",
                "LeavesQty",
                "AvgPx",
                "Status = OrdStatus",
                "Destination = ``",
            ]
        ),
        "ALGO_PARENT": algo.where(algo_parent).view(["LinkId = RaptorOrderId"] + algo_common),
        "ALGO_CHILD": algo.where(f"!({algo_parent})").view(["LinkId = ParentAlgoOrderId"] + algo_common),
        "OR_PARENT": router.where(router_parent).view(["LinkId = UpstreamOrderId"] + router_common),
        "OR_CHILD": router.where(f"!({router_parent})").view(["LinkId = ParentRouterOrderId"] + router_common),
    }
