"""Native tables and the per-level adapters -- doc 14 section 3.

This is the only file a real deployment rewrites. Replace :func:`native_tables` with your
own Raptor / algo / router tables (Kafka, a JDBC poll, a parquet snapshot, a remote URI --
anything that is a Deephaven table) and edit the four ``view``s in :func:`adapters` so
each level comes out with ``config.CANONICAL_COLUMNS``. Nothing downstream changes.

The adapter rules that matter:

* **role split** -- the algo server and the router keep parents and children in one table;
  a row with no parent id (empty *or null*: a JSON feed omits the field) is a parent. Use
  whatever flag your system really has.
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
from deephaven.constants import NULL_DOUBLE, NULL_LONG
from deephaven.table import Table

from order_tree_recon.mockdata import MockData
from order_tree_recon.native import DOUBLE, LONG, NATIVE_SCHEMAS, NativeSchema

__all__ = ["native_tables", "adapters", "RAPTOR_SIDE"]

#: Raptor's FIX tag-54 codes decoded to the names the algo server and router use.
RAPTOR_SIDE = (
    "Side = Side == `1` ? `BUY` : Side == `2` ? `SELL` : Side == `5` ? `SELL_SHORT` : Side"
)

def _table(rows: Sequence[Mapping[str, Any]], schema: NativeSchema) -> Table:
    """Rows of one native table as a static table; ``None`` (or a missing key) is a null."""
    cols = []
    for name, kind in schema.columns:
        values = [row.get(name) for row in rows]
        if kind == LONG:
            cols.append(long_col(name, [NULL_LONG if v is None else int(v) for v in values]))
        elif kind == DOUBLE:
            cols.append(double_col(name, [NULL_DOUBLE if v is None else float(v) for v in values]))
        else:
            cols.append(string_col(name, [None if v is None else str(v) for v in values]))
    return new_table(cols)


def native_tables(data: MockData) -> Dict[str, Table]:
    """The three mock system tables, in their native shapes."""
    rows = {"raptor_orders": data.raptor, "algo_orders": data.algo, "router_orders": data.router}
    return {name: _table(rows[name], schema) for name, schema in NATIVE_SCHEMAS.items()}


def adapters(raptor: Table, algo: Table, router: Table) -> Dict[str, Table]:
    """Map the native tables onto one canonical table per default-topology level."""
    # A null parent id is a parent too: `== ``` alone is false on null, which would file
    # the order as a child with no link (NO_LINK) and orphan its whole subtree.
    algo_parent = "isNull(ParentAlgoOrderId) || ParentAlgoOrderId == ``"
    router_parent = "isNull(ParentRouterOrderId) || ParentRouterOrderId == ``"
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
