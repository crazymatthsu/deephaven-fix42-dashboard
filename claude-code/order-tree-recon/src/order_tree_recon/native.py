"""The three systems' native order tables: names, id keys, columns and types -- doc 14 s3.

Pure stdlib. One definition serves every place a native row crosses a boundary: the mock
tables (:mod:`order_tree_recon.sources`), the AMPS JSON codec and topics
(:mod:`order_tree_recon.amps_codec`), and the publisher. A real deployment whose systems
look different edits this file together with the adapters in ``sources.py``.

Each native table holds the **current state** of every order of one system, one row per
order, keyed by that system's own order id (the AMPS SOW key of its topic).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

__all__ = ["STRING", "LONG", "DOUBLE", "NativeSchema", "RAPTOR", "ALGO", "ROUTER", "NATIVE_SCHEMAS"]

STRING = "string"
LONG = "long"
DOUBLE = "double"


@dataclass(frozen=True)
class NativeSchema:
    """One native order table.

    Attributes:
        name: Table (and global) name, e.g. ``router_orders``; also the AMPS topic suffix.
        key: The column holding the system's order id -- unique per row.
        columns: ``(column, type)`` pairs in table order; type is one of
            :data:`STRING`, :data:`LONG`, :data:`DOUBLE`.
    """

    name: str
    key: str
    columns: Tuple[Tuple[str, str], ...]

    @property
    def column_names(self) -> Tuple[str, ...]:
        return tuple(name for name, _ in self.columns)

    def type_of(self, column: str) -> str:
        for name, kind in self.columns:
            if name == column:
                return kind
        raise KeyError(column)


#: Raptor: FIX side codes, both its own ``OrderId`` and the ``ClOrdID`` it sends out.
RAPTOR = NativeSchema(
    "raptor_orders",
    "OrderId",
    (
        ("OrderId", STRING),
        ("ClOrdID", STRING),
        ("Moniker", STRING),
        ("Symbol", STRING),
        ("Side", STRING),
        ("OrderQty", LONG),
        ("CumQty", LONG),
        ("LeavesQty", LONG),
        ("AvgPx", DOUBLE),
        ("OrdStatus", STRING),
    ),
)
#: The algo server: parents and children in one table; a parent has no ``ParentAlgoOrderId``.
ALGO = NativeSchema(
    "algo_orders",
    "AlgoOrderId",
    (
        ("AlgoOrderId", STRING),
        ("ParentAlgoOrderId", STRING),
        ("RaptorOrderId", STRING),
        ("Strategy", STRING),
        ("Symbol", STRING),
        ("Side", STRING),
        ("Qty", LONG),
        ("Filled", LONG),
        ("Open", LONG),
        ("AvgPrice", DOUBLE),
        ("State", STRING),
    ),
)
#: The order router: parents carry the upstream id, venue orders their parent's id.
ROUTER = NativeSchema(
    "router_orders",
    "RouterOrderId",
    (
        ("RouterOrderId", STRING),
        ("ParentRouterOrderId", STRING),
        ("UpstreamOrderId", STRING),
        ("Venue", STRING),
        ("Symbol", STRING),
        ("Side", STRING),
        ("Qty", LONG),
        ("CumQty", LONG),
        ("LeavesQty", LONG),
        ("AvgPx", DOUBLE),
        ("Status", STRING),
    ),
)

#: In upstream-to-downstream order: Raptor, algo, router.
NATIVE_SCHEMAS: Dict[str, NativeSchema] = {schema.name: schema for schema in (RAPTOR, ALGO, ROUTER)}
