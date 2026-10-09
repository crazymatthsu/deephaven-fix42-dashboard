"""Deterministic mock order tables for Raptor, the algo server and the order router.

The three tables come out in **native** shapes -- each system with its own column names,
id formats and codes -- because that is the situation doc 14 answers: the orders are
already in Deephaven, one table per system, and an adapter per level maps them onto the
canonical node columns. :func:`canonical_rows` is the pure-python twin of those adapters
(:mod:`order_tree_recon.sources`), used by the reference oracle and the unit suite.

=============  ==========================================================================
System         Native columns
=============  ==========================================================================
``RAPTOR``     ``OrderId ClOrdID Moniker Symbol Side(FIX 1/2/5) OrderQty CumQty
               LeavesQty AvgPx OrdStatus``
``ALGO``       ``AlgoOrderId ParentAlgoOrderId RaptorOrderId Strategy Symbol Side Qty
               Filled Open AvgPrice State`` -- parents and children in one table; a
               parent has ``ParentAlgoOrderId == ""`` and carries Raptor's ``OrderId``
``OR``         ``RouterOrderId ParentRouterOrderId UpstreamOrderId Venue Symbol Side Qty
               CumQty LeavesQty AvgPx Status`` -- parents carry the upstream id (an algo
               child's id, or Raptor's ``ClOrdID`` for DMA flow), children their parent's
=============  ==========================================================================

Only Raptor carries the client moniker. Every scenario family is built as a tree, rolled
up leaf-to-root exactly the way upstream systems book fills (a parent's ``CumQty`` is the
sum of what its children reported), and then a scenario injects one realistic fault.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

__all__ = [
    "MONIKERS",
    "SYMBOLS",
    "SCENARIOS",
    "EXPECTED_BREAKS",
    "SIDE_TO_FIX",
    "FIX_TO_SIDE",
    "MockOrder",
    "MockData",
    "generate",
    "canonical_rows",
    "split_qty",
]

#: Fictional client monikers.
MONIKERS: Tuple[str, ...] = ("ACME", "BLUEFIN", "CEDAR", "DUNE")
#: Symbol -> reference price.
SYMBOLS: Dict[str, float] = {"AAPL": 228.0, "MSFT": 431.0, "NVDA": 121.0, "AMZN": 186.0, "JPM": 212.0}
VENUES: Tuple[str, ...] = ("NYSE", "NASDAQ", "ARCA", "BATS", "IEX", "DARK-X")
STRATEGIES: Tuple[str, ...] = ("VWAP", "TWAP", "POV", "IS")

#: Raptor speaks FIX side codes; the algo server and router use names.
SIDE_TO_FIX: Dict[str, str] = {"BUY": "1", "SELL": "2", "SELL_SHORT": "5"}
FIX_TO_SIDE: Dict[str, str] = {code: name for name, code in SIDE_TO_FIX.items()}

#: The fixed scenario catalog, in generation order.
SCENARIOS: Tuple[str, ...] = (
    "clean_filled",
    "working",
    "dma",
    "unrouted_client",
    "unbooked_fill",
    "over_routed",
    "cancel_not_propagated",
    "orphan",
    "side_mismatch",
    "overfill",
    "nested_algo",
)

#: Scenario -> the non-``NONE`` ``(Level, BreakKind)`` pairs its tree must show, sorted.
EXPECTED_BREAKS: Dict[str, List[Tuple[str, str]]] = {
    "clean_filled": [],
    "working": [("ALGO_PARENT", "HELD")],
    "dma": [],
    "unrouted_client": [("CLIENT", "HELD")],
    "unbooked_fill": [("OR_PARENT", "UNBOOKED_FILL")],
    "over_routed": [("OR_PARENT", "OVER_ROUTED")],
    "cancel_not_propagated": [("OR_PARENT", "CHILD_STILL_OPEN")],
    "orphan": [("OR_PARENT", "ORPHAN")],
    "side_mismatch": [("OR_CHILD", "MISMATCH")],
    "overfill": [("ALGO_CHILD", "UNBOOKED_FILL"), ("OR_PARENT", "OVERFILL")],
    "nested_algo": [],
}


@dataclass
class MockOrder:
    """One order in a mock family tree (any level)."""

    level: str
    order_id: str
    qty: float
    symbol: str
    side: str
    link_id: str = ""
    alt_id: str = ""
    moniker: str = ""
    destination: str = ""
    children: List["MockOrder"] = field(default_factory=list)
    #: Street orders only: fraction of ``qty`` filled (rounded to round lots).
    fill: float = 0.0
    px: float = 0.0
    #: ``False`` -> the order is terminal (canceled): ``LeavesQty`` is 0.
    live: bool = True
    #: Added to the children's cum: a negative value is a fill report that never arrived.
    cum_adjust: float = 0.0
    #: Cap ``CumQty`` at ``qty`` (an upstream that rejects fills beyond its order size).
    cap: bool = False
    cum: float = 0.0
    leaves: float = 0.0
    avg_px: Optional[float] = None
    status: str = "NEW"

    def walk(self) -> Iterator["MockOrder"]:
        yield self
        for child in self.children:
            yield from child.walk()


def split_qty(total: float, parts: int) -> List[float]:
    """Split ``total`` into ``parts`` round-lot slices (remainder on the first)."""
    total = int(total)
    lots, odd = divmod(total, 100)
    parts = max(1, min(parts, max(lots, 1)))
    base, extra = divmod(lots, parts)
    out = [float((base + (1 if i < extra else 0)) * 100) for i in range(parts)]
    out[0] += odd
    return [q for q in out if q > 0]


def _rollup(order: MockOrder) -> None:
    """Book fills leaf -> root the way each upstream system would see them."""
    for child in order.children:
        _rollup(child)
    if order.children:
        cum = sum(c.cum for c in order.children)
        notional = sum(c.cum * (c.avg_px or 0.0) for c in order.children)
        avg = notional / cum if cum else None
        cum += order.cum_adjust
        if order.cap:
            cum = min(cum, order.qty)
    else:
        cum = float(min(round(order.qty * order.fill / 100.0) * 100, order.qty)) if order.fill else 0.0
        avg = order.px if cum else None
    order.cum = cum
    order.avg_px = round(avg, 4) if avg is not None and cum > 0 else None
    order.leaves = max(order.qty - cum, 0.0) if order.live else 0.0
    if not order.live:
        order.status = "CANCELED"
    elif cum >= order.qty:
        order.status = "FILLED"
    elif cum > 0:
        order.status = "PARTIALLY_FILLED"
    else:
        order.status = "NEW"


class _Builder:
    """Allocates ids and builds families; deterministic for a given rng."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.n_client = 0
        self.n_algo = 0
        self.n_router = 0

    def price(self, symbol: str) -> float:
        return round(SYMBOLS[symbol] * (1 + self.rng.uniform(-0.004, 0.004)), 2)

    def client(self, moniker: str, symbol: str, side: str, qty: float) -> MockOrder:
        self.n_client += 1
        n = self.n_client
        return MockOrder("CLIENT", f"RPT-{n:05d}", qty, symbol, side, alt_id=f"{moniker}-{n:05d}", moniker=moniker)

    def algo_parent(self, client: MockOrder, qty: Optional[float] = None) -> MockOrder:
        self.n_algo += 1
        order = MockOrder(
            "ALGO_PARENT",
            f"ALG-{self.n_algo:05d}",
            client.qty if qty is None else qty,
            client.symbol,
            client.side,
            link_id=client.order_id,
            destination=self.rng.choice(STRATEGIES),
        )
        client.children.append(order)
        return order

    def algo_child(self, parent: MockOrder, qty: float, destination: str = "") -> MockOrder:
        order = MockOrder(
            "ALGO_CHILD",
            f"{parent.order_id}-{len(parent.children) + 1:02d}",
            qty,
            parent.symbol,
            parent.side,
            link_id=parent.order_id,
            destination=destination or parent.destination,
        )
        parent.children.append(order)
        return order

    def router_parent(self, upstream: Optional[MockOrder], qty: float, link_id: Optional[str] = None,
                      symbol: str = "", side: str = "") -> MockOrder:
        self.n_router += 1
        order = MockOrder(
            "OR_PARENT",
            f"OR-{self.n_router:05d}",
            qty,
            upstream.symbol if upstream else symbol,
            upstream.side if upstream else side,
            link_id=link_id if link_id is not None else (upstream.order_id if upstream else ""),
            destination="SOR",
        )
        if upstream is not None:
            upstream.children.append(order)
        return order

    def venue(self, parent: MockOrder, qty: float, fill: float) -> MockOrder:
        order = MockOrder(
            "OR_CHILD",
            f"{parent.order_id}.{len(parent.children) + 1}",
            qty,
            parent.symbol,
            parent.side,
            link_id=parent.order_id,
            destination=VENUES[(len(parent.children) + self.n_router) % len(VENUES)],
            fill=fill,
            px=self.price(parent.symbol),
        )
        parent.children.append(order)
        return order

    def route(self, upstream: MockOrder, qty: float, venues: int, fill: float) -> MockOrder:
        router = self.router_parent(upstream, qty)
        for venue_qty in split_qty(qty, venues):
            self.venue(router, venue_qty, fill)
        return router

    def algo_family(self, moniker: str, symbol: str, side: str, qty: float, release: float = 1.0,
                    fill: float = 1.0, slices: int = 3, venues: int = 2) -> MockOrder:
        client = self.client(moniker, symbol, side, qty)
        parent = self.algo_parent(client)
        released = int(qty * release / 100) * 100
        for slice_qty in split_qty(released, slices):
            child = self.algo_child(parent, slice_qty)
            self.route(child, slice_qty, venues, fill)
        return client

    def dma_family(self, moniker: str, symbol: str, side: str, qty: float, fill: float = 0.5,
                   venues: int = 3) -> MockOrder:
        client = self.client(moniker, symbol, side, qty)
        # DMA: Raptor sends straight to the router, stamping its outbound ClOrdID --
        # which is why the id index covers AltId as well as OrderId.
        router = self.router_parent(client, qty, link_id=client.alt_id)
        for venue_qty in split_qty(qty, venues):
            self.venue(router, venue_qty, fill)
        return client


def _scenario(builder: _Builder, name: str) -> MockOrder:
    """Build one catalog family; returns its root (a Raptor order, or the orphan)."""
    b = builder
    if name == "clean_filled":
        return b.algo_family("ACME", "AAPL", "BUY", 1000, fill=1.0)
    if name == "working":
        return b.algo_family("ACME", "MSFT", "BUY", 3000, release=0.6, fill=0.34)
    if name == "dma":
        return b.dma_family("BLUEFIN", "NVDA", "SELL", 2000, fill=0.5, venues=3)
    if name == "unrouted_client":
        return b.client("BLUEFIN", "AMZN", "BUY", 500)
    if name == "unbooked_fill":
        client = b.algo_family("CEDAR", "JPM", "BUY", 900, fill=1.0, slices=3, venues=1)
        router = client.children[0].children[1].children[0]
        router.cum_adjust = -100  # the venue's last fill never reached the router parent
        return client
    if name == "over_routed":
        client = b.client("CEDAR", "AAPL", "SELL", 600)
        parent = b.algo_parent(client)
        first = b.algo_child(parent, 300)
        router = b.router_parent(first, 300)
        b.venue(router, 200, 0.5)  # 200 + 200 working against a 300 parent
        b.venue(router, 200, 0.5)
        second = b.algo_child(parent, 300)
        b.route(second, 300, 1, 0.5)
        return client
    if name == "cancel_not_propagated":
        client = b.algo_family("DUNE", "MSFT", "BUY", 800, fill=0.5, slices=2, venues=2)
        for order in client.walk():
            order.live = False
        client.children[0].children[0].children[0].children[1].live = True  # still on the venue
        return client
    if name == "orphan":
        # The router's UpstreamOrderId names an algo child that does not exist (a
        # mistyped id, or an algo tape that never landed): no moniker can be inherited.
        router = b.router_parent(None, 400, link_id="ALG-99999-01", symbol="NVDA", side="BUY")
        b.venue(router, 200, 0.5)
        b.venue(router, 200, 0.5)
        return router
    if name == "side_mismatch":
        client = b.algo_family("ACME", "NVDA", "BUY", 400, fill=1.0, slices=1, venues=2)
        client.children[0].children[0].children[0].children[1].side = "SELL"
        return client
    if name == "overfill":
        client = b.client("BLUEFIN", "JPM", "SELL", 500)
        parent = b.algo_parent(client)
        child = b.algo_child(parent, 500)
        child.cap = True  # the algo books at most its own size
        router = b.router_parent(child, 500)
        b.venue(router, 300, 1.0)  # 600 executed on the street against 500
        b.venue(router, 300, 1.0)
        return client
    if name == "nested_algo":
        client = b.client("CEDAR", "NVDA", "BUY", 1200)
        parent = b.algo_parent(client)
        sub_parent = b.algo_child(parent, 1200, destination="DARK-SEEK")
        for slice_qty in split_qty(1200, 2):
            leaf = b.algo_child(sub_parent, slice_qty)
            b.route(leaf, slice_qty, 2, 0.5)
        return client
    raise KeyError(name)


@dataclass
class MockData:
    """The three native tables as row dicts, plus where each scenario landed."""

    raptor: List[Dict[str, Any]]
    algo: List[Dict[str, Any]]
    router: List[Dict[str, Any]]
    #: scenario -> (System, OrderId) of the family's root order.
    scenario_roots: Dict[str, Tuple[str, str]]
    roots: List[MockOrder]


def generate(seed: int = 42, families: int = 24) -> MockData:
    """Build the scenario catalog plus ``families`` random healthy families.

    Deterministic per ``(seed, families)``. Random families are clean or working algo
    flow (the working ones show the algo parent ``HELD``) or DMA flow.
    """
    rng = random.Random(seed)
    builder = _Builder(rng)
    roots: List[MockOrder] = []
    scenario_roots: Dict[str, Tuple[str, str]] = {}
    for name in SCENARIOS:
        root = _scenario(builder, name)
        roots.append(root)
        system = "RAPTOR" if root.level == "CLIENT" else "OR"
        scenario_roots[name] = (system, root.order_id)
    for _ in range(families):
        moniker = rng.choice(MONIKERS)
        symbol = rng.choice(sorted(SYMBOLS))
        side = rng.choice(("BUY", "BUY", "SELL", "SELL", "SELL_SHORT"))
        qty = float(rng.randrange(5, 51) * 100)
        kind = rng.random()
        if kind < 0.35:
            roots.append(builder.algo_family(moniker, symbol, side, qty, fill=1.0, slices=rng.randint(1, 4)))
        elif kind < 0.75:
            release = rng.choice((0.4, 0.6, 0.8, 1.0))
            fill = rng.choice((0.0, 0.25, 0.5, 0.75))
            roots.append(builder.algo_family(moniker, symbol, side, qty, release=release, fill=fill,
                                             slices=rng.randint(2, 4), venues=rng.randint(1, 3)))
        else:
            roots.append(builder.dma_family(moniker, symbol, side, qty, fill=rng.choice((0.0, 0.5, 1.0)),
                                            venues=rng.randint(1, 3)))
    for root in roots:
        _rollup(root)
    raptor, algo, router = _native_rows(roots)
    return MockData(raptor, algo, router, scenario_roots, roots)


def _native_rows(roots: Sequence[MockOrder]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    raptor: List[Dict[str, Any]] = []
    algo: List[Dict[str, Any]] = []
    router: List[Dict[str, Any]] = []
    for root in roots:
        for order in root.walk():
            if order.level == "CLIENT":
                raptor.append({
                    "OrderId": order.order_id,
                    "ClOrdID": order.alt_id,
                    "Moniker": order.moniker,
                    "Symbol": order.symbol,
                    "Side": SIDE_TO_FIX[order.side],
                    "OrderQty": int(order.qty),
                    "CumQty": int(order.cum),
                    "LeavesQty": int(order.leaves),
                    "AvgPx": order.avg_px,
                    "OrdStatus": order.status,
                })
            elif order.level in ("ALGO_PARENT", "ALGO_CHILD"):
                is_parent = order.level == "ALGO_PARENT"
                algo.append({
                    "AlgoOrderId": order.order_id,
                    "ParentAlgoOrderId": "" if is_parent else order.link_id,
                    "RaptorOrderId": order.link_id if is_parent else "",
                    "Strategy": order.destination,
                    "Symbol": order.symbol,
                    "Side": order.side,
                    "Qty": int(order.qty),
                    "Filled": int(order.cum),
                    "Open": int(order.leaves),
                    "AvgPrice": order.avg_px,
                    "State": order.status,
                })
            else:
                is_parent = order.level == "OR_PARENT"
                router.append({
                    "RouterOrderId": order.order_id,
                    "ParentRouterOrderId": "" if is_parent else order.link_id,
                    "UpstreamOrderId": order.link_id if is_parent else "",
                    "Venue": order.destination,
                    "Symbol": order.symbol,
                    "Side": order.side,
                    "Qty": int(order.qty),
                    "CumQty": int(order.cum),
                    "LeavesQty": int(order.leaves),
                    "AvgPx": order.avg_px,
                    "Status": order.status,
                })
    return raptor, algo, router


def canonical_rows(data: MockData) -> List[Dict[str, Any]]:
    """The python twin of :mod:`order_tree_recon.sources`' adapter views.

    Same role split (an algo / router row with a blank parent id is a parent), same
    renames, same FIX side decoding -- so the reference oracle sees exactly the rows the
    DAG's ``nodes`` table is built from.
    """
    rows: List[Dict[str, Any]] = []
    for r in data.raptor:
        rows.append({
            "Level": "CLIENT", "OrderId": r["OrderId"], "AltId": r["ClOrdID"], "LinkId": "",
            "Moniker": r["Moniker"], "Symbol": r["Symbol"], "Side": FIX_TO_SIDE.get(r["Side"], r["Side"]),
            "OrderQty": r["OrderQty"], "CumQty": r["CumQty"], "LeavesQty": r["LeavesQty"],
            "AvgPx": r["AvgPx"], "Status": r["OrdStatus"], "Destination": "",
        })
    for a in data.algo:
        is_parent = a["ParentAlgoOrderId"] == ""
        rows.append({
            "Level": "ALGO_PARENT" if is_parent else "ALGO_CHILD", "OrderId": a["AlgoOrderId"], "AltId": "",
            "LinkId": a["RaptorOrderId"] if is_parent else a["ParentAlgoOrderId"],
            "Moniker": "", "Symbol": a["Symbol"], "Side": a["Side"],
            "OrderQty": a["Qty"], "CumQty": a["Filled"], "LeavesQty": a["Open"],
            "AvgPx": a["AvgPrice"], "Status": a["State"], "Destination": a["Strategy"],
        })
    for o in data.router:
        is_parent = o["ParentRouterOrderId"] == ""
        rows.append({
            "Level": "OR_PARENT" if is_parent else "OR_CHILD", "OrderId": o["RouterOrderId"], "AltId": "",
            "LinkId": o["UpstreamOrderId"] if is_parent else o["ParentRouterOrderId"],
            "Moniker": "", "Symbol": o["Symbol"], "Side": o["Side"],
            "OrderQty": o["Qty"], "CumQty": o["CumQty"], "LeavesQty": o["LeavesQty"],
            "AvgPx": o["AvgPx"], "Status": o["Status"], "Destination": o["Venue"],
        })
    return rows
