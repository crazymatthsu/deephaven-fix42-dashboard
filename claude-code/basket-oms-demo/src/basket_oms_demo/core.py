"""The order core: baskets, orders, transitions, roll-ups (doc 13 §2-§4). Pure python.

No Deephaven import anywhere in this module. The core is the **single source of truth**;
the Deephaven input tables are its live projection, fed through the :class:`Listener`
protocol (``tables.TableBridge`` on the server, a recording listener in the tests).

Every mutation takes the core's lock, validates the transition, applies it, appends an
:class:`Event`, and only then notifies the listener -- so the projection always sees a
consistent state, and a fill landing from the venue thread while a trader's Modify
dialog is open is serialised against the trader's commit.
"""

from __future__ import annotations

import datetime as dt
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, FrozenSet, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple, Union

__all__ = [
    "SIDES",
    "ORD_TYPES",
    "TIFS",
    "STRATEGIES",
    "ORDER_STATUSES",
    "LIVE_STATUSES",
    "ROUTED_STATUSES",
    "TERMINAL_STATUSES",
    "BASKET_STATUSES",
    "EVENT_TYPES",
    "ACTIONS",
    "OmsError",
    "Basket",
    "Order",
    "Execution",
    "Event",
    "Quote",
    "OrderLine",
    "Rollup",
    "Listener",
    "parse_line",
    "parse_basket_lines",
    "split_quantities",
    "allowed_actions",
    "basket_rollup",
    "weighted_avg_px",
    "OmsCore",
]

SIDES = ("BUY", "SELL", "SHORT")
ORD_TYPES = ("MKT", "LMT")
TIFS = ("DAY", "IOC", "GTC")
STRATEGIES = ("DMA", "VWAP", "TWAP", "POV")
ORDER_STATUSES = ("NEW", "ROUTED", "WORKING", "PARTIAL", "FILLED", "CANCELLED", "REJECTED", "SPLIT")
#: Orders the trader can still act on.
LIVE_STATUSES: FrozenSet[str] = frozenset({"NEW", "ROUTED", "WORKING", "PARTIAL"})
#: Orders a venue is holding.
ROUTED_STATUSES: FrozenSet[str] = frozenset({"ROUTED", "WORKING", "PARTIAL"})
TERMINAL_STATUSES: FrozenSet[str] = frozenset({"FILLED", "CANCELLED", "REJECTED"})
BASKET_STATUSES = ("STAGED", "WORKING", "DONE", "CANCELLED")
EVENT_TYPES = ("CREATE", "ROUTE", "ACK", "MODIFY", "CANCEL", "SPLIT", "FILL", "REJECT", "BASKET")
#: Context-menu actions, in menu order.
ACTIONS = ("modify", "route", "split", "cancel")

_VENUE_USER = "venue"


class OmsError(ValueError):
    """An illegal transition or a malformed input; the message is shown to the trader."""


# --------------------------------------------------------------------------------------
# Records
# --------------------------------------------------------------------------------------


@dataclass
class Basket:
    basket_id: str
    name: str
    strategy: str
    trader: str
    created: dt.datetime
    updated: dt.datetime
    status: str = "STAGED"
    note: str = ""


@dataclass
class Order:
    order_id: str
    basket_id: str
    symbol: str
    side: str
    qty: int
    ord_type: str
    limit_px: Optional[float]
    tif: str
    trader: str
    created: dt.datetime
    updated: dt.datetime
    venue: Optional[str] = None
    status: str = "NEW"
    filled: int = 0
    leaves: int = 0
    avg_px: Optional[float] = None
    last_px: Optional[float] = None
    last_qty: Optional[int] = None
    parent_id: Optional[str] = None
    note: str = ""

    def summary(self) -> str:
        px = f" @ {self.limit_px:.2f}" if self.ord_type == "LMT" and self.limit_px is not None else ""
        return f"{self.order_id} {self.side} {self.qty:,} {self.symbol} {self.ord_type}{px} {self.tif}"


@dataclass
class Execution:
    exec_id: str
    time: dt.datetime
    order_id: str
    basket_id: str
    symbol: str
    side: str
    last_qty: int
    last_px: float
    venue: str


@dataclass
class Event:
    seq: int
    time: dt.datetime
    type: str
    order_id: Optional[str]
    basket_id: str
    detail: str
    user: str


@dataclass
class Quote:
    symbol: str
    bid: float
    ask: float
    last: float
    ref_px: float
    chg_pct: float
    updated: dt.datetime


@dataclass
class OrderLine:
    """One parsed ticket line: ``SYMBOL SIDE QTY [MKT|LMT price] [DAY|IOC|GTC]``."""

    symbol: str
    side: str
    qty: int
    ord_type: str = "MKT"
    limit_px: Optional[float] = None
    tif: str = "DAY"

    def text(self) -> str:
        px = f" {self.limit_px:.2f}" if self.ord_type == "LMT" and self.limit_px is not None else ""
        return f"{self.symbol} {self.side} {self.qty} {self.ord_type}{px} {self.tif}"


@dataclass
class Rollup:
    """Per-basket aggregate over its leaf orders (``SPLIT`` parents excluded)."""

    status: str = "STAGED"
    orders: int = 0
    live: int = 0
    done: int = 0
    qty: int = 0
    filled: int = 0
    leaves: int = 0
    pct_filled: float = 0.0


class Listener(Protocol):
    """What the core reports after each mutation (the Deephaven bridge implements it)."""

    def on_basket(self, basket: Basket) -> None: ...

    def on_order(self, order: Order) -> None: ...

    def on_execution(self, execution: Execution) -> None: ...

    def on_event(self, event: Event) -> None: ...

    def on_quote(self, quote: Quote) -> None: ...


class NullListener:
    def on_basket(self, basket: Basket) -> None:
        pass

    def on_order(self, order: Order) -> None:
        pass

    def on_execution(self, execution: Execution) -> None:
        pass

    def on_event(self, event: Event) -> None:
        pass

    def on_quote(self, quote: Quote) -> None:
        pass


# --------------------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------------------


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_line(text: str) -> OrderLine:
    """Parse one ticket line; raises :class:`OmsError` with a message naming the problem.

    Grammar: ``SYMBOL SIDE QTY [MKT | LMT price] [DAY | IOC | GTC]``, case-insensitive,
    tokens separated by whitespace or commas. ``B``/``S`` are accepted for ``BUY``/``SELL``.
    """
    tokens = [tok for tok in text.replace(",", " ").split() if tok]
    if len(tokens) < 3:
        raise OmsError("expected: SYMBOL SIDE QTY [MKT|LMT price] [DAY|IOC|GTC]")
    symbol = tokens[0].upper()
    if not symbol.isalnum() and not all(ch.isalnum() or ch in ".-" for ch in symbol):
        raise OmsError(f"bad symbol {tokens[0]!r}")
    side = tokens[1].upper()
    side = {"B": "BUY", "S": "SELL", "SS": "SHORT", "SHRT": "SHORT"}.get(side, side)
    if side not in SIDES:
        raise OmsError(f"side must be one of {', '.join(SIDES)} (got {tokens[1]!r})")
    try:
        qty = int(tokens[2].replace("_", ""))
    except ValueError as exc:
        raise OmsError(f"quantity must be a whole number (got {tokens[2]!r})") from exc
    if qty <= 0:
        raise OmsError("quantity must be > 0")
    ord_type, limit_px, tif = "MKT", None, "DAY"
    rest = [tok.upper() for tok in tokens[3:]]
    i = 0
    while i < len(rest):
        tok = rest[i]
        if tok == "MKT":
            ord_type, limit_px = "MKT", None
            i += 1
        elif tok == "LMT":
            if i + 1 >= len(rest):
                raise OmsError("LMT needs a price")
            try:
                limit_px = float(rest[i + 1])
            except ValueError as exc:
                raise OmsError(f"limit price must be a number (got {rest[i + 1]!r})") from exc
            if limit_px <= 0:
                raise OmsError("limit price must be > 0")
            ord_type = "LMT"
            i += 2
        elif tok in TIFS:
            tif = tok
            i += 1
        else:
            raise OmsError(f"unexpected token {tok!r}")
    return OrderLine(symbol=symbol, side=side, qty=qty, ord_type=ord_type, limit_px=limit_px, tif=tif)


def parse_basket_lines(
    lines: Union[str, Iterable[Union[str, OrderLine]]],
) -> Tuple[List[OrderLine], List[Tuple[int, str, str]]]:
    """Parse a paste box (or a list of lines / :class:`OrderLine`) -> ``(lines, errors)``.

    ``errors`` is ``[(line_number, text, message)]``; blank lines and ``#`` comments are
    skipped and do not count as errors. Already-parsed :class:`OrderLine` items pass
    through untouched.
    """
    if isinstance(lines, str):
        items: List[Union[str, OrderLine]] = lines.splitlines()
    else:
        items = list(lines)
    parsed: List[OrderLine] = []
    errors: List[Tuple[int, str, str]] = []
    for number, item in enumerate(items, start=1):
        if isinstance(item, OrderLine):
            parsed.append(item)
            continue
        text = item.split("#", 1)[0].strip()
        if not text:
            continue
        try:
            parsed.append(parse_line(text))
        except OmsError as exc:
            errors.append((number, text, str(exc)))
    return parsed, errors


def split_quantities(qty: int, slices: int) -> List[int]:
    """``qty`` split into ``slices`` parts whose sizes differ by at most 1 and sum to ``qty``."""
    if slices < 2:
        raise OmsError("a split needs at least 2 slices")
    if qty < slices:
        raise OmsError(f"cannot split {qty} shares into {slices} slices")
    base, extra = divmod(qty, slices)
    return [base + 1 if i < extra else base for i in range(slices)]


def allowed_actions(order: Order) -> FrozenSet[str]:
    """Which of :data:`ACTIONS` the trader may take on ``order`` right now (doc 13 §4)."""
    allowed = set()
    if order.status in LIVE_STATUSES:
        allowed.add("modify")
        allowed.add("cancel")
    if order.status == "NEW":
        allowed.add("route")
        if order.qty >= 2:
            allowed.add("split")
    return frozenset(allowed)


def weighted_avg_px(prev_avg: Optional[float], prev_qty: int, px: float, qty: int) -> float:
    if prev_qty <= 0 or prev_avg is None:
        return float(px)
    return (prev_avg * prev_qty + px * qty) / float(prev_qty + qty)


def basket_rollup(orders: Iterable[Order]) -> Rollup:
    """Aggregate a basket's leaf orders (``SPLIT`` parents excluded).

    Status: ``STAGED`` when every leaf is ``NEW`` (or there are none); ``DONE`` when every
    leaf is terminal and something filled; ``CANCELLED`` when every leaf is terminal and
    nothing filled; ``WORKING`` otherwise (something is in flight or still to be routed
    while other orders have finished).
    """
    leaves = [order for order in orders if order.status != "SPLIT"]
    roll = Rollup(orders=len(leaves))
    if not leaves:
        return roll
    roll.live = sum(1 for o in leaves if o.status in LIVE_STATUSES)
    roll.done = sum(1 for o in leaves if o.status in TERMINAL_STATUSES)
    roll.qty = sum(o.qty for o in leaves)
    roll.filled = sum(o.filled for o in leaves)
    roll.leaves = sum(o.leaves for o in leaves)
    roll.pct_filled = round(100.0 * roll.filled / roll.qty, 2) if roll.qty else 0.0
    if all(o.status == "NEW" for o in leaves):
        roll.status = "STAGED"
    elif all(o.status in TERMINAL_STATUSES for o in leaves):
        roll.status = "DONE" if roll.filled > 0 else "CANCELLED"
    else:
        roll.status = "WORKING"
    return roll


# --------------------------------------------------------------------------------------
# The core
# --------------------------------------------------------------------------------------


class OmsCore:
    """In-memory OMS: validated transitions, ids, roll-ups, an audit tape, a listener.

    Args:
        trader: default ``Trader`` / ``User`` when a call does not name one.
        venues: the routing targets (``OMS_VENUES``).
        clock: a callable returning an aware UTC ``datetime`` (tests inject a fixed one).
        listener: receives every changed record (the Deephaven bridge).
    """

    def __init__(
        self,
        trader: str = "trader1",
        venues: Sequence[str] = ("NYSE", "NASDAQ", "ARCA", "BATS", "DARK", "ALGO-VWAP", "ALGO-TWAP"),
        clock: Optional[Callable[[], dt.datetime]] = None,
        listener: Optional[Listener] = None,
    ) -> None:
        self.trader = trader
        self.venues: List[str] = [v.upper() for v in venues]
        self.clock: Callable[[], dt.datetime] = clock or _utcnow
        self.listener: Listener = listener or NullListener()
        self.lock = threading.RLock()
        self.baskets: Dict[str, Basket] = {}
        self.orders: Dict[str, Order] = {}
        self.executions: List[Execution] = []
        self.events: List[Event] = []
        self._basket_seq = 0
        self._order_seq = 0
        self._exec_seq = 0
        self._event_seq = 0

    # -- ids ----------------------------------------------------------------------------

    def _next_basket_id(self) -> str:
        self._basket_seq += 1
        return f"B-{self._basket_seq:04d}"

    def _next_order_id(self) -> str:
        self._order_seq += 1
        return f"O-{self._order_seq:06d}"

    def _next_exec_id(self) -> str:
        self._exec_seq += 1
        return f"X-{self._exec_seq:06d}"

    # -- lookups ------------------------------------------------------------------------

    def basket(self, basket_id: str) -> Basket:
        try:
            return self.baskets[basket_id]
        except KeyError:
            raise OmsError(f"unknown basket {basket_id!r}") from None

    def order(self, order_id: str) -> Order:
        try:
            return self.orders[order_id]
        except KeyError:
            raise OmsError(f"unknown order {order_id!r}") from None

    def orders_in(self, basket_id: str) -> List[Order]:
        return [o for o in self.orders.values() if o.basket_id == basket_id]

    def live_orders(self) -> List[Order]:
        """A snapshot of the orders a venue may act on (``ROUTED`` / ``WORKING`` / ``PARTIAL``)."""
        with self.lock:
            return [o for o in self.orders.values() if o.status in ROUTED_STATUSES]

    def rollup(self, basket_id: str) -> Rollup:
        with self.lock:
            return basket_rollup(self.orders_in(basket_id))

    def counts(self) -> Dict[str, int]:
        with self.lock:
            by_status: Dict[str, int] = {}
            for o in self.orders.values():
                by_status[o.status] = by_status.get(o.status, 0) + 1
            return {
                "baskets": len(self.baskets),
                "orders": len(self.orders),
                "executions": len(self.executions),
                "events": len(self.events),
                **{f"orders_{k}": v for k, v in sorted(by_status.items())},
            }

    # -- internals ----------------------------------------------------------------------

    def _emit(self, type_: str, basket_id: str, order_id: Optional[str], detail: str, user: str) -> Event:
        self._event_seq += 1
        event = Event(self._event_seq, self.clock(), type_, order_id, basket_id, detail, user)
        self.events.append(event)
        self.listener.on_event(event)
        return event

    def _publish_order(self, order: Order) -> None:
        order.updated = self.clock()
        self.listener.on_order(order)

    def _touch_basket(self, basket_id: str) -> Basket:
        basket = self.basket(basket_id)
        basket.status = basket_rollup(self.orders_in(basket_id)).status
        basket.updated = self.clock()
        self.listener.on_basket(basket)
        return basket

    def _check_venue(self, venue: str) -> str:
        name = (venue or "").strip().upper()
        if name not in self.venues:
            raise OmsError(f"unknown venue {venue!r}; choose one of {', '.join(self.venues)}")
        return name

    def _new_order(self, basket: Basket, line: OrderLine, trader: str, parent_id: Optional[str] = None) -> Order:
        now = self.clock()
        order = Order(
            order_id=self._next_order_id(),
            basket_id=basket.basket_id,
            symbol=line.symbol.upper(),
            side=line.side,
            qty=int(line.qty),
            ord_type=line.ord_type,
            limit_px=float(line.limit_px) if line.ord_type == "LMT" and line.limit_px is not None else None,
            tif=line.tif,
            trader=trader,
            created=now,
            updated=now,
            leaves=int(line.qty),
            parent_id=parent_id,
        )
        if order.ord_type == "LMT" and order.limit_px is None:
            raise OmsError("a LMT order needs a limit price")
        self.orders[order.order_id] = order
        return order

    @staticmethod
    def _lines(lines: Union[str, Iterable[Union[str, OrderLine]]]) -> List[OrderLine]:
        parsed, errors = parse_basket_lines(lines)
        if errors:
            first = errors[0]
            raise OmsError(f"line {first[0]} ({first[1]!r}): {first[2]}")
        if not parsed:
            raise OmsError("no order lines given")
        return parsed

    # -- baskets ------------------------------------------------------------------------

    def create_basket(
        self,
        name: str,
        lines: Union[str, Iterable[Union[str, OrderLine]]],
        strategy: str = "DMA",
        trader: Optional[str] = None,
        note: str = "",
    ) -> Basket:
        """Create a basket with its orders (all ``NEW``); raises on any bad line."""
        parsed = self._lines(lines)
        strategy = (strategy or "DMA").upper()
        if strategy not in STRATEGIES:
            raise OmsError(f"strategy must be one of {', '.join(STRATEGIES)}")
        clean_name = (name or "").strip()
        if not clean_name:
            raise OmsError("the basket needs a name")
        who = (trader or self.trader).strip() or self.trader
        with self.lock:
            now = self.clock()
            basket = Basket(self._next_basket_id(), clean_name, strategy, who, now, now, note=note)
            self.baskets[basket.basket_id] = basket
            self.listener.on_basket(basket)
            orders = [self._new_order(basket, line, who) for line in parsed]
            for order in orders:
                self._publish_order(order)
                self._emit("CREATE", basket.basket_id, order.order_id, order.summary(), who)
            self._emit("BASKET", basket.basket_id, None, f"created {clean_name} ({len(orders)} orders, {strategy})", who)
            self._touch_basket(basket.basket_id)
            return basket

    def add_orders(
        self,
        basket_id: str,
        lines: Union[str, Iterable[Union[str, OrderLine]]],
        trader: Optional[str] = None,
    ) -> List[Order]:
        parsed = self._lines(lines)
        who = (trader or self.trader).strip() or self.trader
        with self.lock:
            basket = self.basket(basket_id)
            orders = [self._new_order(basket, line, who) for line in parsed]
            for order in orders:
                self._publish_order(order)
                self._emit("CREATE", basket.basket_id, order.order_id, order.summary(), who)
            self._touch_basket(basket.basket_id)
            return orders

    def route_basket(self, basket_id: str, venue: str, user: Optional[str] = None) -> List[Order]:
        """Route every ``NEW`` order of the basket; returns the routed orders (may be empty)."""
        name = self._check_venue(venue)
        who = user or self.trader
        with self.lock:
            self.basket(basket_id)
            routed = [self._route(o, name, who) for o in self.orders_in(basket_id) if o.status == "NEW"]
            self._emit("BASKET", basket_id, None, f"routed {len(routed)} orders to {name}", who)
            self._touch_basket(basket_id)
            return routed

    def cancel_basket(self, basket_id: str, user: Optional[str] = None) -> List[Order]:
        """Cancel every cancellable order of the basket; returns the cancelled orders."""
        who = user or self.trader
        with self.lock:
            self.basket(basket_id)
            cancelled = [
                self._cancel(o, who, "basket cancel")
                for o in self.orders_in(basket_id)
                if "cancel" in allowed_actions(o)
            ]
            self._emit("BASKET", basket_id, None, f"cancelled {len(cancelled)} orders", who)
            self._touch_basket(basket_id)
            return cancelled

    # -- trader actions -----------------------------------------------------------------

    def _route(self, order: Order, venue: str, who: str) -> Order:
        if "route" not in allowed_actions(order):
            raise OmsError(f"{order.order_id} is {order.status}; only NEW orders can be routed")
        order.venue = venue
        order.status = "ROUTED"
        order.note = f"routed to {venue}"
        self._publish_order(order)
        self._emit("ROUTE", order.basket_id, order.order_id, f"-> {venue}", who)
        return order

    def route(self, order_id: str, venue: str, user: Optional[str] = None) -> Order:
        name = self._check_venue(venue)
        with self.lock:
            order = self._route(self.order(order_id), name, user or self.trader)
            self._touch_basket(order.basket_id)
            return order

    def modify(
        self,
        order_id: str,
        qty: Optional[int] = None,
        limit_px: Optional[float] = None,
        tif: Optional[str] = None,
        user: Optional[str] = None,
    ) -> Order:
        """Cancel/replace: new ``qty`` (>= filled), ``limit_px`` (LMT only), ``tif``."""
        with self.lock:
            order = self.order(order_id)
            if "modify" not in allowed_actions(order):
                raise OmsError(f"{order.order_id} is {order.status} and cannot be modified")
            changes: List[str] = []
            if qty is not None:
                new_qty = int(qty)
                if new_qty <= 0:
                    raise OmsError("quantity must be > 0")
                if new_qty < order.filled:
                    raise OmsError(f"quantity {new_qty:,} is below the filled quantity {order.filled:,}")
                if new_qty != order.qty:
                    changes.append(f"Qty {order.qty:,} -> {new_qty:,}")
                    order.qty = new_qty
            if limit_px is not None:
                if order.ord_type != "LMT":
                    raise OmsError("only a LMT order has a limit price")
                new_px = float(limit_px)
                if new_px <= 0:
                    raise OmsError("limit price must be > 0")
                if new_px != order.limit_px:
                    changes.append(f"Px {order.limit_px:.2f} -> {new_px:.2f}")
                    order.limit_px = new_px
            if tif is not None:
                new_tif = tif.upper()
                if new_tif not in TIFS:
                    raise OmsError(f"TIF must be one of {', '.join(TIFS)}")
                if new_tif != order.tif:
                    changes.append(f"TIF {order.tif} -> {new_tif}")
                    order.tif = new_tif
            if not changes:
                raise OmsError("nothing to modify")
            order.leaves = order.qty - order.filled
            if order.leaves == 0:
                order.status = "FILLED"
            order.note = "; ".join(changes)
            self._publish_order(order)
            self._emit("MODIFY", order.basket_id, order.order_id, order.note, user or self.trader)
            self._touch_basket(order.basket_id)
            return order

    def _cancel(self, order: Order, who: str, reason: str) -> Order:
        if "cancel" not in allowed_actions(order):
            raise OmsError(f"{order.order_id} is {order.status} and cannot be cancelled")
        order.status = "CANCELLED"
        order.leaves = 0
        order.note = reason
        self._publish_order(order)
        self._emit("CANCEL", order.basket_id, order.order_id, reason, who)
        return order

    def cancel(self, order_id: str, user: Optional[str] = None, reason: str = "cancelled by trader") -> Order:
        with self.lock:
            order = self._cancel(self.order(order_id), user or self.trader, reason)
            self._touch_basket(order.basket_id)
            return order

    def split(
        self,
        order_id: str,
        slices: Optional[int] = None,
        qtys: Optional[Sequence[int]] = None,
        user: Optional[str] = None,
    ) -> List[Order]:
        """Slice an unrouted order into children (``ParentId`` set); the parent becomes ``SPLIT``."""
        if (slices is None) == (qtys is None):
            raise OmsError("give either a slice count or a list of quantities")
        with self.lock:
            order = self.order(order_id)
            if "split" not in allowed_actions(order):
                raise OmsError(f"{order.order_id} is {order.status}; only unrouted NEW orders can be split")
            if qtys is not None:
                sizes = [int(q) for q in qtys]
                if len(sizes) < 2:
                    raise OmsError("a split needs at least 2 slices")
                if any(q <= 0 for q in sizes):
                    raise OmsError("every slice must be > 0")
                if sum(sizes) != order.qty:
                    raise OmsError(f"slices sum to {sum(sizes):,}, the order is {order.qty:,}")
            else:
                sizes = split_quantities(order.qty, int(slices))  # type: ignore[arg-type]
            basket = self.basket(order.basket_id)
            who = user or self.trader
            children: List[Order] = []
            for size in sizes:
                line = OrderLine(order.symbol, order.side, size, order.ord_type, order.limit_px, order.tif)
                child = self._new_order(basket, line, order.trader, parent_id=order.order_id)
                child.note = f"slice of {order.order_id}"
                children.append(child)
            order.status = "SPLIT"
            order.leaves = 0
            order.note = f"split into {len(children)}: " + ", ".join(str(s) for s in sizes)
            self._publish_order(order)
            for child in children:
                self._publish_order(child)
                self._emit("CREATE", basket.basket_id, child.order_id, child.summary() + f" (slice of {order.order_id})", who)
            self._emit("SPLIT", basket.basket_id, order.order_id, order.note, who)
            self._touch_basket(basket.basket_id)
            return children

    # -- venue side ---------------------------------------------------------------------

    def ack(self, order_id: str) -> Order:
        with self.lock:
            order = self.order(order_id)
            if order.status != "ROUTED":
                raise OmsError(f"{order.order_id} is {order.status}; only ROUTED orders are acked")
            order.status = "WORKING"
            order.note = f"working at {order.venue}"
            self._publish_order(order)
            self._emit("ACK", order.basket_id, order.order_id, f"acked by {order.venue}", _VENUE_USER)
            self._touch_basket(order.basket_id)
            return order

    def reject(self, order_id: str, reason: str) -> Order:
        with self.lock:
            order = self.order(order_id)
            if order.status not in ROUTED_STATUSES:
                raise OmsError(f"{order.order_id} is {order.status}; not at a venue")
            order.status = "REJECTED"
            order.leaves = 0
            order.note = reason
            self._publish_order(order)
            self._emit("REJECT", order.basket_id, order.order_id, reason, _VENUE_USER)
            self._touch_basket(order.basket_id)
            return order

    def fill(self, order_id: str, qty: int, px: float) -> Tuple[Order, Execution]:
        """A (partial) execution from the venue; ``qty`` is clipped to ``leaves``."""
        with self.lock:
            order = self.order(order_id)
            if order.status not in ("WORKING", "PARTIAL"):
                raise OmsError(f"{order.order_id} is {order.status}; only WORKING/PARTIAL orders fill")
            fill_qty = min(int(qty), order.leaves)
            if fill_qty <= 0:
                raise OmsError("fill quantity must be > 0")
            if px <= 0:
                raise OmsError("fill price must be > 0")
            order.avg_px = weighted_avg_px(order.avg_px, order.filled, float(px), fill_qty)
            order.filled += fill_qty
            order.leaves = order.qty - order.filled
            order.last_px = float(px)
            order.last_qty = fill_qty
            order.status = "FILLED" if order.leaves == 0 else "PARTIAL"
            order.note = f"{order.filled:,}/{order.qty:,} @ {order.avg_px:.4f}"
            execution = Execution(
                self._next_exec_id(),
                self.clock(),
                order.order_id,
                order.basket_id,
                order.symbol,
                order.side,
                fill_qty,
                float(px),
                order.venue or "",
            )
            self.executions.append(execution)
            self._publish_order(order)
            self.listener.on_execution(execution)
            self._emit("FILL", order.basket_id, order.order_id, f"{fill_qty:,} @ {float(px):.2f} ({order.status})", _VENUE_USER)
            self._touch_basket(order.basket_id)
            return order, execution

    def venue_cancel(self, order_id: str, reason: str) -> Order:
        """The venue cancels the remainder (IOC after one shot, end of day)."""
        with self.lock:
            order = self._cancel(self.order(order_id), _VENUE_USER, reason)
            self._touch_basket(order.basket_id)
            return order

    def publish_quote(self, quote: Quote) -> None:
        self.listener.on_quote(quote)

    # -- bulk replay --------------------------------------------------------------------

    def republish(self) -> None:
        """Push every record to the listener again (after attaching a new bridge)."""
        with self.lock:
            for basket in self.baskets.values():
                self.listener.on_basket(basket)
            for order in self.orders.values():
                self.listener.on_order(order)
            for execution in self.executions:
                self.listener.on_execution(execution)
            for event in self.events:
                self.listener.on_event(event)
