"""The FIX 4.2 order state machine (docs 01 §3/§5/§6/§7, doc 05 §3).

Pure python, stdlib only, **no Deephaven imports** -- the Deephaven layer feeds
raw strings in and publishes the returned rows.

The machine is a stateful fold over the message stream:

* identity resolution (§3) keeps one stable ``OrderKey`` per order chain and
  binds every identifier seen (37/11/41/17) to it, idempotently;
* transitions (§5) apply venue truth: ``OrdStatus`` always comes from tag 39 on
  execution reports and 14/151/6 are adopted as absolute snapshots;
* every message yields an audit row, a post-message state snapshot, and 0..n
  execution/event rows (§6).

It is single-threaded by design: Deephaven delivers update-graph rows serially.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

from .fixtags import (
    CxlRejResponseTo,
    ExecTransType,
    ExecType,
    OrdStatus,
    OrdType,
    Side,
    Tag,
    TimeInForce,
)
from .model import (
    EventType,
    ExecutionRow,
    FillStatus,
    MessageRow,
    OrderEventRow,
    OrderState,
    PendingAction,
)
from .parser import parse_fix, parse_transact_time, render_fields

__all__ = ["Result", "OrderStateMachine", "HANDLED_MSG_TYPES", "utcnow"]

#: The message types this project handles (doc 01, "In scope").
HANDLED_MSG_TYPES = frozenset({"D", "G", "F", "8", "9", "Q"})

#: Sentinel a venue may send for tag 37 on a `9` whose target was never acked.
_ORDER_ID_NONE = "NONE"

#: 150 ExecType -> EventType (doc 01 §6); ExecTransType 1/2 override this.
_EXEC_TYPE_EVENTS: dict[ExecType, str] = {
    ExecType.NEW: EventType.NEW_ACK,
    ExecType.PENDING_NEW: EventType.PENDING_NEW,
    ExecType.PARTIAL_FILL: EventType.PARTIAL_FILL,
    ExecType.FILL: EventType.FULL_FILL,
    ExecType.CANCELED: EventType.CANCEL_ACK,
    ExecType.REPLACED: EventType.AMEND_ACK,
    ExecType.PENDING_CANCEL: EventType.PENDING_CANCEL,
    ExecType.PENDING_REPLACE: EventType.PENDING_AMEND,
    ExecType.REJECTED: EventType.NEW_REJECT,
    ExecType.RESTATED: EventType.RESTATED,
    ExecType.EXPIRED: EventType.EXPIRED,
    ExecType.DONE_FOR_DAY: EventType.DONE_FOR_DAY,
}


def utcnow() -> datetime:
    """Default ingest clock: timezone-aware UTC now."""
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# field helpers
# --------------------------------------------------------------------------- #


def _text(fields: dict[int, str], tag: int) -> str:
    """Read a string tag, defaulting to ``""``."""
    return fields.get(int(tag), "")


def _number(fields: dict[int, str], tag: int) -> float | None:
    """Read a numeric tag; ``None`` when absent or unparseable."""
    raw = fields.get(int(tag))
    if raw is None:
        return None
    try:
        return float(raw.strip())
    except ValueError:
        return None


def _enum(enum_cls: type, fields: dict[int, str], tag: int):
    """Map a tag through its enum, or ``None`` when the tag is absent."""
    raw = fields.get(int(tag))
    if raw is None:
        return None
    return enum_cls.from_fix(raw)


def _order_id_of(fields: dict[int, str]) -> str:
    """Tag 37, treating the ``NONE`` sentinel (doc 01 §2) as absent."""
    value = _text(fields, Tag.ORDER_ID)
    return "" if value == _ORDER_ID_NONE else value


def _num(value: float) -> str:
    """Compact number rendering for human-readable ``Detail`` strings."""
    if value == int(value):
        return str(int(value))
    return f"{value:.6g}"


# --------------------------------------------------------------------------- #
# internal per-chain bookkeeping (never published)
# --------------------------------------------------------------------------- #


@dataclass
class _StagedTerms:
    """Terms proposed by a `G` -- applied only when a ``150=5`` confirms it."""

    order_qty: float | None = None
    price: float | None = None
    stop_px: float | None = None
    time_in_force: TimeInForce | None = None


@dataclass
class _PendingRequest:
    """An in-flight `F`/`G` request awaiting an `8` or `9`."""

    action: str
    clordid: str
    terms: _StagedTerms | None = None


@dataclass
class _Chain:
    """All machine state for one order chain."""

    state: OrderState
    #: ExecIDs seen as tag 17 on a `35=8` -- drives dedupe and ``ExecCount``.
    exec_ids: set[str] = field(default_factory=set)
    #: ExecID -> latest emitted row, so bust/correct/DK can re-emit it.
    exec_rows: dict[str, ExecutionRow] = field(default_factory=dict)
    #: In-flight F/G requests, oldest first.
    pending: list[_PendingRequest] = field(default_factory=list)
    #: Request ClOrdID -> OrdStatus captured when that request was sent (§5.5).
    prior_status: dict[str, OrdStatus | None] = field(default_factory=dict)
    #: A `D` is outstanding until the venue responds with a non-PendingNew `8`.
    new_pending: bool = False


@dataclass
class Result:
    """Everything the Deephaven layer publishes for one input message.

    On an unparseable / unsupported / unresolvable message ``error`` is set,
    ``state`` is ``None`` and the row lists are empty; ``message`` is still
    populated whenever the input was parseable enough to audit.
    """

    state: OrderState | None = None
    executions: list[ExecutionRow] = field(default_factory=list)
    events: list[OrderEventRow] = field(default_factory=list)
    message: MessageRow | None = None
    error: str | None = None


class OrderStateMachine:
    """Stateful FIX 4.2 order cache.

    Feed it raw FIX strings with :meth:`process`; each call returns a
    :class:`Result` whose ``state`` is an immutable snapshot of the affected
    order chain.  :meth:`process` never raises.
    """

    def __init__(self, now_fn: Callable[[], datetime] = utcnow) -> None:
        self._now = now_fn
        self._chains: dict[str, _Chain] = {}
        #: Identifier -> OrderKey binding tables (doc 01 §3).
        self.key_by_order_id: dict[str, str] = {}
        self.key_by_clordid: dict[str, str] = {}
        self.key_by_execid: dict[str, str] = {}

    # ----------------------------------------------------------------- #
    # public API
    # ----------------------------------------------------------------- #

    def process(self, raw: str) -> Result:
        """Parse and apply one raw FIX message.  Never raises."""
        try:
            fields = parse_fix(raw)
        except Exception as exc:  # pragma: no cover - parse_fix is total
            return Result(error=f"unparseable: {type(exc).__name__}: {exc}")
        if not fields:
            return Result(error="unparseable: no FIX fields found")
        return self.process_fields(fields, raw=raw)

    def process_fields(self, f: dict[int, str], raw: str | None = None) -> Result:
        """Apply one already-parsed message.  Never raises.

        ``raw`` is the original wire string for the audit row; when omitted the
        fields are re-rendered pipe-delimited.
        """
        try:
            return self._process_fields(f, raw)
        except Exception as exc:  # pragma: no cover - defensive
            return Result(error=f"internal error: {type(exc).__name__}: {exc}")

    def get_by_order_id(self, order_id: str) -> OrderState | None:
        """Look up an order chain by venue OrderID (tag 37)."""
        return self._snapshot(self.key_by_order_id.get(order_id, ""))

    def get_by_clordid(self, clordid: str) -> OrderState | None:
        """Look up an order chain by any ClOrdID it has ever carried (11/41)."""
        return self._snapshot(self.key_by_clordid.get(clordid, ""))

    def get_by_execid(self, exec_id: str) -> OrderState | None:
        """Look up an order chain by any ExecID bound to it (tag 17)."""
        return self._snapshot(self.key_by_execid.get(exec_id, ""))

    def get_by_key(self, order_key: str) -> OrderState | None:
        """Look up an order chain by its OrderKey."""
        return self._snapshot(order_key)

    def find_by_account(self, account: str) -> list[OrderState]:
        """All order chains for an account (tag 1)."""
        return [
            chain.state.copy()
            for chain in self._chains.values()
            if chain.state.account == account
        ]

    def find_by_symbol(self, symbol: str) -> list[OrderState]:
        """All order chains for a symbol (tag 55)."""
        return [
            chain.state.copy()
            for chain in self._chains.values()
            if chain.state.symbol == symbol
        ]

    def order_count(self) -> int:
        """Number of distinct order chains held."""
        return len(self._chains)

    def snapshot_all(self) -> list[OrderState]:
        """Snapshots of every order chain held (test/debug convenience)."""
        return [chain.state.copy() for chain in self._chains.values()]

    # ----------------------------------------------------------------- #
    # dispatch
    # ----------------------------------------------------------------- #

    def _process_fields(self, f: dict[int, str], raw: str | None) -> Result:
        ingest_ts = self._now()
        raw_text = raw if raw is not None else render_fields(f)
        msg_type = _text(f, Tag.MSG_TYPE)

        if msg_type not in HANDLED_MSG_TYPES:
            # Audit it, attribute it if we can, but change nothing.
            key, _ = self._resolve(f, allow_create=False)
            error = (
                "missing MsgType (tag 35)"
                if not msg_type
                else f"unsupported MsgType: {msg_type!r}"
            )
            return Result(
                message=MessageRow.from_fields(f, key, raw_text, ingest_ts),
                error=error,
            )

        key, _ = self._resolve(f, allow_create=True)
        if not key:
            return Result(
                message=MessageRow.from_fields(f, "", raw_text, ingest_ts),
                error="unresolvable: no OrderID/ClOrdID/OrigClOrdID binding",
            )

        self._bind(key, f)
        chain = self._chains.get(key)
        created = chain is None
        if chain is None:
            chain = _Chain(state=OrderState(order_key=key, first_seen_ts=ingest_ts))
            self._chains[key] = chain

        state = chain.state
        state.msg_count += 1
        state.last_msg_type = msg_type
        state.last_update_ts = ingest_ts
        if state.first_seen_ts is None:
            state.first_seen_ts = ingest_ts

        order_id = _order_id_of(f)
        if order_id:
            state.order_id = order_id
        orig_clordid = _text(f, Tag.ORIG_CL_ORD_ID)
        if orig_clordid:
            state.orig_clordid = orig_clordid

        if msg_type == "D":
            executions, events = self._handle_new_order(f, chain, created, ingest_ts)
        elif msg_type == "G":
            executions, events = self._handle_replace_request(
                f, chain, created, ingest_ts
            )
        elif msg_type == "F":
            executions, events = self._handle_cancel_request(
                f, chain, created, ingest_ts
            )
        elif msg_type == "8":
            executions, events = self._handle_execution_report(
                f, chain, created, ingest_ts
            )
        elif msg_type == "9":
            executions, events = self._handle_cancel_reject(f, chain, ingest_ts)
        else:  # "Q"
            executions, events = self._handle_dont_know_trade(f, chain, ingest_ts)

        return Result(
            state=state.copy(),
            executions=executions,
            events=events,
            message=MessageRow.from_fields(f, key, raw_text, ingest_ts),
        )

    # ----------------------------------------------------------------- #
    # identity resolution (doc 01 §3)
    # ----------------------------------------------------------------- #

    def _resolve(self, f: dict[int, str], *, allow_create: bool) -> tuple[str, bool]:
        """Resolve the OrderKey for a message.

        Priority: 37 OrderID, then 11 ClOrdID, then 41 OrigClOrdID, then (for
        `Q`, doc 01 §5.6) 17 ExecID.  When nothing is bound yet a new chain is
        keyed by OrderID if present, else ClOrdID, else OrigClOrdID -- an ExecID
        is never itself an OrderKey.  Returns ``("", False)`` when the message
        carries no usable identifier.
        """
        order_id = _order_id_of(f)
        clordid = _text(f, Tag.CL_ORD_ID)
        orig_clordid = _text(f, Tag.ORIG_CL_ORD_ID)
        exec_id = _text(f, Tag.EXEC_ID)

        if order_id and order_id in self.key_by_order_id:
            return self.key_by_order_id[order_id], False
        if clordid and clordid in self.key_by_clordid:
            return self.key_by_clordid[clordid], False
        if orig_clordid and orig_clordid in self.key_by_clordid:
            return self.key_by_clordid[orig_clordid], False
        if exec_id and exec_id in self.key_by_execid:
            return self.key_by_execid[exec_id], False

        if not allow_create:
            return "", False
        key = order_id or clordid or orig_clordid
        if not key:
            return "", False
        return key, True

    def _bind(self, key: str, f: dict[int, str]) -> None:
        """Bind every identifier present to ``key`` (idempotent; first wins)."""
        order_id = _order_id_of(f)
        if order_id:
            self.key_by_order_id.setdefault(order_id, key)
        for clordid in (_text(f, Tag.CL_ORD_ID), _text(f, Tag.ORIG_CL_ORD_ID)):
            if clordid:
                self.key_by_clordid.setdefault(clordid, key)
        exec_id = _text(f, Tag.EXEC_ID)
        if exec_id:
            self.key_by_execid.setdefault(exec_id, key)

    def _snapshot(self, key: str) -> OrderState | None:
        chain = self._chains.get(key) if key else None
        return chain.state.copy() if chain is not None else None

    # ----------------------------------------------------------------- #
    # shared state helpers
    # ----------------------------------------------------------------- #

    def _apply_terms(
        self, state: OrderState, f: dict[int, str], *, only_if_empty: bool
    ) -> None:
        """Copy order terms from a message onto the state.

        With ``only_if_empty`` set, existing values are preserved -- used when a
        late `D` merges into a chain the venue already created (edge case 9).
        """
        account = _text(f, Tag.ACCOUNT)
        if account and (not only_if_empty or not state.account):
            state.account = account
        symbol = _text(f, Tag.SYMBOL)
        if symbol and (not only_if_empty or not state.symbol):
            state.symbol = symbol
        side = _enum(Side, f, Tag.SIDE)
        if side is not None and (not only_if_empty or state.side is None):
            state.side = side
        ord_type = _enum(OrdType, f, Tag.ORD_TYPE)
        if ord_type is not None and (not only_if_empty or state.ord_type is None):
            state.ord_type = ord_type
        tif = _enum(TimeInForce, f, Tag.TIME_IN_FORCE)
        if tif is not None and (not only_if_empty or state.time_in_force is None):
            state.time_in_force = tif
        order_qty = _number(f, Tag.ORDER_QTY)
        if order_qty is not None and (not only_if_empty or state.order_qty == 0.0):
            state.order_qty = order_qty
        price = _number(f, Tag.PRICE)
        if price is not None and (not only_if_empty or state.price == 0.0):
            state.price = price
        stop_px = _number(f, Tag.STOP_PX)
        if stop_px is not None and (not only_if_empty or state.stop_px == 0.0):
            state.stop_px = stop_px

    def _note_clordid(self, state: OrderState, clordid: str) -> None:
        """Record an identity-carrying ClOrdID in the chain history.

        Only the original `D` id and ids adopted by a ``150=5`` rotation belong
        here -- cancel-request ids never become the order's identity.
        """
        if clordid and clordid not in state.clordid_chain:
            state.clordid_chain.append(clordid)

    def _recompute_pending(self, chain: _Chain) -> None:
        """Derive ``PendingAction``/``PendingClOrdID`` from in-flight requests."""
        state = chain.state
        if chain.pending:
            latest = chain.pending[-1]
            state.pending_action = latest.action
            state.pending_clordid = latest.clordid
        elif chain.new_pending:
            state.pending_action = PendingAction.NEW
            state.pending_clordid = ""
        else:
            state.pending_action = PendingAction.NONE
            state.pending_clordid = ""

    def _clear_pending(
        self, chain: _Chain, action: str, clordid: str
    ) -> _PendingRequest | None:
        """Remove one in-flight request, preferring an exact ClOrdID match."""
        if clordid:
            for index in range(len(chain.pending) - 1, -1, -1):
                request = chain.pending[index]
                if request.action == action and request.clordid == clordid:
                    return chain.pending.pop(index)
        for index in range(len(chain.pending) - 1, -1, -1):
            if chain.pending[index].action == action:
                return chain.pending.pop(index)
        return None

    def _register_request(
        self,
        chain: _Chain,
        action: str,
        clordid: str,
        terms: _StagedTerms | None,
    ) -> None:
        """Snapshot the prior status and record an in-flight `F`/`G`."""
        chain.prior_status[clordid] = chain.state.ord_status
        chain.pending.append(_PendingRequest(action=action, clordid=clordid, terms=terms))

    # ----------------------------------------------------------------- #
    # 35=D  NewOrderSingle (rule 1; edge case 9)
    # ----------------------------------------------------------------- #

    def _handle_new_order(
        self,
        f: dict[int, str],
        chain: _Chain,
        created: bool,
        ingest_ts: datetime,
    ) -> tuple[list[ExecutionRow], list[OrderEventRow]]:
        state = chain.state
        clordid = _text(f, Tag.CL_ORD_ID)

        if created:
            state.clordid = clordid
            state.root_clordid = clordid
            self._note_clordid(state, clordid)
            self._apply_terms(state, f, only_if_empty=False)
            state.ord_status = OrdStatus.PENDING_NEW
            state.cum_qty = 0.0
            state.leaves_qty = state.order_qty
            state.avg_px = 0.0
            chain.new_pending = True
        else:
            # Late `D` for a chain the venue already opened: fill gaps only,
            # never clobber venue state (edge case 9).
            if not state.clordid:
                state.clordid = clordid
            if not state.root_clordid:
                state.root_clordid = clordid
            self._note_clordid(state, clordid)
            self._apply_terms(state, f, only_if_empty=True)
        self._recompute_pending(chain)

        detail = "new order request: {side} {qty} {symbol} {ord_type}{price}".format(
            side=state.side.name if state.side else "",
            qty=_num(_number(f, Tag.ORDER_QTY) or state.order_qty),
            symbol=state.symbol,
            ord_type=state.ord_type.name if state.ord_type else "",
            price=(
                f" @ {_num(_number(f, Tag.PRICE) or state.price)}"
                if (_number(f, Tag.PRICE) or state.price)
                else ""
            ),
        )
        event = self._build_event(
            chain,
            f,
            event_type=EventType.NEW_REQUEST,
            msg_type="D",
            order_qty=_number(f, Tag.ORDER_QTY),
            price=_number(f, Tag.PRICE),
            detail=detail,
            ingest_ts=ingest_ts,
            clordid=clordid or state.clordid,
        )
        return [], [event]

    # ----------------------------------------------------------------- #
    # 35=G  OrderCancelReplaceRequest (rule 3)
    # ----------------------------------------------------------------- #

    def _handle_replace_request(
        self,
        f: dict[int, str],
        chain: _Chain,
        created: bool,
        ingest_ts: datetime,
    ) -> tuple[list[ExecutionRow], list[OrderEventRow]]:
        state = chain.state
        clordid = _text(f, Tag.CL_ORD_ID)
        orig_clordid = _text(f, Tag.ORIG_CL_ORD_ID)

        if created:
            # Mid-stream start: the order we are amending was never seen.
            state.clordid = orig_clordid or clordid
            state.root_clordid = state.clordid
            self._note_clordid(state, state.clordid)
            self._apply_terms(state, f, only_if_empty=False)

        terms = _StagedTerms(
            order_qty=_number(f, Tag.ORDER_QTY),
            price=_number(f, Tag.PRICE),
            stop_px=_number(f, Tag.STOP_PX),
            time_in_force=_enum(TimeInForce, f, Tag.TIME_IN_FORCE),
        )
        self._register_request(chain, PendingAction.REPLACE, clordid, terms)
        state.ord_status = OrdStatus.PENDING_REPLACE
        self._recompute_pending(chain)

        proposed_qty = terms.order_qty if terms.order_qty is not None else state.order_qty
        proposed_price = terms.price if terms.price is not None else state.price
        detail = (
            f"amend request {orig_clordid or state.clordid} -> {clordid}: "
            f"qty {_num(state.order_qty)} -> {_num(proposed_qty)}, "
            f"price {_num(state.price)} -> {_num(proposed_price)}"
        )
        event = self._build_event(
            chain,
            f,
            event_type=EventType.AMEND_REQUEST,
            msg_type="G",
            order_qty=proposed_qty,
            price=proposed_price,
            detail=detail,
            ingest_ts=ingest_ts,
            clordid=clordid,
        )
        return [], [event]

    # ----------------------------------------------------------------- #
    # 35=F  OrderCancelRequest (rule 4)
    # ----------------------------------------------------------------- #

    def _handle_cancel_request(
        self,
        f: dict[int, str],
        chain: _Chain,
        created: bool,
        ingest_ts: datetime,
    ) -> tuple[list[ExecutionRow], list[OrderEventRow]]:
        state = chain.state
        clordid = _text(f, Tag.CL_ORD_ID)
        orig_clordid = _text(f, Tag.ORIG_CL_ORD_ID)

        if created:
            state.clordid = orig_clordid or clordid
            state.root_clordid = state.clordid
            self._note_clordid(state, state.clordid)
            self._apply_terms(state, f, only_if_empty=False)

        self._register_request(chain, PendingAction.CANCEL, clordid, None)
        state.ord_status = OrdStatus.PENDING_CANCEL
        self._recompute_pending(chain)

        detail = (
            f"cancel request {orig_clordid or state.clordid} -> {clordid}: "
            f"leaves {_num(state.leaves_qty)}"
        )
        event = self._build_event(
            chain,
            f,
            event_type=EventType.CANCEL_REQUEST,
            msg_type="F",
            order_qty=_number(f, Tag.ORDER_QTY),
            price=_number(f, Tag.PRICE),
            detail=detail,
            ingest_ts=ingest_ts,
            clordid=clordid,
        )
        return [], [event]

    # ----------------------------------------------------------------- #
    # 35=8  ExecutionReport (rule 2; edge cases 3, 6, 7, 9, 10, 11, 12)
    # ----------------------------------------------------------------- #

    def _handle_execution_report(
        self,
        f: dict[int, str],
        chain: _Chain,
        created: bool,
        ingest_ts: datetime,
    ) -> tuple[list[ExecutionRow], list[OrderEventRow]]:
        state = chain.state
        exec_id = _text(f, Tag.EXEC_ID)
        exec_ref_id = _text(f, Tag.EXEC_REF_ID)
        trans_type = _enum(ExecTransType, f, Tag.EXEC_TRANS_TYPE)
        exec_type = _enum(ExecType, f, Tag.EXEC_TYPE)
        status = _enum(OrdStatus, f, Tag.ORD_STATUS)

        is_bust = trans_type is ExecTransType.CANCEL
        is_correct = trans_type is ExecTransType.CORRECT
        is_new_trans = trans_type is None or trans_type is ExecTransType.NEW

        cum_qty = _number(f, Tag.CUM_QTY)
        duplicate = bool(exec_id) and exec_id in chain.exec_ids
        # Stale guard (rule 2, edge case 12): a non-bust/correct report whose
        # CumQty went backwards carries no economic truth.
        stale = (
            not duplicate
            and not is_bust
            and not is_correct
            and cum_qty is not None
            and cum_qty < state.cum_qty
        )

        if created:
            # Audit feeds can start mid-stream: seed identity from the report.
            clordid = _text(f, Tag.CL_ORD_ID)
            state.clordid = clordid
            state.root_clordid = clordid
            self._note_clordid(state, clordid)
        # Venue reports echo the order terms: fill gaps only, never clobber the
        # client's terms (staged `G` terms are applied by the 150=5 branch).
        self._apply_terms(state, f, only_if_empty=True)

        if duplicate:
            # Replay guard (edge case 3): bind + count only, no economics and no
            # new lifecycle event; re-emit the stored row so the executions
            # table keeps this ExecID's current disposition.
            stored = chain.exec_rows.get(exec_id)
            row = (
                replace(stored, ingest_ts=ingest_ts)
                if stored is not None
                else self._build_exec_row(
                    chain, f, exec_id, exec_ref_id, trans_type, exec_type, status,
                    is_new_trans, ingest_ts,
                )
            )
            return [row], []

        if exec_id:
            chain.exec_ids.add(exec_id)
            state.exec_count = len(chain.exec_ids)

        if status is not None:
            state.ord_status = status
        if exec_type is not None:
            state.last_exec_type = exec_type
        if Tag.TEXT in f:
            state.text = f[int(Tag.TEXT)]
        if Tag.ORD_REJ_REASON in f:
            state.ord_rej_reason = f[int(Tag.ORD_REJ_REASON)]

        if not stale:
            leaves_qty = _number(f, Tag.LEAVES_QTY)
            avg_px = _number(f, Tag.AVG_PX)
            if cum_qty is not None:
                state.cum_qty = cum_qty
            if leaves_qty is not None:
                state.leaves_qty = leaves_qty
            if avg_px is not None:
                state.avg_px = avg_px
            if is_new_trans and exec_type in (ExecType.PARTIAL_FILL, ExecType.FILL):
                last_shares = _number(f, Tag.LAST_SHARES)
                last_px = _number(f, Tag.LAST_PX)
                last_mkt = _text(f, Tag.LAST_MKT)
                if last_shares is not None:
                    state.last_shares = last_shares
                if last_px is not None:
                    state.last_px = last_px
                if last_mkt:
                    state.last_mkt = last_mkt
            self._resolve_pending_from_exec(chain, f, exec_type, status)

        primary = self._build_exec_row(
            chain, f, exec_id, exec_ref_id, trans_type, exec_type, status,
            is_new_trans, ingest_ts,
        )
        if exec_id:
            chain.exec_rows[exec_id] = primary
        executions = [primary]

        if (is_bust or is_correct) and exec_ref_id:
            executions.append(
                self._reemit_reference(
                    chain,
                    exec_ref_id,
                    FillStatus.BUSTED if is_bust else FillStatus.CORRECTED,
                    ingest_ts,
                    transact_time=parse_transact_time(f.get(int(Tag.TRANSACT_TIME))),
                    corrected_from=f if is_correct else None,
                )
            )

        event = self._exec_event(chain, f, trans_type, exec_type, ingest_ts)
        return executions, [event]

    def _resolve_pending_from_exec(
        self,
        chain: _Chain,
        f: dict[int, str],
        exec_type: ExecType | None,
        status: OrdStatus | None,
    ) -> None:
        """Clear/apply pending requests that this report resolves (rule 2)."""
        state = chain.state
        clordid = _text(f, Tag.CL_ORD_ID)

        # Any venue response other than PendingNew resolves the local `D` wait.
        if exec_type is not ExecType.PENDING_NEW:
            chain.new_pending = False

        if exec_type is ExecType.CANCELED or status is OrdStatus.CANCELED:
            self._clear_pending(chain, PendingAction.CANCEL, clordid)

        if exec_type is ExecType.REPLACED:
            request = self._clear_pending(chain, PendingAction.REPLACE, clordid)
            if request is not None and request.terms is not None:
                terms = request.terms
                if terms.order_qty is not None:
                    state.order_qty = terms.order_qty
                if terms.price is not None:
                    state.price = terms.price
                if terms.stop_px is not None:
                    state.stop_px = terms.stop_px
                if terms.time_in_force is not None:
                    state.time_in_force = terms.time_in_force
            new_clordid = clordid or (request.clordid if request is not None else "")
            if new_clordid:
                state.clordid = new_clordid
                self._note_clordid(state, new_clordid)

        self._recompute_pending(chain)

    def _build_exec_row(
        self,
        chain: _Chain,
        f: dict[int, str],
        exec_id: str,
        exec_ref_id: str,
        trans_type: ExecTransType | None,
        exec_type: ExecType | None,
        status: OrdStatus | None,
        is_new_trans: bool,
        ingest_ts: datetime,
    ) -> ExecutionRow:
        """Build the executions row for the report itself."""
        state = chain.state
        cum_qty = _number(f, Tag.CUM_QTY)
        leaves_qty = _number(f, Tag.LEAVES_QTY)
        avg_px = _number(f, Tag.AVG_PX)
        last_shares = _number(f, Tag.LAST_SHARES)
        last_px = _number(f, Tag.LAST_PX)
        return ExecutionRow(
            order_key=state.order_key,
            order_id=state.order_id,
            clordid=_text(f, Tag.CL_ORD_ID) or state.clordid,
            exec_id=exec_id,
            exec_ref_id=exec_ref_id,
            exec_trans_type=trans_type,
            exec_type=exec_type,
            ord_status=status if status is not None else state.ord_status,
            last_shares=last_shares if last_shares is not None else 0.0,
            last_px=last_px if last_px is not None else 0.0,
            cum_qty=cum_qty if cum_qty is not None else state.cum_qty,
            leaves_qty=leaves_qty if leaves_qty is not None else state.leaves_qty,
            avg_px=avg_px if avg_px is not None else state.avg_px,
            last_mkt=_text(f, Tag.LAST_MKT),
            text=_text(f, Tag.TEXT),
            is_fill=is_new_trans
            and exec_type in (ExecType.PARTIAL_FILL, ExecType.FILL),
            fill_status=FillStatus.NORMAL,
            transact_time=parse_transact_time(f.get(int(Tag.TRANSACT_TIME))),
            ingest_ts=ingest_ts,
        )

    def _reemit_reference(
        self,
        chain: _Chain,
        ref_exec_id: str,
        disposition: str,
        ingest_ts: datetime,
        transact_time: datetime | None = None,
        corrected_from: dict[int, str] | None = None,
    ) -> ExecutionRow:
        """Re-emit a referenced execution with its new disposition.

        Same ``ExecID``, updated ``FillStatus`` and current order values, so a
        downstream ``last_by(ExecID)`` shows the execution's current truth.  A
        reference to an ExecID this machine never saw (mid-stream start) is
        synthesised so the disposition is still visible.
        """
        state = chain.state
        stored = chain.exec_rows.get(ref_exec_id)
        if stored is None:
            stored = ExecutionRow(
                order_key=state.order_key,
                order_id=state.order_id,
                clordid=state.clordid,
                exec_id=ref_exec_id,
            )
        row = replace(
            stored,
            order_key=state.order_key,
            order_id=state.order_id,
            clordid=state.clordid,
            ord_status=state.ord_status,
            cum_qty=state.cum_qty,
            leaves_qty=state.leaves_qty,
            avg_px=state.avg_px,
            fill_status=disposition,
            transact_time=transact_time or stored.transact_time,
            ingest_ts=ingest_ts,
        )
        if corrected_from is not None:
            # The correcting report's 32/31/30 become the execution's values.
            last_shares = _number(corrected_from, Tag.LAST_SHARES)
            last_px = _number(corrected_from, Tag.LAST_PX)
            last_mkt = _text(corrected_from, Tag.LAST_MKT)
            text = _text(corrected_from, Tag.TEXT)
            if last_shares is not None:
                row = replace(row, last_shares=last_shares)
            if last_px is not None:
                row = replace(row, last_px=last_px)
            if last_mkt:
                row = replace(row, last_mkt=last_mkt)
            if text:
                row = replace(row, text=text)
        chain.exec_rows[ref_exec_id] = row
        return row

    def _exec_event(
        self,
        chain: _Chain,
        f: dict[int, str],
        trans_type: ExecTransType | None,
        exec_type: ExecType | None,
        ingest_ts: datetime,
    ) -> OrderEventRow:
        """Derive the lifecycle event for an execution report (doc 01 §6)."""
        state = chain.state
        exec_id = _text(f, Tag.EXEC_ID)
        exec_ref_id = _text(f, Tag.EXEC_REF_ID)
        last_shares = _number(f, Tag.LAST_SHARES)
        last_px = _number(f, Tag.LAST_PX)

        if trans_type is ExecTransType.CANCEL:
            event_type = EventType.FILL_BUST
            detail = (
                f"bust of {exec_ref_id or '?'}: restated cum {_num(state.cum_qty)}, "
                f"leaves {_num(state.leaves_qty)}, avg {_num(state.avg_px)}"
            )
        elif trans_type is ExecTransType.CORRECT:
            event_type = EventType.FILL_CORRECT
            detail = (
                f"correct of {exec_ref_id or '?'}: "
                f"{_num(last_shares or 0.0)} @ {_num(last_px or 0.0)}, "
                f"restated cum {_num(state.cum_qty)}, avg {_num(state.avg_px)}"
            )
        else:
            event_type = _EXEC_TYPE_EVENTS.get(exec_type, EventType.STATUS)
            if event_type in (EventType.PARTIAL_FILL, EventType.FULL_FILL):
                detail = (
                    f"fill {_num(last_shares or 0.0)} @ {_num(last_px or 0.0)}"
                    f" (cum {_num(state.cum_qty)}, leaves {_num(state.leaves_qty)},"
                    f" avg {_num(state.avg_px)})"
                )
            elif event_type == EventType.NEW_REJECT:
                detail = (
                    f"reject: {_text(f, Tag.TEXT) or 'order rejected'} "
                    f"(103={_text(f, Tag.ORD_REJ_REASON)})"
                )
            elif event_type == EventType.AMEND_ACK:
                detail = (
                    f"amend confirmed: qty {_num(state.order_qty)}, "
                    f"price {_num(state.price)}, ClOrdID {state.clordid}"
                )
            elif event_type == EventType.CANCEL_ACK:
                detail = f"cancel confirmed: cum {_num(state.cum_qty)}, leaves {_num(state.leaves_qty)}"
            elif event_type == EventType.NEW_ACK:
                detail = f"ack: OrderID {state.order_id or '?'}"
            else:
                detail = (
                    f"exec report {exec_id or '?'}: "
                    f"ExecType={exec_type.name if exec_type else ''}, "
                    f"OrdStatus={state.ord_status.name if state.ord_status else ''}"
                )

        return self._build_event(
            chain,
            f,
            event_type=event_type,
            msg_type="8",
            order_qty=state.order_qty,
            price=state.price,
            detail=detail,
            ingest_ts=ingest_ts,
            clordid=_text(f, Tag.CL_ORD_ID) or state.clordid,
        )

    # ----------------------------------------------------------------- #
    # 35=9  OrderCancelReject (rule 5; edge case 4)
    # ----------------------------------------------------------------- #

    def _handle_cancel_reject(
        self,
        f: dict[int, str],
        chain: _Chain,
        ingest_ts: datetime,
    ) -> tuple[list[ExecutionRow], list[OrderEventRow]]:
        state = chain.state
        clordid = _text(f, Tag.CL_ORD_ID)
        response_to = _enum(CxlRejResponseTo, f, Tag.CXL_REJ_RESPONSE_TO)

        if response_to is CxlRejResponseTo.ORDER_CANCEL_REQUEST:
            action: str | None = PendingAction.CANCEL
        elif response_to is CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST:
            action = PendingAction.REPLACE
        else:
            # 434 absent/unknown: fall back to whichever request this ClOrdID was.
            action = next(
                (r.action for r in reversed(chain.pending) if r.clordid == clordid),
                None,
            )

        if action is not None:
            # Removing the request also discards its staged terms.
            self._clear_pending(chain, action, clordid)

        # Revert to the per-request snapshot; a venue-supplied 39 wins over it.
        venue_status = _enum(OrdStatus, f, Tag.ORD_STATUS)
        if venue_status is not None:
            state.ord_status = venue_status
        elif clordid in chain.prior_status:
            state.ord_status = chain.prior_status[clordid]

        if Tag.CXL_REJ_REASON in f:
            state.cxl_rej_reason = f[int(Tag.CXL_REJ_REASON)]
        if Tag.TEXT in f:
            state.text = f[int(Tag.TEXT)]
        self._recompute_pending(chain)

        event_type = (
            EventType.AMEND_REJECT
            if action == PendingAction.REPLACE
            else EventType.CANCEL_REJECT
        )
        detail = (
            f"reject: {_text(f, Tag.TEXT) or 'request rejected'} "
            f"(102={_text(f, Tag.CXL_REJ_REASON)})"
        )
        event = self._build_event(
            chain,
            f,
            event_type=event_type,
            msg_type="9",
            order_qty=state.order_qty,
            price=state.price,
            detail=detail,
            ingest_ts=ingest_ts,
            clordid=clordid,
        )
        return [], [event]

    # ----------------------------------------------------------------- #
    # 35=Q  DontKnowTrade (rule 6; edge case 8)
    # ----------------------------------------------------------------- #

    def _handle_dont_know_trade(
        self,
        f: dict[int, str],
        chain: _Chain,
        ingest_ts: datetime,
    ) -> tuple[list[ExecutionRow], list[OrderEventRow]]:
        state = chain.state
        exec_id = _text(f, Tag.EXEC_ID)
        if Tag.DK_REASON in f:
            state.dk_reason = f[int(Tag.DK_REASON)]
        if Tag.TEXT in f:
            state.text = f[int(Tag.TEXT)]

        # No economic change; the referenced execution is re-emitted as DK'd.
        row = self._reemit_reference(
            chain,
            exec_id,
            FillStatus.DK,
            ingest_ts,
            transact_time=parse_transact_time(f.get(int(Tag.TRANSACT_TIME))),
        )
        detail = f"DK trade {exec_id or '?'} (127={_text(f, Tag.DK_REASON)})"
        event = self._build_event(
            chain,
            f,
            event_type=EventType.DK_TRADE,
            msg_type="Q",
            order_qty=state.order_qty,
            price=state.price,
            detail=detail,
            ingest_ts=ingest_ts,
            clordid=state.clordid,
        )
        return [row], [event]

    # ----------------------------------------------------------------- #
    # events
    # ----------------------------------------------------------------- #

    def _build_event(
        self,
        chain: _Chain,
        f: dict[int, str],
        *,
        event_type: str,
        msg_type: str,
        order_qty: float | None,
        price: float | None,
        detail: str,
        ingest_ts: datetime,
        clordid: str,
    ) -> OrderEventRow:
        state = chain.state
        return OrderEventRow(
            order_key=state.order_key,
            clordid=clordid,
            orig_clordid=_text(f, Tag.ORIG_CL_ORD_ID),
            order_id=state.order_id,
            event_type=event_type,
            msg_type=msg_type,
            ord_status=state.ord_status,
            order_qty=order_qty if order_qty is not None else state.order_qty,
            price=price if price is not None else state.price,
            detail=detail,
            transact_time=parse_transact_time(f.get(int(Tag.TRANSACT_TIME))),
            ingest_ts=ingest_ts,
        )
