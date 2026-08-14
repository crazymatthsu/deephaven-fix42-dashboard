"""Row models published by the state machine.

``to_row()`` on each dataclass produces the **frozen** Deephaven column names
from ``docs/01-fix42-messages-and-state-machine.md`` §4 (OrderState) and §6
(executions / order_events / fix_messages).  The ``dh_app`` layer builds its
table schemas from those names, so they must not drift; the ``*_COLUMNS``
tuples in this module are the canonical, ordered lists.

Conventions:

* enum-valued columns render as the readable enum ``.name`` (``""`` when unset);
* timestamps are timezone-aware ``datetime`` objects (UTC) or ``None``;
* OrderState numeric columns default to ``0.0``/``0`` and string columns to
  ``""`` -- the snapshot is always fully populated;
* fix_messages numeric columns are ``None`` when the tag is absent, so the audit
  table distinguishes "absent" from "zero".
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime

from .fixtags import (
    CxlRejResponseTo,
    ExecTransType,
    ExecType,
    OrdStatus,
    OrdType,
    Side,
    Tag,
    TimeInForce,
    is_terminal,
)
from .parser import checksum_ok, parse_transact_time, render_pipe

__all__ = [
    "PendingAction",
    "FillStatus",
    "EventType",
    "OrderState",
    "ExecutionRow",
    "OrderEventRow",
    "MessageRow",
    "ORDER_STATE_COLUMNS",
    "EXECUTION_COLUMNS",
    "ORDER_EVENT_COLUMNS",
    "MESSAGE_COLUMNS",
]


class PendingAction:
    """Values of the ``PendingAction`` column (doc 01 §4)."""

    NONE = "NONE"
    NEW = "NEW"
    CANCEL = "CANCEL"
    REPLACE = "REPLACE"

    ALL = ("NONE", "NEW", "CANCEL", "REPLACE")


class FillStatus:
    """Values of the executions ``FillStatus`` column (doc 01 §6)."""

    NORMAL = "NORMAL"
    BUSTED = "BUSTED"
    CORRECTED = "CORRECTED"
    DK = "DK"

    ALL = ("NORMAL", "BUSTED", "CORRECTED", "DK")


class EventType:
    """Frozen ``EventType`` names for the order_events stream (doc 01 §6)."""

    NEW_REQUEST = "NEW_REQUEST"
    NEW_ACK = "NEW_ACK"
    NEW_REJECT = "NEW_REJECT"
    AMEND_REQUEST = "AMEND_REQUEST"
    AMEND_ACK = "AMEND_ACK"
    AMEND_REJECT = "AMEND_REJECT"
    CANCEL_REQUEST = "CANCEL_REQUEST"
    CANCEL_ACK = "CANCEL_ACK"
    CANCEL_REJECT = "CANCEL_REJECT"
    PENDING_NEW = "PENDING_NEW"
    PENDING_AMEND = "PENDING_AMEND"
    PENDING_CANCEL = "PENDING_CANCEL"
    PARTIAL_FILL = "PARTIAL_FILL"
    FULL_FILL = "FULL_FILL"
    FILL_BUST = "FILL_BUST"
    FILL_CORRECT = "FILL_CORRECT"
    DK_TRADE = "DK_TRADE"
    RESTATED = "RESTATED"
    STATUS = "STATUS"
    EXPIRED = "EXPIRED"
    DONE_FOR_DAY = "DONE_FOR_DAY"

    ALL = (
        "NEW_REQUEST",
        "NEW_ACK",
        "NEW_REJECT",
        "AMEND_REQUEST",
        "AMEND_ACK",
        "AMEND_REJECT",
        "CANCEL_REQUEST",
        "CANCEL_ACK",
        "CANCEL_REJECT",
        "PENDING_NEW",
        "PENDING_AMEND",
        "PENDING_CANCEL",
        "PARTIAL_FILL",
        "FULL_FILL",
        "FILL_BUST",
        "FILL_CORRECT",
        "DK_TRADE",
        "RESTATED",
        "STATUS",
        "EXPIRED",
        "DONE_FOR_DAY",
    )


def _name(value: object | None) -> str:
    """Render an enum member as its readable name (``""`` when unset)."""
    return value.name if value is not None else ""


# --------------------------------------------------------------------------- #
# OrderState (doc 01 §4)
# --------------------------------------------------------------------------- #

ORDER_STATE_COLUMNS: tuple[str, ...] = (
    "OrderKey",
    "OrderID",
    "ClOrdID",
    "OrigClOrdID",
    "RootClOrdID",
    "ClOrdIDChain",
    "Account",
    "Symbol",
    "Side",
    "OrdType",
    "TimeInForce",
    "OrderQty",
    "Price",
    "StopPx",
    "OrdStatus",
    "PendingAction",
    "PendingClOrdID",
    "LastExecType",
    "CumQty",
    "LeavesQty",
    "AvgPx",
    "LastShares",
    "LastPx",
    "LastMkt",
    "OrdRejReason",
    "CxlRejReason",
    "DKReason",
    "Text",
    "ExecCount",
    "MsgCount",
    "FirstSeenTs",
    "LastUpdateTs",
    "LastMsgType",
    "Terminal",
)


@dataclass
class OrderState:
    """The cache value: one row per order chain (doc 01 §4).

    Instances held by the state machine are mutable; every ``Result`` carries a
    :meth:`copy` so consumers observe an immutable snapshot.
    """

    order_key: str
    order_id: str = ""
    clordid: str = ""
    orig_clordid: str = ""
    root_clordid: str = ""
    clordid_chain: list[str] = field(default_factory=list)
    account: str = ""
    symbol: str = ""
    side: Side | None = None
    ord_type: OrdType | None = None
    time_in_force: TimeInForce | None = None
    order_qty: float = 0.0
    price: float = 0.0
    stop_px: float = 0.0
    ord_status: OrdStatus | None = None
    pending_action: str = PendingAction.NONE
    pending_clordid: str = ""
    last_exec_type: ExecType | None = None
    cum_qty: float = 0.0
    leaves_qty: float = 0.0
    avg_px: float = 0.0
    last_shares: float = 0.0
    last_px: float = 0.0
    last_mkt: str = ""
    ord_rej_reason: str = ""
    cxl_rej_reason: str = ""
    dk_reason: str = ""
    text: str = ""
    exec_count: int = 0
    msg_count: int = 0
    first_seen_ts: datetime | None = None
    last_update_ts: datetime | None = None
    last_msg_type: str = ""

    @property
    def terminal(self) -> bool:
        """``True`` when OrdStatus is FILLED/CANCELED/REJECTED/EXPIRED/DONE_FOR_DAY."""
        return is_terminal(self.ord_status)

    def copy(self) -> "OrderState":
        """Return an independent snapshot (the ClOrdID chain list is copied)."""
        return replace(self, clordid_chain=list(self.clordid_chain))

    def to_row(self) -> dict[str, object]:
        """Render the frozen doc 01 §4 columns."""
        return {
            "OrderKey": self.order_key,
            "OrderID": self.order_id,
            "ClOrdID": self.clordid,
            "OrigClOrdID": self.orig_clordid,
            "RootClOrdID": self.root_clordid,
            "ClOrdIDChain": ",".join(self.clordid_chain),
            "Account": self.account,
            "Symbol": self.symbol,
            "Side": _name(self.side),
            "OrdType": _name(self.ord_type),
            "TimeInForce": _name(self.time_in_force),
            "OrderQty": self.order_qty,
            "Price": self.price,
            "StopPx": self.stop_px,
            "OrdStatus": _name(self.ord_status),
            "PendingAction": self.pending_action,
            "PendingClOrdID": self.pending_clordid,
            "LastExecType": _name(self.last_exec_type),
            "CumQty": self.cum_qty,
            "LeavesQty": self.leaves_qty,
            "AvgPx": self.avg_px,
            "LastShares": self.last_shares,
            "LastPx": self.last_px,
            "LastMkt": self.last_mkt,
            "OrdRejReason": self.ord_rej_reason,
            "CxlRejReason": self.cxl_rej_reason,
            "DKReason": self.dk_reason,
            "Text": self.text,
            "ExecCount": self.exec_count,
            "MsgCount": self.msg_count,
            "FirstSeenTs": self.first_seen_ts,
            "LastUpdateTs": self.last_update_ts,
            "LastMsgType": self.last_msg_type,
            "Terminal": self.terminal,
        }


# --------------------------------------------------------------------------- #
# executions (doc 01 §6)
# --------------------------------------------------------------------------- #

EXECUTION_COLUMNS: tuple[str, ...] = (
    "OrderKey",
    "OrderID",
    "ClOrdID",
    "ExecID",
    "ExecRefID",
    "ExecTransType",
    "ExecType",
    "OrdStatus",
    "LastShares",
    "LastPx",
    "CumQty",
    "LeavesQty",
    "AvgPx",
    "LastMkt",
    "Text",
    "IsFill",
    "FillStatus",
    "TransactTime",
    "IngestTs",
)


@dataclass
class ExecutionRow:
    """One executions-stream row: per ``35=8``, per ``35=Q``, plus re-emissions.

    ``FillStatus`` carries the *latest* disposition of this ``ExecID``; bust /
    correct / DK messages re-emit the referenced execution's row with the new
    disposition so ``last_by(ExecID)`` downstream shows current truth.
    """

    order_key: str
    order_id: str = ""
    clordid: str = ""
    exec_id: str = ""
    exec_ref_id: str = ""
    exec_trans_type: ExecTransType | None = None
    exec_type: ExecType | None = None
    ord_status: OrdStatus | None = None
    last_shares: float = 0.0
    last_px: float = 0.0
    cum_qty: float = 0.0
    leaves_qty: float = 0.0
    avg_px: float = 0.0
    last_mkt: str = ""
    text: str = ""
    is_fill: bool = False
    fill_status: str = FillStatus.NORMAL
    transact_time: datetime | None = None
    ingest_ts: datetime | None = None

    def to_row(self) -> dict[str, object]:
        """Render the frozen doc 01 §6 executions columns."""
        return {
            "OrderKey": self.order_key,
            "OrderID": self.order_id,
            "ClOrdID": self.clordid,
            "ExecID": self.exec_id,
            "ExecRefID": self.exec_ref_id,
            "ExecTransType": _name(self.exec_trans_type),
            "ExecType": _name(self.exec_type),
            "OrdStatus": _name(self.ord_status),
            "LastShares": self.last_shares,
            "LastPx": self.last_px,
            "CumQty": self.cum_qty,
            "LeavesQty": self.leaves_qty,
            "AvgPx": self.avg_px,
            "LastMkt": self.last_mkt,
            "Text": self.text,
            "IsFill": self.is_fill,
            "FillStatus": self.fill_status,
            "TransactTime": self.transact_time,
            "IngestTs": self.ingest_ts,
        }


# --------------------------------------------------------------------------- #
# order_events (doc 01 §6)
# --------------------------------------------------------------------------- #

ORDER_EVENT_COLUMNS: tuple[str, ...] = (
    "OrderKey",
    "ClOrdID",
    "OrigClOrdID",
    "OrderID",
    "EventType",
    "MsgType",
    "OrdStatus",
    "OrderQty",
    "Price",
    "Detail",
    "TransactTime",
    "IngestTs",
)


@dataclass
class OrderEventRow:
    """One lifecycle event feeding the order-history panel (doc 01 §6)."""

    order_key: str
    clordid: str = ""
    orig_clordid: str = ""
    order_id: str = ""
    event_type: str = EventType.STATUS
    msg_type: str = ""
    ord_status: OrdStatus | None = None
    order_qty: float = 0.0
    price: float = 0.0
    detail: str = ""
    transact_time: datetime | None = None
    ingest_ts: datetime | None = None

    def to_row(self) -> dict[str, object]:
        """Render the frozen doc 01 §6 order_events columns."""
        return {
            "OrderKey": self.order_key,
            "ClOrdID": self.clordid,
            "OrigClOrdID": self.orig_clordid,
            "OrderID": self.order_id,
            "EventType": self.event_type,
            "MsgType": self.msg_type,
            "OrdStatus": _name(self.ord_status),
            "OrderQty": self.order_qty,
            "Price": self.price,
            "Detail": self.detail,
            "TransactTime": self.transact_time,
            "IngestTs": self.ingest_ts,
        }


# --------------------------------------------------------------------------- #
# fix_messages (doc 01 §6)
# --------------------------------------------------------------------------- #

MESSAGE_COLUMNS: tuple[str, ...] = (
    "OrderKey",
    "MsgType",
    "ClOrdID",
    "OrigClOrdID",
    "OrderID",
    "ExecID",
    "ExecRefID",
    "ExecTransType",
    "ExecType",
    "OrdStatus",
    "Account",
    "Symbol",
    "Side",
    "OrderQty",
    "OrdType",
    "Price",
    "TimeInForce",
    "CumQty",
    "LeavesQty",
    "AvgPx",
    "LastShares",
    "LastPx",
    "LastMkt",
    "OrdRejReason",
    "CxlRejReason",
    "CxlRejResponseTo",
    "DKReason",
    "Text",
    "TransactTime",
    "HandlInst",
    "RawFix",
    "ChecksumOk",
    "SeqNum",
    "SendingTime",
    "IngestTs",
)


def _opt_float(fields: dict[int, str], tag: int) -> float | None:
    """Read a numeric tag, returning ``None`` when absent or unparseable."""
    raw = fields.get(int(tag))
    if raw is None:
        return None
    try:
        return float(raw.strip())
    except ValueError:
        return None


def _opt_int(fields: dict[int, str], tag: int) -> int | None:
    """Read an integer tag, returning ``None`` when absent or unparseable."""
    raw = fields.get(int(tag))
    if raw is None:
        return None
    try:
        return int(raw.strip())
    except ValueError:
        return None


def _str(fields: dict[int, str], tag: int) -> str:
    """Read a string tag, defaulting to ``""``."""
    return fields.get(int(tag), "")


def _enum_name(enum_cls: type, fields: dict[int, str], tag: int) -> str:
    """Render a tag through its enum, or ``""`` when the tag is absent."""
    raw = fields.get(int(tag))
    if raw is None:
        return ""
    return enum_cls.from_fix(raw).name


@dataclass
class MessageRow:
    """The raw-message audit row: every doc 01 §2 tag as a typed column."""

    order_key: str
    msg_type: str = ""
    clordid: str = ""
    orig_clordid: str = ""
    order_id: str = ""
    exec_id: str = ""
    exec_ref_id: str = ""
    exec_trans_type: str = ""
    exec_type: str = ""
    ord_status: str = ""
    account: str = ""
    symbol: str = ""
    side: str = ""
    order_qty: float | None = None
    ord_type: str = ""
    price: float | None = None
    time_in_force: str = ""
    cum_qty: float | None = None
    leaves_qty: float | None = None
    avg_px: float | None = None
    last_shares: float | None = None
    last_px: float | None = None
    last_mkt: str = ""
    ord_rej_reason: str = ""
    cxl_rej_reason: str = ""
    cxl_rej_response_to: str = ""
    dk_reason: str = ""
    text: str = ""
    transact_time: datetime | None = None
    handl_inst: str = ""
    raw_fix: str = ""
    checksum_ok: bool | None = None
    seq_num: int | None = None
    sending_time: datetime | None = None
    ingest_ts: datetime | None = None

    @classmethod
    def from_fields(
        cls,
        fields: dict[int, str],
        order_key: str,
        raw: str,
        ingest_ts: datetime | None,
    ) -> "MessageRow":
        """Build an audit row from parsed fields plus the original wire string.

        ``order_key`` may be ``""`` when the message cannot be attributed to a
        chain -- the audit table records everything regardless.
        """
        return cls(
            order_key=order_key,
            msg_type=_str(fields, Tag.MSG_TYPE),
            clordid=_str(fields, Tag.CL_ORD_ID),
            orig_clordid=_str(fields, Tag.ORIG_CL_ORD_ID),
            order_id=_str(fields, Tag.ORDER_ID),
            exec_id=_str(fields, Tag.EXEC_ID),
            exec_ref_id=_str(fields, Tag.EXEC_REF_ID),
            exec_trans_type=_enum_name(ExecTransType, fields, Tag.EXEC_TRANS_TYPE),
            exec_type=_enum_name(ExecType, fields, Tag.EXEC_TYPE),
            ord_status=_enum_name(OrdStatus, fields, Tag.ORD_STATUS),
            account=_str(fields, Tag.ACCOUNT),
            symbol=_str(fields, Tag.SYMBOL),
            side=_enum_name(Side, fields, Tag.SIDE),
            order_qty=_opt_float(fields, Tag.ORDER_QTY),
            ord_type=_enum_name(OrdType, fields, Tag.ORD_TYPE),
            price=_opt_float(fields, Tag.PRICE),
            time_in_force=_enum_name(TimeInForce, fields, Tag.TIME_IN_FORCE),
            cum_qty=_opt_float(fields, Tag.CUM_QTY),
            leaves_qty=_opt_float(fields, Tag.LEAVES_QTY),
            avg_px=_opt_float(fields, Tag.AVG_PX),
            last_shares=_opt_float(fields, Tag.LAST_SHARES),
            last_px=_opt_float(fields, Tag.LAST_PX),
            last_mkt=_str(fields, Tag.LAST_MKT),
            ord_rej_reason=_str(fields, Tag.ORD_REJ_REASON),
            cxl_rej_reason=_str(fields, Tag.CXL_REJ_REASON),
            cxl_rej_response_to=_enum_name(
                CxlRejResponseTo, fields, Tag.CXL_REJ_RESPONSE_TO
            ),
            dk_reason=_str(fields, Tag.DK_REASON),
            text=_str(fields, Tag.TEXT),
            transact_time=parse_transact_time(fields.get(int(Tag.TRANSACT_TIME))),
            handl_inst=_str(fields, Tag.HANDL_INST),
            raw_fix=render_pipe(raw),
            checksum_ok=checksum_ok(raw),
            seq_num=_opt_int(fields, Tag.MSG_SEQ_NUM),
            sending_time=parse_transact_time(fields.get(int(Tag.SENDING_TIME))),
            ingest_ts=ingest_ts,
        )

    def to_row(self) -> dict[str, object]:
        """Render the frozen doc 01 §6 fix_messages columns."""
        return {
            "OrderKey": self.order_key,
            "MsgType": self.msg_type,
            "ClOrdID": self.clordid,
            "OrigClOrdID": self.orig_clordid,
            "OrderID": self.order_id,
            "ExecID": self.exec_id,
            "ExecRefID": self.exec_ref_id,
            "ExecTransType": self.exec_trans_type,
            "ExecType": self.exec_type,
            "OrdStatus": self.ord_status,
            "Account": self.account,
            "Symbol": self.symbol,
            "Side": self.side,
            "OrderQty": self.order_qty,
            "OrdType": self.ord_type,
            "Price": self.price,
            "TimeInForce": self.time_in_force,
            "CumQty": self.cum_qty,
            "LeavesQty": self.leaves_qty,
            "AvgPx": self.avg_px,
            "LastShares": self.last_shares,
            "LastPx": self.last_px,
            "LastMkt": self.last_mkt,
            "OrdRejReason": self.ord_rej_reason,
            "CxlRejReason": self.cxl_rej_reason,
            "CxlRejResponseTo": self.cxl_rej_response_to,
            "DKReason": self.dk_reason,
            "Text": self.text,
            "TransactTime": self.transact_time,
            "HandlInst": self.handl_inst,
            "RawFix": self.raw_fix,
            "ChecksumOk": self.checksum_ok,
            "SeqNum": self.seq_num,
            "SendingTime": self.sending_time,
            "IngestTs": self.ingest_ts,
        }
