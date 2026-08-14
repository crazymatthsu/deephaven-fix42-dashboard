"""FIX 4.2 tag numbers and enumerations used by this project.

Vocabulary and code -> name mappings come from
``docs/01-fix42-messages-and-state-machine.md`` §2 (the binding contract).

Every enum is a ``str``-valued :class:`enum.Enum` whose *value* is the raw FIX
code and whose *name* is the readable label published in Deephaven rows
(``PARTIALLY_FILLED`` rather than ``1``).  Each one exposes
:meth:`from_fix`, which is deliberately lenient: an unknown or missing code maps
to the ``UNKNOWN`` sentinel instead of raising, because the cache must never
reject a message from an audit feed.
"""

from __future__ import annotations

from enum import Enum, IntEnum
from typing import TypeVar

__all__ = [
    "Tag",
    "OrdStatus",
    "ExecType",
    "ExecTransType",
    "Side",
    "OrdType",
    "TimeInForce",
    "CxlRejResponseTo",
    "TERMINAL_STATUSES",
    "is_terminal",
]


class Tag(IntEnum):
    """FIX tag numbers (doc 01 §2 plus the standard header tags).

    Members are ``int`` constants, so they can be used directly as keys into the
    ``dict[int, str]`` returned by :func:`fix42cache.parser.parse_fix`.
    """

    ACCOUNT = 1
    AVG_PX = 6
    BEGIN_STRING = 8
    BODY_LENGTH = 9
    CHECK_SUM = 10
    CL_ORD_ID = 11
    CUM_QTY = 14
    EXEC_ID = 17
    EXEC_REF_ID = 19
    EXEC_TRANS_TYPE = 20
    HANDL_INST = 21
    LAST_MKT = 30
    LAST_PX = 31
    LAST_SHARES = 32
    MSG_SEQ_NUM = 34
    MSG_TYPE = 35
    ORDER_ID = 37
    ORDER_QTY = 38
    ORD_STATUS = 39
    ORD_TYPE = 40
    ORIG_CL_ORD_ID = 41
    PRICE = 44
    SENDER_COMP_ID = 49
    SENDING_TIME = 52
    SIDE = 54
    SYMBOL = 55
    TARGET_COMP_ID = 56
    TEXT = 58
    TIME_IN_FORCE = 59
    TRANSACT_TIME = 60
    STOP_PX = 99
    CXL_REJ_REASON = 102
    ORD_REJ_REASON = 103
    DK_REASON = 127
    EXEC_TYPE = 150
    LEAVES_QTY = 151
    CXL_REJ_RESPONSE_TO = 434


_E = TypeVar("_E", bound="_FixEnum")

#: Value used by every enum's ``UNKNOWN`` sentinel.  It is not a valid FIX code,
#: so it can never collide with a real one.
UNKNOWN_CODE = "?"


class _FixEnum(str, Enum):
    """Base class for the str-valued FIX enums (no members of its own)."""

    @classmethod
    def from_fix(cls: type[_E], code: str | None) -> _E:
        """Map a raw FIX code to a member.

        Unknown, empty and missing codes map to ``cls.UNKNOWN`` rather than
        raising -- the cache tolerates anything the wire hands it.
        """
        if code is None:
            return cls.UNKNOWN  # type: ignore[attr-defined,no-any-return]
        try:
            return cls(code)
        except ValueError:
            return cls.UNKNOWN  # type: ignore[attr-defined,no-any-return]


class OrdStatus(_FixEnum):
    """Tag 39 OrdStatus."""

    NEW = "0"
    PARTIALLY_FILLED = "1"
    FILLED = "2"
    DONE_FOR_DAY = "3"
    CANCELED = "4"
    REPLACED = "5"
    PENDING_CANCEL = "6"
    REJECTED = "8"
    PENDING_NEW = "A"
    EXPIRED = "C"
    PENDING_REPLACE = "E"
    UNKNOWN = UNKNOWN_CODE


class ExecType(_FixEnum):
    """Tag 150 ExecType."""

    NEW = "0"
    PARTIAL_FILL = "1"
    FILL = "2"
    DONE_FOR_DAY = "3"
    CANCELED = "4"
    REPLACED = "5"
    PENDING_CANCEL = "6"
    REJECTED = "8"
    PENDING_NEW = "A"
    EXPIRED = "C"
    RESTATED = "D"
    PENDING_REPLACE = "E"
    UNKNOWN = UNKNOWN_CODE


class ExecTransType(_FixEnum):
    """Tag 20 ExecTransType (0=New, 1=Cancel/bust, 2=Correct, 3=Status)."""

    NEW = "0"
    CANCEL = "1"
    CORRECT = "2"
    STATUS = "3"
    UNKNOWN = UNKNOWN_CODE


class Side(_FixEnum):
    """Tag 54 Side."""

    BUY = "1"
    SELL = "2"
    SELL_SHORT = "5"
    UNKNOWN = UNKNOWN_CODE


class OrdType(_FixEnum):
    """Tag 40 OrdType."""

    MARKET = "1"
    LIMIT = "2"
    UNKNOWN = UNKNOWN_CODE


class TimeInForce(_FixEnum):
    """Tag 59 TimeInForce."""

    DAY = "0"
    GTC = "1"
    IOC = "3"
    FOK = "4"
    UNKNOWN = UNKNOWN_CODE


class CxlRejResponseTo(_FixEnum):
    """Tag 434 CxlRejResponseTo (1 = a cancel `F` was rejected, 2 = a replace `G`)."""

    ORDER_CANCEL_REQUEST = "1"
    ORDER_CANCEL_REPLACE_REQUEST = "2"
    UNKNOWN = UNKNOWN_CODE


#: Statuses that make an order terminal (doc 01 §4, ``Terminal`` column).
TERMINAL_STATUSES: frozenset[OrdStatus] = frozenset(
    {
        OrdStatus.FILLED,
        OrdStatus.CANCELED,
        OrdStatus.REJECTED,
        OrdStatus.EXPIRED,
        OrdStatus.DONE_FOR_DAY,
    }
)


def is_terminal(status: OrdStatus | None) -> bool:
    """Return ``True`` when ``status`` is one of the terminal statuses."""
    return status in TERMINAL_STATUSES
