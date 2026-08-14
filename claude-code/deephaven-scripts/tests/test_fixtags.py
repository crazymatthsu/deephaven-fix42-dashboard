"""Tag constants and enum mappings (doc 01 §2)."""

from __future__ import annotations

import pytest

from fix42cache.fixtags import (
    TERMINAL_STATUSES,
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

ALL_ENUMS = [OrdStatus, ExecType, ExecTransType, Side, OrdType, TimeInForce, CxlRejResponseTo]


def test_tag_numbers_match_doc_vocabulary() -> None:
    assert (Tag.CL_ORD_ID, Tag.ORIG_CL_ORD_ID, Tag.ORDER_ID, Tag.EXEC_ID) == (11, 41, 37, 17)
    assert (Tag.EXEC_REF_ID, Tag.EXEC_TRANS_TYPE, Tag.EXEC_TYPE, Tag.ORD_STATUS) == (19, 20, 150, 39)
    assert (Tag.ACCOUNT, Tag.SYMBOL, Tag.SIDE, Tag.ORDER_QTY) == (1, 55, 54, 38)
    assert (Tag.ORD_TYPE, Tag.PRICE, Tag.TIME_IN_FORCE) == (40, 44, 59)
    assert (Tag.CUM_QTY, Tag.LEAVES_QTY, Tag.AVG_PX) == (14, 151, 6)
    assert (Tag.LAST_SHARES, Tag.LAST_PX, Tag.LAST_MKT) == (32, 31, 30)
    assert (Tag.ORD_REJ_REASON, Tag.CXL_REJ_REASON, Tag.CXL_REJ_RESPONSE_TO) == (103, 102, 434)
    assert (Tag.DK_REASON, Tag.TEXT, Tag.TRANSACT_TIME, Tag.HANDL_INST) == (127, 58, 60, 21)
    assert (Tag.MSG_SEQ_NUM, Tag.MSG_TYPE, Tag.SENDING_TIME, Tag.CHECK_SUM) == (34, 35, 52, 10)


def test_tag_members_are_plain_ints() -> None:
    """Tags double as dict keys into parse_fix output."""
    fields = {11: "C1"}
    assert fields[Tag.CL_ORD_ID] == "C1"
    assert Tag.PRICE == 44


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("0", "NEW"),
        ("1", "PARTIALLY_FILLED"),
        ("2", "FILLED"),
        ("3", "DONE_FOR_DAY"),
        ("4", "CANCELED"),
        ("5", "REPLACED"),
        ("6", "PENDING_CANCEL"),
        ("8", "REJECTED"),
        ("A", "PENDING_NEW"),
        ("C", "EXPIRED"),
        ("E", "PENDING_REPLACE"),
    ],
)
def test_ord_status_from_fix(code: str, expected: str) -> None:
    assert OrdStatus.from_fix(code).name == expected


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("0", "NEW"),
        ("1", "PARTIAL_FILL"),
        ("2", "FILL"),
        ("3", "DONE_FOR_DAY"),
        ("4", "CANCELED"),
        ("5", "REPLACED"),
        ("6", "PENDING_CANCEL"),
        ("8", "REJECTED"),
        ("A", "PENDING_NEW"),
        ("C", "EXPIRED"),
        ("D", "RESTATED"),
        ("E", "PENDING_REPLACE"),
    ],
)
def test_exec_type_from_fix(code: str, expected: str) -> None:
    assert ExecType.from_fix(code).name == expected


def test_exec_trans_type_from_fix() -> None:
    assert ExecTransType.from_fix("0").name == "NEW"
    assert ExecTransType.from_fix("1").name == "CANCEL"
    assert ExecTransType.from_fix("2").name == "CORRECT"
    assert ExecTransType.from_fix("3").name == "STATUS"


def test_side_ord_type_and_tif_from_fix() -> None:
    assert [Side.from_fix(c).name for c in ("1", "2", "5")] == ["BUY", "SELL", "SELL_SHORT"]
    assert [OrdType.from_fix(c).name for c in ("1", "2")] == ["MARKET", "LIMIT"]
    assert [TimeInForce.from_fix(c).name for c in ("0", "1", "3", "4")] == [
        "DAY",
        "GTC",
        "IOC",
        "FOK",
    ]


def test_cxl_rej_response_to_from_fix() -> None:
    assert CxlRejResponseTo.from_fix("1") is CxlRejResponseTo.ORDER_CANCEL_REQUEST
    assert CxlRejResponseTo.from_fix("2") is CxlRejResponseTo.ORDER_CANCEL_REPLACE_REQUEST


@pytest.mark.parametrize("enum_cls", ALL_ENUMS)
@pytest.mark.parametrize("code", ["Z", "", "99", None])
def test_unknown_codes_map_to_sentinel_without_raising(enum_cls: type, code: str | None) -> None:
    assert enum_cls.from_fix(code) is enum_cls.UNKNOWN
    assert enum_cls.from_fix(code).name == "UNKNOWN"


def test_enum_values_are_the_raw_fix_codes() -> None:
    assert OrdStatus.PARTIALLY_FILLED.value == "1"
    assert ExecType.PARTIAL_FILL.value == "1"
    assert Side.SELL_SHORT.value == "5"


def test_terminal_statuses() -> None:
    assert TERMINAL_STATUSES == frozenset(
        {
            OrdStatus.FILLED,
            OrdStatus.CANCELED,
            OrdStatus.REJECTED,
            OrdStatus.EXPIRED,
            OrdStatus.DONE_FOR_DAY,
        }
    )
    assert is_terminal(OrdStatus.FILLED) is True
    assert is_terminal(OrdStatus.PARTIALLY_FILLED) is False
    assert is_terminal(None) is False
