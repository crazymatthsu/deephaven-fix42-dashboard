"""Builders for well-formed FIX 4.2 test messages.

Every builder emits correct ``9 BodyLength`` and ``10 CheckSum`` framing (doc 01
§1) so parser and state-machine tests share one source of realistic input.
Messages are rendered pipe-delimited by default (readable in assertions); pass
``delimiter=SOH`` for the real wire form.
"""

from __future__ import annotations

from fix42cache.parser import PIPE, SOH

DEFAULT_SENDING_TIME = "20240115-14:30:00.000"
DEFAULT_TRANSACT_TIME = "20240115-14:30:00.000"

#: Sensible OrdStatus for a given ExecType when a test does not pin tag 39.
_STATUS_FOR_EXEC_TYPE = {
    "0": "0",
    "1": "1",
    "2": "2",
    "3": "3",
    "4": "4",
    "5": "5",
    "6": "6",
    "8": "8",
    "A": "A",
    "C": "C",
    "D": "D",
    "E": "E",
}


def _fmt(value: object) -> str:
    """Render a python value as a FIX field value."""
    if isinstance(value, bool):  # pragma: no cover - not used, guards misuse
        raise TypeError("bool is not a FIX value")
    if isinstance(value, float) and value == int(value):
        return str(int(value))
    return str(value)


def build_fix(
    msg_type: str,
    fields: dict[int, object] | None = None,
    *,
    seq: int = 1,
    sender: str = "VENUE",
    target: str = "OMS",
    sending_time: str = DEFAULT_SENDING_TIME,
    delimiter: str = PIPE,
) -> str:
    """Build a FIX 4.2 message with correct BodyLength/CheckSum.

    ``fields`` values that are ``None`` are omitted, so builders can expose
    optional tags as keyword arguments.
    """
    body_fields: list[tuple[int, str]] = [
        (35, msg_type),
        (34, str(seq)),
        (49, sender),
        (56, target),
        (52, sending_time),
    ]
    for tag, value in (fields or {}).items():
        if value is None:
            continue
        body_fields.append((int(tag), _fmt(value)))

    body = "".join(f"{tag}={value}{SOH}" for tag, value in body_fields)
    prefix = f"8=FIX.4.2{SOH}9={len(body)}{SOH}{body}"
    checksum = sum(prefix.encode("utf-8")) % 256
    message = f"{prefix}10={checksum:03d}{SOH}"
    return message if delimiter == SOH else message.replace(SOH, delimiter)


def new_order(
    clordid: str = "C1",
    *,
    account: str = "ACC1",
    symbol: str = "IBM",
    side: str = "1",
    qty: float = 1000,
    ord_type: str = "2",
    price: float | None = 185.50,
    tif: str = "0",
    stop_px: float | None = None,
    transact_time: str = DEFAULT_TRANSACT_TIME,
    order_id: str | None = None,
    seq: int = 1,
    delimiter: str = PIPE,
) -> str:
    """`35=D` NewOrderSingle."""
    return build_fix(
        "D",
        {
            11: clordid,
            37: order_id,
            1: account,
            55: symbol,
            54: side,
            38: qty,
            40: ord_type,
            44: price,
            59: tif,
            99: stop_px,
            21: "1",
            60: transact_time,
        },
        seq=seq,
        delimiter=delimiter,
    )


def exec_report(
    exec_type: str,
    *,
    exec_id: str = "E1",
    order_id: str = "ORD-1",
    clordid: str | None = "C1",
    orig_clordid: str | None = None,
    ord_status: str | None = None,
    exec_trans_type: str | None = None,
    exec_ref_id: str | None = None,
    cum_qty: float | None = None,
    leaves_qty: float | None = None,
    avg_px: float | None = None,
    last_shares: float | None = None,
    last_px: float | None = None,
    last_mkt: str | None = None,
    symbol: str | None = "IBM",
    side: str | None = "1",
    qty: float | None = 1000,
    price: float | None = None,
    account: str | None = None,
    ord_rej_reason: str | None = None,
    text: str | None = None,
    transact_time: str = DEFAULT_TRANSACT_TIME,
    seq: int = 1,
    delimiter: str = PIPE,
) -> str:
    """`35=8` ExecutionReport (tag 39 defaults to the natural status for 150)."""
    return build_fix(
        "8",
        {
            37: order_id,
            11: clordid,
            41: orig_clordid,
            17: exec_id,
            19: exec_ref_id,
            20: exec_trans_type,
            150: exec_type,
            39: ord_status if ord_status is not None else _STATUS_FOR_EXEC_TYPE.get(exec_type),
            1: account,
            55: symbol,
            54: side,
            38: qty,
            44: price,
            32: last_shares,
            31: last_px,
            30: last_mkt,
            14: cum_qty,
            151: leaves_qty,
            6: avg_px,
            103: ord_rej_reason,
            58: text,
            60: transact_time,
        },
        seq=seq,
        delimiter=delimiter,
    )


def replace_request(
    clordid: str,
    orig_clordid: str,
    *,
    order_id: str | None = None,
    account: str = "ACC1",
    symbol: str = "IBM",
    side: str = "1",
    qty: float | None = None,
    ord_type: str = "2",
    price: float | None = None,
    tif: str | None = None,
    transact_time: str = DEFAULT_TRANSACT_TIME,
    seq: int = 1,
    delimiter: str = PIPE,
) -> str:
    """`35=G` OrderCancelReplaceRequest."""
    return build_fix(
        "G",
        {
            11: clordid,
            41: orig_clordid,
            37: order_id,
            1: account,
            55: symbol,
            54: side,
            38: qty,
            40: ord_type,
            44: price,
            59: tif,
            21: "1",
            60: transact_time,
        },
        seq=seq,
        delimiter=delimiter,
    )


def cancel_request(
    clordid: str,
    orig_clordid: str,
    *,
    order_id: str | None = None,
    account: str = "ACC1",
    symbol: str = "IBM",
    side: str = "1",
    qty: float | None = None,
    transact_time: str = DEFAULT_TRANSACT_TIME,
    seq: int = 1,
    delimiter: str = PIPE,
) -> str:
    """`35=F` OrderCancelRequest."""
    return build_fix(
        "F",
        {
            11: clordid,
            41: orig_clordid,
            37: order_id,
            1: account,
            55: symbol,
            54: side,
            38: qty,
            60: transact_time,
        },
        seq=seq,
        delimiter=delimiter,
    )


def cancel_reject(
    clordid: str,
    orig_clordid: str,
    *,
    response_to: str = "1",
    order_id: str = "NONE",
    ord_status: str | None = None,
    cxl_rej_reason: str | None = None,
    text: str | None = None,
    transact_time: str = DEFAULT_TRANSACT_TIME,
    seq: int = 1,
    delimiter: str = PIPE,
) -> str:
    """`35=9` OrderCancelReject (tag 37 defaults to the `NONE` sentinel)."""
    return build_fix(
        "9",
        {
            11: clordid,
            41: orig_clordid,
            37: order_id,
            39: ord_status,
            102: cxl_rej_reason,
            434: response_to,
            58: text,
            60: transact_time,
        },
        seq=seq,
        delimiter=delimiter,
    )


def dk_trade(
    exec_id: str,
    *,
    order_id: str | None = "ORD-1",
    dk_reason: str = "A",
    symbol: str = "IBM",
    side: str = "1",
    qty: float | None = 1000,
    last_shares: float | None = None,
    last_px: float | None = None,
    text: str | None = None,
    transact_time: str = DEFAULT_TRANSACT_TIME,
    seq: int = 1,
    delimiter: str = PIPE,
) -> str:
    """`35=Q` DontKnowTrade."""
    return build_fix(
        "Q",
        {
            37: order_id,
            17: exec_id,
            127: dk_reason,
            55: symbol,
            54: side,
            38: qty,
            32: last_shares,
            31: last_px,
            58: text,
            60: transact_time,
        },
        seq=seq,
        delimiter=delimiter,
    )
