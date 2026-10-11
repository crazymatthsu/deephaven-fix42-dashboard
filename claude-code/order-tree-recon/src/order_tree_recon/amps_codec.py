"""JSON <-> native order rows for the AMPS SOW topics -- doc 14 section 13.

Pure stdlib, so the publisher, the Deephaven source and the unit suite share one codec.
A message is one JSON object holding the current state of one order in its system's
native shape (:mod:`order_tree_recon.native`); the topic is ``prefix + table name`` and
the AMPS SOW key is the table's id column.

Decoding is lenient about *shape* and strict about *meaning*. A missing or ``null``
field is a null (the adapters treat a null parent id like an empty one); a number sent
as a string is accepted. A message that is not an object, has no order id, or carries a
value that can't be the column's type is a :class:`DecodeError`: the Deephaven source
counts and logs it and keeps the subscription alive.
"""

from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from order_tree_recon.native import DOUBLE, LONG, NATIVE_SCHEMAS, STRING, NativeSchema

__all__ = ["DecodeError", "topic", "topics", "encode", "decode", "key_of", "delete_payload"]


class DecodeError(ValueError):
    """A message that can't become a native row."""


def topic(schema: NativeSchema, prefix: str) -> str:
    """The AMPS topic of one native table, e.g. ``otr.router_orders``."""
    return f"{prefix}{schema.name}"


def topics(prefix: str) -> Dict[str, str]:
    """``{table name: topic}`` for every native table, upstream first."""
    return {name: topic(schema, prefix) for name, schema in NATIVE_SCHEMAS.items()}


def _encode_value(kind: str, value: Any) -> Any:
    if value is None:
        return None
    if kind == STRING:
        return str(value)
    if kind == LONG:
        return int(value)
    return float(value)


def encode(schema: NativeSchema, row: Mapping[str, Any]) -> str:
    """One native row as the JSON message its system publishes.

    Every schema column is written (``None`` as ``null``), in schema order; anything
    else in ``row`` is ignored. NaN and infinities are refused: they are not JSON.

    Raises:
        KeyError: ``row`` has no value for the key column.
        ValueError: a value is not a finite number where one is expected.
    """
    if row.get(schema.key) in (None, ""):
        raise KeyError(f"{schema.name}: row has no {schema.key}")
    body = {name: _encode_value(kind, row.get(name)) for name, kind in schema.columns}
    return json.dumps(body, separators=(",", ":"), allow_nan=False)


def delete_payload(schema: NativeSchema, order_id: str) -> str:
    """The minimal message ``sow_delete_by_data`` needs: AMPS derives the SOW key from it."""
    return json.dumps({schema.key: order_id}, separators=(",", ":"))


def _decode_value(schema: NativeSchema, name: str, kind: str, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool) or isinstance(value, (dict, list)):
        raise DecodeError(f"{schema.name}.{name}: expected a {kind}, got {value!r}")
    if kind == STRING:
        return value if isinstance(value, str) else str(value)
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise DecodeError(f"{schema.name}.{name}: expected a {kind}, got {value!r}") from None
    if kind == DOUBLE:
        return number
    if not math.isfinite(number) or number != int(number):
        raise DecodeError(f"{schema.name}.{name}: expected a whole number, got {value!r}")
    return int(number)


def decode(schema: NativeSchema, data: Union[str, bytes]) -> Tuple[Any, ...]:
    """One message as a row tuple in ``schema.columns`` order.

    Raises:
        DecodeError: not a JSON object, no order id, or a value of the wrong type.
    """
    try:
        body = json.loads(data)
    except (TypeError, ValueError) as exc:
        raise DecodeError(f"{schema.name}: not JSON ({exc})") from None
    if not isinstance(body, dict):
        raise DecodeError(f"{schema.name}: expected a JSON object, got {type(body).__name__}")
    values: List[Any] = [_decode_value(schema, name, kind, body.get(name)) for name, kind in schema.columns]
    key = values[schema.column_names.index(schema.key)]
    if not key:
        raise DecodeError(f"{schema.name}: message has no {schema.key}")
    return tuple(values)


def key_of(schema: NativeSchema, data: Union[str, bytes]) -> Optional[str]:
    """The order id in a message, or ``None`` when there is none.

    Reads the key field only: an OOF for a record whose *other* fields were malformed
    must still resolve to the order it removes.
    """
    try:
        body = json.loads(data)
    except (TypeError, ValueError):
        return None
    key = body.get(schema.key) if isinstance(body, dict) else None
    if key is None or isinstance(key, (bool, dict, list)) or key == "":
        return None
    return key if isinstance(key, str) else str(key)
