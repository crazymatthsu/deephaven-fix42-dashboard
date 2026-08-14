"""Query API over the derived DAG -- doc 03 section 2.5 / doc 05 section 4.

Every function returns a **live** table (still a DAG node, so callers may subscribe
to it); snapshotting is the caller's choice.  Aliases are resolved through the
index tables with ``where_in`` so the resolution itself stays live and incremental
rather than being frozen at call time.

All identifiers are sanitized before interpolation into query strings (doc 04
section 9.7): ids are generator-controlled alphanumerics, but the filters are
f-strings compiled to Java, so quoting characters are stripped regardless.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Mapping

from deephaven.table import Table

__all__ = ["QUERY_API_NAMES", "sanitize_id", "make_query_api"]

#: The function names exported as globals by ``app.py``.
QUERY_API_NAMES = (
    "get_by_order_id",
    "get_by_clordid",
    "get_by_execid",
    "find_by_account",
    "find_by_symbol",
    "order_detail",
)

#: Characters that could break out of a backtick-quoted query-string literal.
_FORBIDDEN = "`\"'\\\n\r\t"


def sanitize_id(value: Any) -> str:
    """Strip quoting/escape characters from a user-supplied identifier.

    Args:
        value: Any identifier (``OrderKey``, ``ClOrdID``, ``Account``, ...).

    Returns:
        The value as a string with backticks, quotes, backslashes and control
        whitespace removed, safe to interpolate into a backtick-quoted literal.
    """
    if value is None:
        return ""
    text = value if isinstance(value, str) else str(value)
    cleaned = "".join(ch for ch in text if ch not in _FORBIDDEN and ch >= " ")
    return cleaned.strip()


def make_query_api(tables: Mapping[str, Table]) -> Dict[str, Callable[..., Any]]:
    """Build the query-API functions bound to a set of derived tables.

    Args:
        tables: The dict returned by :func:`dh_app.dag.build_derived`.

    Returns:
        A dict of plain functions keyed by :data:`QUERY_API_NAMES`, ready to be
        splatted into module globals by ``app.py``.
    """
    order_state_latest = tables["order_state_latest"]
    executions = tables["executions"]
    order_events = tables["order_events"]
    clordid_index = tables["clordid_index"]
    execid_index = tables["execid_index"]

    def get_by_order_id(order_id: str) -> Table:
        """Return the live cache row(s) for a venue ``OrderID`` (tag 37).

        Args:
            order_id: The venue order id.

        Returns:
            A filtered live view of ``order_state_latest`` (empty if unknown).
        """
        value = sanitize_id(order_id)
        return order_state_latest.where(f"OrderID == `{value}`")

    def get_by_clordid(clordid: str) -> Table:
        """Return the live cache row for any ``ClOrdID`` in an amend chain.

        Resolution goes through ``clordid_index`` so superseded client ids (the
        ``C1`` of a ``C1 -> C2 -> C3`` chain) still find their order.

        Args:
            clordid: Any client order id ever seen for the chain (tag 11).

        Returns:
            A filtered live view of ``order_state_latest`` (empty if unknown).
        """
        value = sanitize_id(clordid)
        matches = clordid_index.where(f"ClOrdID == `{value}`")
        return order_state_latest.where_in(matches, "OrderKey")

    def get_by_execid(execid: str) -> Table:
        """Return the live cache row owning an ``ExecID`` (tag 17).

        Args:
            execid: The execution id.

        Returns:
            A filtered live view of ``order_state_latest`` (empty if unknown).
        """
        value = sanitize_id(execid)
        matches = execid_index.where(f"ExecID == `{value}`")
        return order_state_latest.where_in(matches, "OrderKey")

    def find_by_account(account: str) -> Table:
        """Return all live cache rows for an account (tag 1).

        Args:
            account: The account identifier.

        Returns:
            A filtered live view of ``order_state_latest``.
        """
        value = sanitize_id(account)
        return order_state_latest.where(f"Account == `{value}`")

    def find_by_symbol(symbol: str) -> Table:
        """Return all live cache rows for a symbol (tag 55).

        Args:
            symbol: The instrument symbol.

        Returns:
            A filtered live view of ``order_state_latest``.
        """
        value = sanitize_id(symbol)
        return order_state_latest.where(f"Symbol == `{value}`")

    def order_detail(order_key: str) -> Dict[str, Table]:
        """Return the three linked views for one order chain.

        Args:
            order_key: The chain's stable ``OrderKey`` (doc 01 section 3).

        Returns:
            ``{"state": ..., "executions": ..., "events": ...}`` -- all live tables
            filtered to that chain, executions/events newest first.
        """
        value = sanitize_id(order_key)
        predicate = f"OrderKey == `{value}`"
        return {
            "state": order_state_latest.where(predicate),
            "executions": executions.where(predicate).sort_descending(["IngestTs"]),
            "events": order_events.where(predicate).sort_descending(["IngestTs"]),
        }

    api: Dict[str, Callable[..., Any]] = {
        "get_by_order_id": get_by_order_id,
        "get_by_clordid": get_by_clordid,
        "get_by_execid": get_by_execid,
        "find_by_account": find_by_account,
        "find_by_symbol": find_by_symbol,
        "order_detail": order_detail,
    }
    return api
