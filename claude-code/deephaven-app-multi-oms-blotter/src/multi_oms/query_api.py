"""Query API over the multi-OMS DAG -- doc 09 section 7.

Every function returns a **live** table (still a DAG node, so callers may subscribe
to it); snapshotting is the caller's choice.  Ids are resolved through ``id_index``
with ``where_in`` so the resolution itself stays live and incremental rather than
being frozen at call time -- which is what makes ``find_chain`` heal when a late
parent tape arrives.

All identifiers are sanitized before interpolation into query strings: ids are
generator-controlled alphanumerics, but the filters are f-strings compiled to Java,
so quoting characters are stripped regardless.  :func:`sanitize_id` is a copy of
``dh_app.query_api``'s (doc 05 section 8 module ownership), not an import.

``deephaven`` is deliberately **not** imported at module scope -- only table methods
are called, at runtime -- so :func:`sanitize_id` is unit-testable on a bare python.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Tuple

__all__ = ["QUERY_API_NAMES", "CHAIN_SORT", "sanitize_id", "make_query_api"]

#: The function names exported as globals by ``app.py``.
QUERY_API_NAMES: Tuple[str, ...] = (
    "find_chain",
    "get_order",
    "find_by_account",
    "find_by_symbol",
    "hub_orders",
    "breaks_only",
    "order_detail",
)

#: Canonical ordering of a family: chains grouped, then upstream -> downstream.
CHAIN_SORT = ["RootKey", "Depth", "Oms", "OrderKey"]

#: Characters that could break out of a backtick-quoted query-string literal.
_FORBIDDEN = "`\"'\\\n\r\t"


def sanitize_id(value: Any) -> str:
    """Strip quoting/escape characters from a user-supplied identifier.

    Args:
        value: Any identifier (``GlobalKey``, ``ClOrdID``, ``Account``, ``Oms``, ...).

    Returns:
        The value as a string with backticks, quotes, backslashes and control
        whitespace removed, safe to interpolate into a backtick-quoted literal.
    """
    if value is None:
        return ""
    text = value if isinstance(value, str) else str(value)
    cleaned = "".join(ch for ch in text if ch not in _FORBIDDEN and ch >= " ")
    return cleaned.strip()


def make_query_api(tables: Mapping[str, Any]) -> Dict[str, Callable[..., Any]]:
    """Build the query-API functions bound to a set of derived tables.

    Args:
        tables: The dict returned by :func:`multi_oms.dag.build_derived`.

    Returns:
        A dict of plain functions keyed by :data:`QUERY_API_NAMES`, ready to be
        splatted into module globals by ``app.py``.
    """
    orders_recon = tables["orders_recon"]
    oms_executions = tables["oms_executions"]
    oms_events = tables["oms_events"]
    id_index = tables["id_index"]

    def find_chain(any_id: str) -> Any:
        """Return the whole family (every hop, every hub) containing ``any_id``.

        Works from either end of the chain: the id may be an ``OrderID`` or any
        ``ClOrdID`` the order has ever carried, on any hub. Selecting an OMS-C order
        returns its OMS-A ancestor and every sibling child, and vice versa.

        Args:
            any_id: Any identifier on any hub.

        Returns:
            A live view of ``orders_recon`` restricted to every matching ``RootKey``,
            grouped by chain and sorted upstream -> downstream (empty if unknown).
        """
        value = sanitize_id(any_id)
        matches = id_index.where(f"Id == `{value}`")
        hits = orders_recon.where_in(matches, "GlobalKey")
        roots = hits.select_distinct(["RootKey"])
        return orders_recon.where_in(roots, "RootKey").sort(CHAIN_SORT)

    def get_order(oms: str, any_id: str) -> Any:
        """Return one hub's order by any of its ids.

        Args:
            oms: The hub name (``Oms`` column value).
            any_id: An ``OrderID`` or any ``ClOrdID`` the chain has carried.

        Returns:
            A live view of ``orders_recon`` (empty if unknown on that hub).
        """
        hub = sanitize_id(oms)
        value = sanitize_id(any_id)
        matches = id_index.where([f"Oms == `{hub}`", f"Id == `{value}`"])
        return orders_recon.where_in(matches, "GlobalKey")

    def find_by_account(account: str) -> Any:
        """Return every hub order for an account (tag 1)."""
        value = sanitize_id(account)
        return orders_recon.where(f"Account == `{value}`")

    def find_by_symbol(symbol: str) -> Any:
        """Return every hub order for a symbol (tag 55)."""
        value = sanitize_id(symbol)
        return orders_recon.where(f"Symbol == `{value}`")

    def hub_orders(oms: str) -> Any:
        """Return one hub's blotter rows.

        Args:
            oms: The hub name (``Oms`` column value).

        Returns:
            A live view of ``orders_recon`` filtered to that hub.
        """
        value = sanitize_id(oms)
        return orders_recon.where(f"Oms == `{value}`")

    def breaks_only() -> Any:
        """Return every order whose ``BreakKind`` is not ``NONE``.

        Wider than the ``oms_breaks`` table, which additionally excludes the amber
        ``UNROUTED`` rows: this is the "show me anything that is not clean" view.
        """
        return orders_recon.where("BreakKind != `NONE`")

    def order_detail(global_key: str) -> Dict[str, Any]:
        """Return the three linked views for one hop.

        Args:
            global_key: The cross-hub key ``"<Oms>|<OrderKey>"``.

        Returns:
            ``{"state": ..., "executions": ..., "events": ...}`` -- all live tables
            for that one hop (never rolled up across hubs), history newest first.
        """
        value = sanitize_id(global_key)
        predicate = f"GlobalKey == `{value}`"
        return {
            "state": orders_recon.where(predicate),
            "executions": oms_executions.where(predicate).sort_descending(["IngestTs"]),
            "events": oms_events.where(predicate).sort_descending(["IngestTs"]),
        }

    return {
        "find_chain": find_chain,
        "get_order": get_order,
        "find_by_account": find_by_account,
        "find_by_symbol": find_by_symbol,
        "hub_orders": hub_orders,
        "breaks_only": breaks_only,
        "order_detail": order_detail,
    }
