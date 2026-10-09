"""Console query API and the filter builders the dashboard shares -- doc 14 section 7.

Every function returns **live** tables. ``deephaven`` is not imported at module scope:
only table methods are called, at runtime, so the filter builders are unit-tested on a
bare host python.

Filtering is always on the **root's** attributes (``RootMoniker``/``RootSymbol``/
``RootSide``), never on a node's own: the algo and router orders carry no moniker, and
filtering a tree node by node would tear it apart. Matching on the root keeps every
tree whole.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

__all__ = [
    "QUERY_API_NAMES",
    "sanitize_id",
    "node_filters",
    "bucket_filters",
    "make_query_api",
]

QUERY_API_NAMES: Tuple[str, ...] = ("order_tree", "find_tree", "exposure")

#: Characters that could break out of a backtick-quoted query-string literal.
_FORBIDDEN = "`\"'\\\n\r\t"


def sanitize_id(value: Any) -> str:
    """Strip quoting/escape characters from a user-supplied value (same rules as doc 09)."""
    if value is None:
        return ""
    text = value if isinstance(value, str) else str(value)
    return "".join(ch for ch in text if ch not in _FORBIDDEN and ch >= " ").strip()


def _match(column: str, value: Any, prefix: bool) -> Optional[str]:
    text = sanitize_id(value)
    if not text:
        return None
    if prefix:
        # Typing "ac" should already show ACME: case-insensitive prefix match.
        return f"{column}.toUpperCase().startsWith(`{text.upper()}`)"
    return f"{column} == `{text}`"


def _filters(
    moniker: Any,
    symbol: Any,
    side: Any,
    prefix: bool,
    include_unlinked: bool,
    unlinked_clause: str,
    breaks_only: bool,
    breaks_clause: str,
) -> List[str]:
    out: List[str] = []
    by_moniker = _match("RootMoniker", moniker, prefix)
    if by_moniker:
        # An unlinked router order cannot inherit a moniker, so a moniker search would
        # hide exactly the orders most worth seeing; keep them (they still have to match
        # symbol and side).
        out.append(f"({by_moniker} || {unlinked_clause})" if include_unlinked else by_moniker)
    by_symbol = _match("RootSymbol", symbol, prefix)
    if by_symbol:
        out.append(by_symbol)
    by_side = _match("RootSide", side, False)
    if by_side:
        out.append(by_side)
    if breaks_only:
        out.append(breaks_clause)
    return out


def node_filters(
    moniker: Any = None,
    symbol: Any = None,
    side: Any = None,
    include_unlinked: bool = True,
    breaks_only: bool = False,
    prefix: bool = False,
) -> List[str]:
    """``where`` clauses for node tables (``otr_recon``, ``otr_breaks``, the tree).

    ``breaks_only`` keeps whole trees that contain a red node (``TreeBreaks > 0``).
    """
    return _filters(
        moniker, symbol, side, prefix,
        include_unlinked, "RootLinkState != `ROOT`",
        breaks_only, "TreeBreaks > 0",
    )


def bucket_filters(
    moniker: Any = None,
    symbol: Any = None,
    side: Any = None,
    include_unlinked: bool = True,
    breaks_only: bool = False,
    prefix: bool = False,
) -> List[str]:
    """``where`` clauses for bucket tables (``otr_exposure``, ``otr_level_ladder``).

    An unlinked bucket is one whose root carries no moniker (``RootMoniker == ""``).
    ``breaks_only`` needs a ``Breaks`` column -- pass ``False`` for the ladder, whose
    ``Breaks`` is per level, so a bucket's healthy levels stay visible.
    """
    return _filters(
        moniker, symbol, side, prefix,
        include_unlinked, "RootMoniker == ``",
        breaks_only, "Breaks > 0",
    )


def _where(table: Any, clauses: List[str]) -> Any:
    return table.where(clauses) if clauses else table


def make_query_api(tables: Mapping[str, Any]) -> Dict[str, Callable[..., Any]]:
    """Bind the API to the tables returned by :func:`order_tree_recon.dag.build_order_tree`."""
    from order_tree_recon.dag import TREE_COLUMNS, flat_tree

    recon = tables["otr_recon"]
    id_index = tables["otr_id_index"]
    ladder = tables["otr_level_ladder"]
    summary = tables["otr_exposure"]
    breaks = tables["otr_breaks"]

    def order_tree(moniker: Any = None, symbol: Any = None, side: Any = None,
                   include_unlinked: bool = True) -> Dict[str, Any]:
        """Everything for a (moniker, symbol, side) lookup -- each argument optional.

        Returns:
            ``{"tree", "flat", "ladder", "exposure", "breaks"}``: the native tree, the
            same trees as an indented flat table, the per-level ladder, the bucket
            exposure rows and the red nodes. Exact match on each given value.
        """
        nodes = node_filters(moniker, symbol, side, include_unlinked)
        buckets = bucket_filters(moniker, symbol, side, include_unlinked)
        subset = _where(recon, nodes)
        return {
            "tree": subset.view(list(TREE_COLUMNS)).tree("NodeKey", "ParentKey", promote_orphans=True),
            "flat": flat_tree(subset),
            "ladder": _where(ladder, buckets),
            "exposure": _where(summary, buckets),
            "breaks": _where(breaks, nodes),
        }

    def find_tree(any_id: Any) -> Any:
        """The whole tree(s) containing ``any_id`` -- an id from **any** system.

        A venue order id returns its router parent, algo child, algo parent and Raptor
        client order, plus every sibling: the tree works from either end.
        """
        value = sanitize_id(any_id)
        hits = recon.where_in(id_index.where(f"Id == `{value}`"), "NodeKey")
        return flat_tree(recon.where_in(hits.select_distinct(["RootKey"]), "RootKey"))

    def exposure(moniker: Any = None, symbol: Any = None, side: Any = None) -> Any:
        """``otr_exposure`` rows for a lookup (exact match on each given value).

        With a moniker, unlinked buckets are not added in; without one, an unattributed
        bucket that matches the symbol/side is returned like any other -- it is street
        exposure with no client order, the row most worth seeing.
        """
        return _where(summary, bucket_filters(moniker, symbol, side, include_unlinked=False))

    return {"order_tree": order_tree, "find_tree": find_tree, "exposure": exposure}
