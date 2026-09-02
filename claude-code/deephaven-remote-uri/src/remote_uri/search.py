"""The (source OMS, account, symbol) lookup's filters -- doc 10 section 9.

The assignment's question -- *given a source OMS, a client account and a symbol,
show every hop upstream -> downstream* -- is one clause list applied to three
tables: ``orders_marked`` (every hop), ``exposure_by_level`` (per-level sums) and
``exposure_by_source`` (the totals). All three carry ``RootOms`` / ``RootAccount`` /
``RootSymbol``, so the filter is built once here and reused, which is what keeps the
three panels of the dashboard consistent with each other by construction.

The match is on the **root** of the family, never on the hop: asking for
``("OMS-A", "ACC-1", "NVDA")`` must return the ``OMS-C`` legs of those families too,
and those legs carry their own ``Oms`` (``OMS-C``) while sharing the root's columns.

Every value passes through ``multi_oms.query_api.sanitize_id`` before it reaches an
f-string that becomes a compiled Java filter; a blank value means "any" and
contributes no clause at all rather than an always-true one.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence, Tuple

from multi_oms.query_api import sanitize_id

__all__ = [
    "ROOT_COLUMNS",
    "FAMILY_SORT",
    "root_filters",
    "describe_filters",
    "apply_filters",
]

#: The three columns a lookup matches on, in argument order.
ROOT_COLUMNS: Tuple[str, str, str] = ("RootOms", "RootAccount", "RootSymbol")

#: Canonical ordering of the hops: families grouped, then upstream -> downstream.
#: Identical to doc 09's ``CHAIN_SORT`` but rooted on the cross-server ``RootKey``.
FAMILY_SORT: List[str] = ["RootKey", "Depth", "Oms", "OrderKey"]


def root_filters(
    source_oms: Any = None,
    account: Any = None,
    symbol: Any = None,
) -> List[str]:
    """Build the ``where`` clauses for one lookup.

    Args:
        source_oms: The family's root hub (``RootOms``); blank = any.
        account: The client account (``RootAccount``); blank = any.
        symbol: The instrument (``RootSymbol``); blank = any.

    Returns:
        Zero to three ``==`` clauses, in :data:`ROOT_COLUMNS` order. An empty list
        means "no filter", which callers apply as the unfiltered table -- not as a
        compiled always-true ``where``.
    """
    clauses: List[str] = []
    for column, value in zip(ROOT_COLUMNS, (source_oms, account, symbol)):
        cleaned = sanitize_id(value)
        if cleaned:
            clauses.append(f"{column} == `{cleaned}`")
    return clauses


def describe_filters(
    source_oms: Any = None,
    account: Any = None,
    symbol: Any = None,
) -> str:
    """Human-readable form of a lookup, for panel titles and log lines."""
    parts = []
    for label, value in zip(("oms", "account", "symbol"), (source_oms, account, symbol)):
        cleaned = sanitize_id(value)
        parts.append(f"{label}={cleaned or '*'}")
    return " ".join(parts)


def apply_filters(table: Any, clauses: Optional[Sequence[str]]) -> Any:
    """Apply :func:`root_filters`' output to a table.

    ``deephaven`` is not imported: only ``Table.where`` is called, so this stays
    unit-testable against a stub.

    Args:
        table: Any Deephaven table carrying :data:`ROOT_COLUMNS`.
        clauses: The clause list; empty/``None`` returns ``table`` unchanged.

    Returns:
        The filtered (or original) table.
    """
    if not clauses:
        return table
    return table.where(list(clauses))
