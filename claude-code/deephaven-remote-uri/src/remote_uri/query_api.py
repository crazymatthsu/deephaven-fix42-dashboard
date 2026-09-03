"""The collector's query API -- doc 10 section 9.

Eight globals answering the assignment's question and demonstrating the three remote
mechanisms:

=============================  ==========================================================
Function                       Behaviour
=============================  ==========================================================
``find_exposure``              every hop of every family whose **root** matches
``family_totals``              the matching rows of ``exposure_by_level``
``exposure_for``               the matching rows of ``exposure_by_source`` -- the totals
``leaf_of``                    which leaf owns a hub
``remote_executions``          **remote query**: that hop's executions, from its leaf
``remote_live_executions``     the same, as a live Barrage subscription
``snapshot_leaf``              **remote snapshot**: a static copy of a leaf's stats
``reconnect``                  re-resolve, rebuild, re-export
=============================  ==========================================================

The first three return **live** tables (still DAG nodes, so a caller may subscribe);
the remote ones return what their mechanism produces. Every identifier passes
through ``multi_oms.query_api.sanitize_id`` before it is interpolated into a query
string -- twice over here, because a remote query's text is compiled *on the leaf*.

``deephaven`` is deliberately not imported at module scope: only table methods are
called, at runtime.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Tuple

from multi_oms.query_api import sanitize_id

from remote_uri import search
from remote_uri.config import CollectorSettings
from remote_uri.remote import RemoteError, RemoteResolver

__all__ = [
    "QUERY_API_NAMES",
    "EXECUTIONS_TABLE",
    "STATS_TABLE",
    "executions_expression",
    "make_query_api",
]

#: The function names exported as globals by ``app.py`` (doc 10 section 9).
QUERY_API_NAMES: Tuple[str, ...] = (
    "find_exposure",
    "family_totals",
    "exposure_for",
    "leaf_of",
    "remote_executions",
    "remote_live_executions",
    "snapshot_leaf",
    "reconnect",
)

#: The leaf-side global a remote execution query filters. Not held by the collector
#: at all (doc 10 section 2.5): executions are ~70% of the message count.
EXECUTIONS_TABLE = "oms_executions"

#: The leaf-side global a remote snapshot reads for fleet health.
STATS_TABLE = "rx_leaf_stats"


def executions_expression(global_key: str) -> str:
    """The python expression a leaf evaluates for one hop's executions.

    Args:
        global_key: An **already sanitised** ``"<Oms>|<OrderKey>"``.

    Returns:
        ``'oms_executions.where("GlobalKey == `OMS-A|A-0001`")'`` -- python string
        quotes outside, Deephaven backtick literal inside, so the two quoting layers
        never collide.
    """
    return f'{EXECUTIONS_TABLE}.where("GlobalKey == `{global_key}`")'


def _oms_of(global_key: str) -> str:
    """The hub name a ``GlobalKey`` belongs to (``"<Oms>|<OrderKey>"``)."""
    return global_key.split("|", 1)[0] if global_key else ""


def make_query_api(
    tables: Mapping[str, Any],
    resolver: RemoteResolver,
    settings: CollectorSettings,
    reconnect: Callable[[], Any],
) -> Dict[str, Callable[..., Any]]:
    """Build the query-API functions bound to one build of the collector DAG.

    Args:
        tables: The dict returned by :func:`remote_uri.collector.build_collector`.
        resolver: The collector's :class:`~remote_uri.remote.RemoteResolver`.
        settings: The validated collector configuration.
        reconnect: The runtime's rebuild callable, exported as ``reconnect``.

    Returns:
        A dict of plain functions keyed by :data:`QUERY_API_NAMES`, ready to be
        splatted into module globals by ``app.py``.
    """
    orders_marked = tables["orders_marked"]
    exposure_by_level = tables["exposure_by_level"]
    exposure_by_source = tables["exposure_by_source"]

    def find_exposure(source_oms: Any = None, account: Any = None, symbol: Any = None) -> Any:
        """Every hop of every family rooted at the given (OMS, account, symbol).

        The answer to the assignment's question: upstream -> downstream, with each
        hop's latest ``CumQty`` / ``LeavesQty`` and its marked notional exposure. A
        blank argument means "any".

        Args:
            source_oms: The family's root hub, e.g. ``"OMS-A"``.
            account: The client account (tag 1).
            symbol: The instrument (tag 55).

        Returns:
            A live view of ``orders_marked``, sorted ``RootKey, Depth, Oms, OrderKey``.
        """
        clauses = search.root_filters(source_oms, account, symbol)
        return search.apply_filters(orders_marked, clauses).sort(search.FAMILY_SORT)

    def family_totals(source_oms: Any = None, account: Any = None, symbol: Any = None) -> Any:
        """Per-level sums for the matching families (``exposure_by_level``).

        Shows *where the flow went*: one row per hub per family, so a routed-but-not
        -filled leg is visible as its own line rather than folded into a total.
        """
        clauses = search.root_filters(source_oms, account, symbol)
        return search.apply_filters(exposure_by_level, clauses)

    def exposure_for(source_oms: Any = None, account: Any = None, symbol: Any = None) -> Any:
        """**The** totals for a lookup (``exposure_by_source``, root level only).

        Summing across hubs would count one economic flow once per hop (doc 09's
        rule), so the totals are taken over the ``Depth == 0`` rows and
        :func:`family_totals` shows the per-hub breakdown.
        """
        clauses = search.root_filters(source_oms, account, symbol)
        return search.apply_filters(exposure_by_source, clauses)

    def leaf_of(oms: Any) -> str:
        """The name of the leaf folding hub ``oms`` (``""`` when no leaf owns it)."""
        leaf = settings.leaf_of(sanitize_id(oms))
        return leaf.name if leaf is not None else ""

    def remote_executions(global_key: Any) -> Any:
        """**Remote query**: one hop's executions, fetched from the owning leaf.

        The filter runs *on the leaf* -- ``oms_executions`` never leaves it, and only
        the matching rows cross the wire. The transient ``rx_q_<n>`` global is
        deleted as soon as the snapshot has been taken.

        Args:
            global_key: The cross-hub key ``"<Oms>|<OrderKey>"``.

        Returns:
            A **static** table of that hop's executions.

        Raises:
            RemoteError: If the key names no configured hub, or the leaf is down.
        """
        key = sanitize_id(global_key)
        leaf = _require_leaf(key)
        return resolver.query_snapshot(leaf, executions_expression(key))

    def remote_live_executions(global_key: Any) -> Any:
        """The same as a **live** Barrage subscription.

        Leaves its ``rx_q_<n>`` global bound on the leaf (deleting it would pull the
        subscription's source away); ``reconnect()`` drops them.
        """
        key = sanitize_id(global_key)
        leaf = _require_leaf(key)
        return resolver.query_live(leaf, executions_expression(key))

    def snapshot_leaf(name: Any) -> Any:
        """**Remote snapshot**: a static copy of one leaf's ``rx_leaf_stats``."""
        return resolver.snapshot(sanitize_id(name), STATS_TABLE)

    def _require_leaf(global_key: str) -> str:
        """Resolve the leaf that owns a ``GlobalKey``, or say why it cannot."""
        if not global_key:
            raise RemoteError(
                "a GlobalKey is required, e.g. 'OMS-A|A-0001' "
                "(the GlobalKey column of orders_marked)"
            )
        oms = _oms_of(global_key)
        leaf = settings.leaf_of(oms)
        if leaf is None:
            raise RemoteError(
                f"no configured leaf folds hub {oms!r} (from GlobalKey {global_key!r}); "
                f"leaves own {[(l.name, list(l.hubs)) for l in settings.leaves]}"
            )
        return leaf.name

    return {
        "find_exposure": find_exposure,
        "family_totals": family_totals,
        "exposure_for": exposure_for,
        "leaf_of": leaf_of,
        "remote_executions": remote_executions,
        "remote_live_executions": remote_live_executions,
        "snapshot_leaf": snapshot_leaf,
        "reconnect": reconnect,
    }
