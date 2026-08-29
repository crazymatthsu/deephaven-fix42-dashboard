"""Declarative derived nodes -- doc 09 section 5.

Everything here is a pure, incrementally-computed table operation over the blink
streams published by :class:`multi_oms.pipeline.MultiOmsPipeline`.  No state, no
python callbacks: late/duplicate data is already resolved inside ``fix42cache``, and
a link that cannot yet be resolved simply *heals* when the upstream tape catches up
(a ``DANGLING`` order becomes ``LINKED`` with no replay).

The two ideas the whole module rests on:

``RootKey``
    the chain id.  Every hop of a family shares it, which is what makes "select any
    hop, see the whole chain, both directions" a single ``where``.  It is resolved by
    ``K-1`` iterated ``natural_join``s up the parent pointers (``K`` = number of
    hubs), so a data cycle cannot loop -- it just stops resolving past ``K`` hops.

per-**edge** reconciliation
    a parent is compared against the sum over its *direct* children only, never
    against its whole subtree (that would double-count mid hops) and never by
    overwriting a hop's own authoritative values (issue #10).
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

from deephaven import agg, new_table
from deephaven.column import long_col, string_col
from deephaven.stream import blink_to_append_only
from deephaven.table import Table

from multi_oms import config
from multi_oms.config import Topology
from multi_oms.linking import sanitize_hub

try:  # `merge` is re-exported at the package root on every server we target
    from deephaven import merge
except ImportError:  # pragma: no cover - keep working if the re-export ever moves
    from deephaven.table_operations import merge

__all__ = [
    "DERIVED_TABLE_NAMES",
    "BREAK_KINDS",
    "RED_BREAK_KINDS",
    "build_derived",
]

#: Every global published by :func:`build_derived`, in dependency order.
#: ``orders_tree`` is appended only when the server accepts ``Table.tree`` (see below).
DERIVED_TABLE_NAMES: Tuple[str, ...] = (
    "oms_orders_latest",
    "oms_executions",
    "oms_executions_latest",
    "oms_events",
    "oms_fix_messages",
    "id_index",
    "hub_config",
    "orders_linked",
    "child_rollup",
    "orders_recon",
    "oms_breaks",
    "break_summary",
    "chain_summary",
    "chain_recon",
    "account_list",
    "symbol_list",
    "side_list",
    "oms_list",
)

#: The doc 09 section 5.4 break taxonomy.
BREAK_KINDS: Tuple[str, ...] = (
    "QTY_BREAK",
    "NOTIONAL_BREAK",
    "DANGLING",
    "NO_LINK",
    "UNROUTED",
    "NONE",
)

#: The subset the UI colors red (``UNROUTED`` is amber, ``NONE`` is quiet).
RED_BREAK_KINDS: Tuple[str, ...] = ("QTY_BREAK", "NOTIONAL_BREAK", "DANGLING", "NO_LINK")


def _lit(value: float) -> str:
    """Render a python float as a Java double literal for a query string.

    ``repr`` yields ``'1e-06'`` / ``'0.01'``, both valid Java floating-point
    literals.  Interpolating the *value* rather than binding a query-scope
    parameter keeps the formulas self-contained: the compiled filter carries the
    tolerance it was built with, so nothing can silently retune a live DAG.
    """
    return repr(float(value))


def _hub_config_table(topology: Topology) -> Table:
    """Build the tiny static ``hub_config`` table (doc 09 section 5.3).

    ``UpstreamOms`` is ``""`` for roots (not null) so the ``LinkState`` formula can
    compare it with ``==`` without a null guard; ``LinkTag`` is ``0`` for roots, which
    is not a legal FIX tag and therefore unambiguous.
    """
    hubs = list(topology)
    return new_table(
        [
            string_col("Oms", [hub.name for hub in hubs]),
            string_col("UpstreamOms", [hub.upstream for hub in hubs]),
            long_col("LinkTag", [int(hub.link_tag) for hub in hubs]),
            long_col("HubDepth", [int(hub.depth) for hub in hubs]),
            string_col("Topic", [hub.topic for hub in hubs]),
        ]
    )


def _build_id_index(oms_events: Table, oms_orders_latest: Table) -> Table:
    """``(Oms, Id) -> GlobalKey`` over every id a hub order has ever carried.

    The link value may be the upstream order's ``ClOrdID`` **or** ``OrderID``, and an
    amend rotates ``ClOrdID`` -- so the index must cover every id ever seen (doc 03's
    ``clordid_index`` trick, namespaced by hub).  Both sides are ``view``-normalized
    to the same three columns before the merge so column order cannot drift.
    """
    clordid_ids = (
        oms_events.where("ClOrdID != ``")
        .view(["Oms", "Id = ClOrdID", "GlobalKey"])
        .last_by(["Oms", "Id"])
        .view(["Oms", "Id", "GlobalKey"])
    )
    orderid_ids = oms_orders_latest.where("OrderID != ``").view(
        ["Oms", "Id = OrderID", "GlobalKey"]
    )
    # A ClOrdID colliding with an OrderID string *within one hub* resolves to the
    # later writer; id schemes make this implausible and it is documented, not
    # defended (doc 09 section 5.2).
    return merge([clordid_ids, orderid_ids]).last_by(["Oms", "Id"]).view(
        ["Oms", "Id", "GlobalKey"]
    )


def _build_orders_linked(
    topology: Topology, oms_orders_latest: Table, hub_config: Table, id_index: Table
) -> Table:
    """Resolve each order's parent, then its root and depth (doc 09 section 5.3)."""
    linked = (
        oms_orders_latest.natural_join(
            hub_config, on=["Oms"], joins=["UpstreamOms", "HubDepth"]
        )
        .natural_join(
            id_index,
            on=["UpstreamOms=Oms", "ExtOrdID=Id"],
            joins=["ParentGlobalKey=GlobalKey"],
        )
        .update_view(
            [
                "LinkState = ExtOrdID == `` ? (UpstreamOms == `` ? `ROOT` : `NO_LINK`)"
                " : (ParentGlobalKey == null ? `DANGLING` : `LINKED`)"
            ]
        )
    )

    # Transitive root + depth by K-1 iterated joins. `oms_orders_latest` is
    # last_by(Oms, OrderKey) and GlobalKey = Oms|OrderKey, so parent_map is unique on
    # GlobalKey -- the precondition natural_join needs.
    parent_map = linked.view(["GlobalKey", "ParentGlobalKey"])
    walked = linked.update(
        [
            "RootKey = ParentGlobalKey == null ? GlobalKey : ParentGlobalKey",
            "Depth = ParentGlobalKey == null ? 0 : 1",
        ]
    )
    for _ in range(max(0, len(topology) - 1)):
        walked = (
            walked.natural_join(
                parent_map, on=["RootKey=GlobalKey"], joins=["NextUp=ParentGlobalKey"]
            )
            .update(
                [
                    "Depth = NextUp == null ? Depth : Depth + 1",
                    "RootKey = NextUp == null ? RootKey : NextUp",
                ]
            )
            .drop_columns(["NextUp"])
        )

    # Notional is defined here, not in orders_recon: child_rollup sums it over the
    # direct children and is built from *this* table (doc 09 section 5.4's snippet
    # references it one step before it defines it).
    return walked.update_view(
        ["Notional = (isNull(AvgPx) ? 0.0 : AvgPx) * (isNull(CumQty) ? 0.0 : CumQty)"]
    )


def _build_child_rollup(orders_linked: Table) -> Table:
    """Sum each parent's **direct** children (doc 09 section 5.4)."""
    return orders_linked.where("ParentGlobalKey != null").agg_by(
        [
            agg.count_("ChildCount"),
            agg.sum_(
                [
                    "ChildOrderQty = OrderQty",
                    "ChildCumQty = CumQty",
                    "ChildLeavesQty = LeavesQty",
                    "ChildNotional = Notional",
                ]
            ),
        ],
        by=["ParentGlobalKey"],
    )


def _build_orders_recon(
    orders_linked: Table, child_rollup: Table, qty_tol: float, notional_tol: float
) -> Table:
    """THE blotter table: per-edge deltas, ``BreakKind`` and ``OnBrokenEdge``."""
    qty = _lit(qty_tol)
    notional = _lit(notional_tol)

    recon = orders_linked.natural_join(
        child_rollup,
        on=["GlobalKey=ParentGlobalKey"],
        joins=["ChildCount", "ChildOrderQty", "ChildCumQty", "ChildLeavesQty", "ChildNotional"],
    ).update_view(
        [
            "HasChildren = !isNull(ChildCount)",
            "DeltaCumQty = HasChildren ? CumQty - ChildCumQty : NULL_DOUBLE",
            "DeltaLeavesQty = HasChildren ? LeavesQty - ChildLeavesQty : NULL_DOUBLE",
            "DeltaNotional = HasChildren ? Notional - ChildNotional : NULL_DOUBLE",
            # `HasChildren &&` short-circuits, so abs() never sees a NULL_DOUBLE.
            f"EdgeBreak = HasChildren && (abs(DeltaCumQty) > {qty}"
            f" || abs(DeltaNotional) > {notional})",
        ]
    )

    # Second pass: pull the parent's EdgeBreak down so the *child* side of a broken
    # edge is visible too (the blotter must show both ends of the discrepancy).
    # This is a diamond over `recon`, not a cycle -- the same shape as parent_map.
    return recon.natural_join(
        recon.view(["GlobalKey", "ParentEdgeBreak = EdgeBreak"]),
        on=["ParentGlobalKey=GlobalKey"],
        joins=["ParentEdgeBreak"],
    ).update_view(
        [
            # Doc 09 writes `ParentEdgeBreak == true`; the isNull guard is added
            # because the joined column is a *boxed* Boolean that is null for every
            # root and every orphan, and unboxing a null in the generated Java would
            # throw. Semantics are identical (null reads as "no parent break").
            "OnBrokenEdge = EdgeBreak || (!isNull(ParentEdgeBreak) && ParentEdgeBreak)",
            "BreakKind = (LinkState == `DANGLING` || LinkState == `NO_LINK`) ? LinkState"
            f" : (HasChildren && abs(DeltaCumQty) > {qty}) ? `QTY_BREAK`"
            f" : (HasChildren && abs(DeltaNotional) > {notional}) ? `NOTIONAL_BREAK`"
            f" : (HasChildren && abs(DeltaLeavesQty) > {qty}) ? `UNROUTED`"
            " : `NONE`",
        ]
    )


def _build_chain_summary(orders_recon: Table) -> Tuple[Table, str]:
    """Per (``RootKey``, ``Oms``) level sums (doc 09 section 5.5).

    Returns:
        ``(table, how)`` where ``how`` names the aggregation route actually taken --
        the doc's ``agg.max_`` over the Boolean, or the int-flag fallback for servers
        whose max aggregation rejects a Boolean column. Both yield an identical
        ``MaxBreak`` Boolean column; the banner reports which ran.
    """
    aggs = [
        agg.count_("Orders"),
        agg.sum_(["CumQty", "LeavesQty", "Notional", "OrderQty"]),
    ]
    try:
        summary = orders_recon.agg_by(
            aggs + [agg.max_(["MaxBreak = OnBrokenEdge"])], by=["RootKey", "Oms"]
        )
        return summary, "agg.max_(Boolean)"
    except Exception as exc:  # noqa: BLE001 - max over a Boolean is the version-fragile bit
        print(
            f"[multi-oms] agg.max_ over the OnBrokenEdge Boolean was rejected "
            f"({type(exc).__name__}: {exc}); using the int-flag route instead"
        )
        summary = (
            orders_recon.update_view(["BrokenFlag = OnBrokenEdge ? 1 : 0"])
            .agg_by(aggs + [agg.max_(["MaxBreakFlag = BrokenFlag"])], by=["RootKey", "Oms"])
            .update_view(["MaxBreak = MaxBreakFlag > 0"])
            .drop_columns(["MaxBreakFlag"])
        )
        return summary, "agg.max_(int flag) fallback"


def _build_chain_recon(
    topology: Topology, chain_summary: Table, qty_tol: float, notional_tol: float
) -> Table:
    """Pivot ``chain_summary`` into one row per family with per-hub columns.

    Level sums *are* comparable per chain (each hub reports the same economic flow),
    so the pivot answers "which chain, and between which two systems" at a glance;
    the per-order ``orders_recon`` rows then localize the exact edge.
    """
    qty = _lit(qty_tol)
    notional = _lit(notional_tol)

    pivot = chain_summary.select_distinct(["RootKey"])
    for hub in topology:
        suffix = sanitize_hub(hub.name)
        columns = [
            f"Orders_{suffix}",
            f"CumQty_{suffix}",
            f"LeavesQty_{suffix}",
            f"Notional_{suffix}",
        ]
        side = chain_summary.where(f"Oms == `{hub.name}`").view(
            [
                "RootKey",
                f"Orders_{suffix} = Orders",
                f"CumQty_{suffix} = CumQty",
                f"LeavesQty_{suffix} = LeavesQty",
                f"Notional_{suffix} = Notional",
            ]
        )
        pivot = pivot.natural_join(side, on=["RootKey"], joins=columns)

    # One QtyBreak/NotionalBreak per *configured edge*: a chain-level break is
    # attributable to the two systems whose level sums disagree. A hub absent from a
    # chain is not a break -- only both-present-and-different is.
    edge_formulas = []
    qty_flags = []
    notional_flags = []
    for hub in topology.linked_hubs:
        child = sanitize_hub(hub.name)
        parent = sanitize_hub(hub.upstream)
        qty_flags.append(f"QtyBreak_{child}")
        notional_flags.append(f"NotionalBreak_{child}")
        edge_formulas.append(
            f"QtyBreak_{child} = (isNull(CumQty_{child}) || isNull(CumQty_{parent}))"
            f" ? false : abs(CumQty_{child} - CumQty_{parent}) > {qty}"
        )
        edge_formulas.append(
            f"NotionalBreak_{child} = (isNull(Notional_{child}) || isNull(Notional_{parent}))"
            f" ? false : abs(Notional_{child} - Notional_{parent}) > {notional}"
        )
    edge_formulas.append(
        "QtyBreak = " + (" || ".join(qty_flags) if qty_flags else "false")
    )
    edge_formulas.append(
        "NotionalBreak = " + (" || ".join(notional_flags) if notional_flags else "false")
    )
    return pivot.update_view(edge_formulas)


def build_derived(
    topology: Topology,
    streams: Mapping[str, Table],
    qty_tol: Optional[float] = None,
    notional_tol: Optional[float] = None,
) -> Dict[str, Any]:
    """Build the whole doc 09 section 5 DAG from the publisher blink tables.

    Args:
        topology: The validated hub graph (doc 09 section 3).
        streams: The blink tables keyed by their doc 09 section 4.1 names.
        qty_tol: Absolute qty tolerance; defaults to :func:`config.qty_tolerance`.
        notional_tol: Absolute notional tolerance; defaults to
            :func:`config.notional_tolerance`.

    Returns:
        A dict keyed by :data:`DERIVED_TABLE_NAMES` (plus ``orders_tree`` when the
        server accepts it):

        ``oms_orders_latest``
            THE cache: ``last_by(["Oms", "OrderKey"])``, O(#orders) not O(#messages).
        ``oms_executions`` / ``oms_events`` / ``oms_fix_messages``
            Append-only per-hop history; quantities are never summed across hubs here.
        ``oms_executions_latest``
            ``last_by(["Oms", "ExecID"])`` -- disposition after bust/correct/DK.
        ``id_index``
            ``(Oms, Id) -> GlobalKey`` over every id ever seen.
        ``hub_config``
            The running topology, as a panel.
        ``orders_linked``
            ``+UpstreamOms +ParentGlobalKey +LinkState +RootKey +Depth +Notional``.
        ``child_rollup``
            Per-parent sums over its *direct* children.
        ``orders_recon``
            THE blotter table: deltas, ``BreakKind``, ``OnBrokenEdge``.
        ``oms_breaks`` / ``break_summary`` / ``chain_summary`` / ``chain_recon``
            The rollup views ("which system", "which chain, between which systems").
        ``orders_tree``
            Hierarchical companion panel; omitted when ``Table.tree`` is unavailable.
        ``account_list`` / ``symbol_list`` / ``side_list`` / ``oms_list``
            Dashboard filter sources.
    """
    qty = config.qty_tolerance() if qty_tol is None else float(qty_tol)
    notional = config.notional_tolerance() if notional_tol is None else float(notional_tol)

    order_state_blink = streams["oms_order_state_blink"]
    executions_blink = streams["oms_executions_blink"]
    order_events_blink = streams["oms_order_events_blink"]
    fix_messages_blink = streams["oms_fix_messages_blink"]

    # -- 5.1 caches and history --------------------------------------------------
    oms_orders_latest = order_state_blink.last_by(["Oms", "OrderKey"])
    oms_executions = blink_to_append_only(executions_blink)
    oms_executions_latest = executions_blink.last_by(["Oms", "ExecID"])
    oms_events = blink_to_append_only(order_events_blink)
    oms_fix_messages = blink_to_append_only(fix_messages_blink)

    # -- 5.2 / 5.3 identity, linking, roots, depth -------------------------------
    id_index = _build_id_index(oms_events, oms_orders_latest)
    hub_config = _hub_config_table(topology)
    orders_linked = _build_orders_linked(topology, oms_orders_latest, hub_config, id_index)

    # -- 5.4 per-edge reconciliation ---------------------------------------------
    child_rollup = _build_child_rollup(orders_linked)
    orders_recon = _build_orders_recon(orders_linked, child_rollup, qty, notional)

    # -- 5.5 rollup views ---------------------------------------------------------
    oms_breaks = orders_recon.where("BreakKind != `NONE` && BreakKind != `UNROUTED`")
    break_summary = orders_recon.where("BreakKind != `NONE`").count_by(
        "Count", by=["Oms", "BreakKind"]
    )
    chain_summary, chain_summary_how = _build_chain_summary(orders_recon)
    chain_recon = _build_chain_recon(topology, chain_summary, qty, notional)

    # -- filter sources -----------------------------------------------------------
    # orders_recon is a row-preserving transform of oms_orders_latest, so distincts
    # taken from the cheaper table cover exactly the blotter's domain.
    account_list = oms_orders_latest.select_distinct(["Account"]).sort(["Account"])
    symbol_list = oms_orders_latest.select_distinct(["Symbol"]).sort(["Symbol"])
    side_list = oms_orders_latest.select_distinct(["Side"]).sort(["Side"])
    oms_list = hub_config.view(["Oms"])

    tables: Dict[str, Any] = {
        "oms_orders_latest": oms_orders_latest,
        "oms_executions": oms_executions,
        "oms_executions_latest": oms_executions_latest,
        "oms_events": oms_events,
        "oms_fix_messages": oms_fix_messages,
        "id_index": id_index,
        "hub_config": hub_config,
        "orders_linked": orders_linked,
        "child_rollup": child_rollup,
        "orders_recon": orders_recon,
        "oms_breaks": oms_breaks,
        "break_summary": break_summary,
        "chain_summary": chain_summary,
        "chain_recon": chain_recon,
        "account_list": account_list,
        "symbol_list": symbol_list,
        "side_list": side_list,
        "oms_list": oms_list,
    }

    # The native hierarchical panel: expand/collapse upstream -> downstream with the
    # recon columns on every node. A companion to the flat blotter, never a
    # dependency of it -- issue #10's caveats about tree selection stand, so a server
    # that rejects `tree` costs the panel and nothing else.
    tree = _build_tree(orders_recon)
    if tree is not None:
        tables["orders_tree"] = tree

    tables["_chain_summary_how"] = chain_summary_how
    return tables


def _build_tree(orders_recon: Table) -> Optional[Any]:
    """``orders_recon.tree("GlobalKey", "ParentGlobalKey", promote_orphans=True)``.

    Returns ``None`` (with a log line) when the server's ``Table.tree`` is absent or
    rejects the arguments -- ``promote_orphans`` in particular is load-bearing here:
    a ``DANGLING`` order has a non-null parent pointer that resolves to nothing, and
    without promotion the whole subtree would vanish from the panel.
    """
    try:
        return orders_recon.tree("GlobalKey", "ParentGlobalKey", promote_orphans=True)
    except Exception as exc:  # noqa: BLE001 - optional companion panel
        print(f"[multi-oms] orders_tree unavailable ({type(exc).__name__}: {exc}); "
              "the flat blotter and every other table are unaffected")
        return None
