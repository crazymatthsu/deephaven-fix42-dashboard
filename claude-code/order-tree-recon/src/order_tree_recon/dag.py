"""The order-tree DAG -- doc 14 section 4.

Input: one table per level, already mapped onto ``config.CANONICAL_COLUMNS`` by an
adapter (:mod:`order_tree_recon.sources` has the mock ones; a real deployment writes its
own ``view``s over its Raptor / algo / router tables). Everything below is declarative
and incremental: when a fill ticks on a venue order, only that tree's rows recompute.

::

    level tables --canonicalize--> otr_nodes (last_by NodeKey)
                                     |-> otr_id_index   (System, Id) -> NodeKey over OrderId + AltId
    otr_levels (static) -------------+-> parent resolution: one natural_join per candidate system
                                     |-> root walk: max_depth natural_joins -> RootKey, Depth, TreePath
                                     |-> root attributes: RootMoniker / RootSymbol / RootSide on every node
                                     |-> direct-children rollup -> FillGap, RouteGap, BreakKind
                                     '-> otr_recon --> otr_tree, otr_tree_flat, otr_level_ladder,
                                                       otr_exposure, otr_breaks, filter lists

The python twin of every formula is :mod:`order_tree_recon.reference`; the embedded
engine test asserts both produce identical rows.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from deephaven import agg, merge, new_table
from deephaven.column import bool_col, int_col, string_col
from deephaven.table import Table

from order_tree_recon import config, reference
from order_tree_recon.config import LevelSpec, Topology

__all__ = [
    "TABLE_NAMES",
    "RECON_COLUMNS",
    "TREE_COLUMNS",
    "canonical_level",
    "build_order_tree",
]

#: Every table global :func:`build_order_tree` returns, in dependency order.
TABLE_NAMES: Tuple[str, ...] = (
    "otr_nodes",
    "otr_id_index",
    "otr_levels",
    "otr_recon",
    "otr_tree",
    "otr_tree_flat",
    "otr_level_ladder",
    "otr_exposure",
    "otr_breaks",
    "otr_break_summary",
    "otr_moniker_list",
    "otr_symbol_list",
    "otr_side_list",
)

#: ``otr_recon`` column order: what a person reads first comes first.
RECON_COLUMNS: Tuple[str, ...] = (
    "Level",
    "OrderId",
    "System",
    "Status",
    "Destination",
    "Symbol",
    "Side",
    "OrderQty",
    "CumQty",
    "LeavesQty",
    "AvgPx",
    "ChildCount",
    "ChildCumQty",
    "ChildLeavesQty",
    "FillGap",
    "RouteGap",
    "BreakKind",
    "OnBrokenEdge",
    "LinkState",
    "LinkId",
    "AltId",
    "Moniker",
    "RootMoniker",
    "RootSymbol",
    "RootSide",
    "RootLevel",
    "RootOrderId",
    "RootLinkState",
    "RootResolved",
    "Depth",
    "LevelNo",
    "IsStreet",
    "HasChildren",
    "ChildOrderQty",
    "ExecNotional",
    "HeldQty",
    "OverRoutedQty",
    "UnbookedQty",
    "PhantomQty",
    "ClientOrderQty",
    "ClientCumQty",
    "ClientLeavesQty",
    "StreetCumQty",
    "StreetLeavesQty",
    "Trees",
    "UnlinkedTrees",
    "Breaks",
    "TreeBreaks",
    "ParentSymbol",
    "ParentSide",
    "TreePath",
    "NodeKey",
    "ParentKey",
    "RootKey",
)

#: The tree panel: the node, its own numbers, the edge below it, then the keys.
TREE_COLUMNS: Tuple[str, ...] = (
    "Level",
    "OrderId",
    "BreakKind",
    "Status",
    "OrderQty",
    "CumQty",
    "LeavesQty",
    "FillGap",
    "RouteGap",
    "ChildCumQty",
    "ChildLeavesQty",
    "AvgPx",
    "Destination",
    "Side",
    "OnBrokenEdge",
    "LinkState",
    "LinkId",
    "System",
    "Symbol",
    "RootMoniker",
    "Depth",
    "NodeKey",
    "ParentKey",
    "RootKey",
)

_SEP = reference.PATH_SEP


def _lit(value: float) -> str:
    """A python float as a Java double literal (``repr`` gives ``1e-06`` / ``0.5``)."""
    return repr(float(value))


def _any_of(column: str, values: Sequence[str]) -> str:
    """``(C == `a` || C == `b`)`` -- ``in`` is a filter, not a formula operator."""
    if not values:
        return "false"
    return "(" + " || ".join(f"{column} == `{value}`" for value in values) + ")"


def canonical_level(table: Table, level: LevelSpec) -> Table:
    """Project one adapter table onto the canonical columns and stamp its level.

    Null strings become ``""`` and null quantities ``0.0`` (a null would poison every
    sum above it); ``AvgPx`` keeps its null. The ``(double)`` casts accept int/long
    quantity columns and keep a null long null.
    """
    formulas: List[str] = [f"{c} = isNull({c}) ? `` : {c}" for c in config.STRING_COLUMNS]
    formulas += [f"{c} = isNull({c}) ? 0.0 : (double) {c}" for c in config.QTY_COLUMNS]
    formulas.append("AvgPx = (double) AvgPx")
    canonical = table.view(formulas)
    return canonical.view(
        list(config.CANONICAL_COLUMNS)
        + [
            f"Level = `{level.name}`",
            f"LevelNo = (int) {level.level_no}",
            f"System = `{level.system}`",
            f"IsStreet = {'true' if level.street else 'false'}",
            "NodeKey = System + `|` + OrderId",
        ]
    )


def _levels_table(topology: Topology) -> Table:
    """``otr_levels``: Level, LevelNo, System, IsRootLevel, IsStreet, Cand_1..Cand_m."""
    levels = list(topology)
    columns = [
        string_col("Level", [level.name for level in levels]),
        int_col("LevelNo", [level.level_no for level in levels]),
        string_col("System", [level.system for level in levels]),
        bool_col("IsRootLevel", [level.is_root for level in levels]),
        bool_col("IsStreet", [level.street for level in levels]),
    ]
    for i in range(topology.max_parents):
        columns.append(
            string_col(
                f"Cand_{i + 1}",
                [level.parents[i] if i < len(level.parents) else "" for level in levels],
            )
        )
    return new_table(columns)


def _id_index(nodes: Table) -> Table:
    """``(System, Id) -> NodeKey`` over ``OrderId`` and ``AltId``.

    A downstream system may reference either id (DMA flow stamps Raptor's outbound
    ``ClOrdID``, the algo server stores Raptor's ``OrderId``), so both are indexed in the
    owning system's namespace. On a collision the ``AltId`` row wins (merge order).
    """
    by_order = nodes.where("OrderId != ``").view(["System", "Id = OrderId", "NodeKey"])
    by_alt = nodes.where("AltId != ``").view(["System", "Id = AltId", "NodeKey"])
    return merge([by_order, by_alt]).last_by(["System", "Id"]).view(["System", "Id", "NodeKey"])


def _link(topology: Topology, nodes: Table, levels: Table, id_index: Table) -> Table:
    """Resolve ``ParentKey`` by probing each candidate system in preference order."""
    m = topology.max_parents
    cands = [f"Cand_{i + 1}" for i in range(m)]
    linked = nodes.natural_join(levels, on=["Level"], joins=["IsRootLevel"] + cands)
    for i in range(m):
        linked = linked.natural_join(
            id_index, on=[f"Cand_{i + 1}=System", "LinkId=Id"], joins=[f"Hit_{i + 1}=NodeKey"]
        )
    parent = "(String) null"
    for i in reversed(range(m)):
        parent = f"!isNull(Hit_{i + 1}) ? Hit_{i + 1} : ({parent})"
    linked = linked.update(
        [
            f"ParentKey = {parent}",
            "LinkState = IsRootLevel ? `ROOT` : (LinkId == `` ? `NO_LINK`"
            " : (isNull(ParentKey) ? `ORPHAN` : `LINKED`))",
        ]
    ).drop_columns(cands + [f"Hit_{i + 1}" for i in range(m)] + ["IsRootLevel"])
    # The parent's own symbol/side, for the MISMATCH check (a diamond, not a cycle).
    return linked.natural_join(
        linked.view(["NodeKey", "ParentSymbol = Symbol", "ParentSide = Side"]),
        on=["ParentKey=NodeKey"],
        joins=["ParentSymbol", "ParentSide"],
    )


def _walk(topology: Topology, linked: Table) -> Table:
    """``RootKey`` / ``Depth`` / ``TreePath`` / ``RootResolved`` by bounded iterated joins.

    Each pass climbs one level. ``Up`` is the next ancestor still to visit; when it is
    still non-null after ``max_depth`` passes the tree is deeper than the topology allows
    (or the data has a cycle) and the node is flagged ``TOO_DEEP`` -- otherwise it would
    silently inherit an intermediate node's (blank) moniker and drop out of searches.
    """
    parent_map = linked.view(["NodeKey", "ParentKey"])
    walked = linked.update(["RootKey = NodeKey", "Depth = (int) 0", "TreePath = NodeKey", "Up = ParentKey"])
    for _ in range(topology.max_depth):
        walked = (
            walked.natural_join(parent_map, on=["Up=NodeKey"], joins=["NextUp=ParentKey"])
            .update(
                [
                    f"TreePath = isNull(Up) ? TreePath : Up + `{_SEP}` + TreePath",
                    "RootKey = isNull(Up) ? RootKey : Up",
                    "Depth = isNull(Up) ? Depth : Depth + 1",
                    "Up = isNull(Up) ? Up : NextUp",
                ]
            )
            .drop_columns(["NextUp"])
        )
    return walked.update(["RootResolved = isNull(Up)"]).drop_columns(["Up"])


def _with_roots(walked: Table) -> Table:
    """Copy the root's moniker/symbol/side onto every node of its tree.

    Only Raptor knows the moniker; this is what lets a moniker search return the algo
    and router orders under it, and what buckets every level of a flow together.
    """
    roots = walked.view(
        [
            "RootKey = NodeKey",
            "RootMoniker = Moniker",
            "RootSymbol = Symbol",
            "RootSide = Side",
            "RootLevel = Level",
            "RootOrderId = OrderId",
            "RootLinkState = LinkState",
        ]
    )
    return walked.natural_join(
        roots,
        on=["RootKey"],
        joins=["RootMoniker", "RootSymbol", "RootSide", "RootLevel", "RootOrderId", "RootLinkState"],
    )


def _child_rollup(rooted: Table) -> Table:
    """Per parent, the sums over its **direct** children (never the whole subtree)."""
    return rooted.where("!isNull(ParentKey)").agg_by(
        [
            agg.count_("ChildCount"),
            agg.sum_(["ChildOrderQty = OrderQty", "ChildCumQty = CumQty", "ChildLeavesQty = LeavesQty"]),
        ],
        by=["ParentKey"],
    )


def _break_kind(tol: str) -> str:
    """The doc 14 section 5 taxonomy as one formula, in ``config.BREAK_KINDS`` order."""
    return (
        "BreakKind = (LinkState == `ORPHAN` || LinkState == `NO_LINK`) ? LinkState"
        " : !RootResolved ? `TOO_DEEP`"
        " : (HasParent && (Symbol != ParentSymbol || Side != ParentSide)) ? `MISMATCH`"
        f" : CumQty > OrderQty + {tol} ? `OVERFILL`"
        f" : FillGap < -{tol} ? `UNBOOKED_FILL`"
        f" : FillGap > {tol} ? `PHANTOM_FILL`"
        f" : (RouteGap < -{tol} && LeavesQty <= {tol}) ? `CHILD_STILL_OPEN`"
        f" : RouteGap < -{tol} ? `OVER_ROUTED`"
        f" : RouteGap > {tol} ? `HELD`"
        " : `NONE`"
    )


def _recon(rooted: Table, child_rollup: Table, qty_tol: float) -> Table:
    """Per-edge gaps, the exposure columns and ``BreakKind`` (doc 14 section 5)."""
    tol = _lit(qty_tol)
    recon = rooted.natural_join(
        child_rollup,
        on=["NodeKey=ParentKey"],
        joins=["ChildCount", "ChildOrderQty", "ChildCumQty", "ChildLeavesQty"],
    ).update(
        [
            "HasChildren = !isNull(ChildCount)",
            "HasParent = !isNull(ParentKey)",
            "ExecNotional = (isNull(AvgPx) ? 0.0 : AvgPx) * CumQty",
            # A street order is the end of the line: its open qty IS the market exposure.
            "FillGap = IsStreet ? 0.0 : CumQty - (HasChildren ? ChildCumQty : 0.0)",
            "RouteGap = IsStreet ? 0.0 : LeavesQty - (HasChildren ? ChildLeavesQty : 0.0)",
            "HeldQty = RouteGap > 0 ? RouteGap : 0.0",
            "OverRoutedQty = RouteGap < 0 ? -RouteGap : 0.0",
            "UnbookedQty = FillGap < 0 ? -FillGap : 0.0",
            "PhantomQty = FillGap > 0 ? FillGap : 0.0",
            "IsClientRoot = LinkState == `ROOT`",
            "ClientOrderQty = IsClientRoot ? OrderQty : 0.0",
            "ClientCumQty = IsClientRoot ? CumQty : 0.0",
            "ClientLeavesQty = IsClientRoot ? LeavesQty : 0.0",
            "StreetCumQty = IsStreet ? CumQty : 0.0",
            "StreetLeavesQty = IsStreet ? LeavesQty : 0.0",
            "Trees = Depth == 0 ? 1 : 0",
            "UnlinkedTrees = (Depth == 0 && !IsClientRoot) ? 1 : 0",
            _break_kind(tol),
            "Breaks = " + _any_of("BreakKind", config.RED_BREAK_KINDS) + " ? 1 : 0",
        ]
    )
    # Second pass: flag the children of a broken edge, and count breaks per tree (so a
    # "breaks only" view can keep whole trees rather than tearing them apart).
    edges = recon.view(["NodeKey", "ParentEdgeBreak = " + _any_of("BreakKind", config.EDGE_BREAK_KINDS)])
    tree_breaks = recon.agg_by([agg.sum_(["TreeBreaks = Breaks"])], by=["RootKey"])
    return (
        recon.natural_join(edges, on=["ParentKey=NodeKey"], joins=["ParentEdgeBreak"])
        .natural_join(tree_breaks, on=["RootKey"], joins=["TreeBreaks"])
        .update(["OnBrokenEdge = Breaks == 1 || (!isNull(ParentEdgeBreak) && ParentEdgeBreak)"])
        .view(list(RECON_COLUMNS))
    )


def _exposure(recon: Table) -> Table:
    """``otr_exposure``: one row per (moniker, symbol, side) -- see reference.exposure_summary."""
    return (
        recon.agg_by(
            [agg.count_("Orders"), agg.sum_(list(reference.SUMMARY_SUMS))],
            by=list(reference.SUMMARY_BY),
        )
        .update_view(
            [
                "NetUnbookedQty = StreetCumQty - ClientCumQty",
                "StreetOpenVsClient = StreetLeavesQty - ClientLeavesQty",
            ]
        )
        .move_columns_up(list(reference.SUMMARY_BY) + ["Breaks", "Trees", "UnlinkedTrees"])
        .sort(list(reference.SUMMARY_BY))
    )


def _ladder(recon: Table, exposure: Table) -> Table:
    """``otr_level_ladder``: per bucket and level, totals and the per-edge gaps."""
    return (
        recon.agg_by([agg.count_("Orders"), agg.sum_(list(reference.LADDER_SUMS))], by=list(reference.LADDER_BY))
        .natural_join(
            exposure,
            on=list(reference.SUMMARY_BY),
            joins=["BucketClientCum = ClientCumQty", "BucketClientLeaves = ClientLeavesQty"],
        )
        .update_view(
            [
                "CumVsClient = CumQty - BucketClientCum",
                "LeavesVsClient = LeavesQty - BucketClientLeaves",
            ]
        )
        .drop_columns(["BucketClientCum", "BucketClientLeaves"])
        .sort(list(reference.LADDER_BY))
    )


def _tree(recon: Table) -> Optional[Any]:
    """The native hierarchical panel; ``None`` (logged) if the server rejects ``tree``.

    ``promote_orphans`` is load-bearing: an ``ORPHAN`` has a link id that resolves to
    nothing, and without promotion its whole venue subtree would vanish from the panel.
    """
    try:
        return recon.view(list(TREE_COLUMNS)).tree("NodeKey", "ParentKey", promote_orphans=True)
    except Exception as exc:  # noqa: BLE001 - optional panel; every flat table still works
        print(f"[order-tree] otr_tree unavailable ({type(exc).__name__}: {exc})", flush=True)
        return None


def flat_tree(recon: Table) -> Table:
    """An indented, depth-first flat rendering of the trees in ``recon``.

    ``TreePath`` sorts depth-first (see ``reference.PATH_SEP``), and ``Tree`` indents
    each node by its depth -- a tree that pages, filters and row-presses like any table.
    """
    return (
        recon.update_view(["Tree = `  `.repeat(Depth) + (Depth == 0 ? `` : `+- `) + Level + `  ` + OrderId"])
        .move_columns_up(["Tree"])
        .sort(["RootMoniker", "RootSymbol", "RootSide", "TreePath"])
    )


def build_order_tree(
    topology: Topology,
    level_tables: Mapping[str, Table],
    qty_tol: Optional[float] = None,
) -> Dict[str, Any]:
    """Build every doc 14 table from one adapter table per level.

    Args:
        topology: The level topology (``config.default_topology()`` for
            Raptor -> algo -> router).
        level_tables: ``{level name: table}`` carrying ``config.CANONICAL_COLUMNS``;
            exactly one entry per topology level.
        qty_tol: Absolute quantity tolerance; defaults to ``config.qty_tolerance()``.

    Returns:
        ``{name: table}`` keyed by :data:`TABLE_NAMES` (``otr_tree`` is omitted when
        the server rejects ``Table.tree``).

    Raises:
        ValueError: when ``level_tables`` does not name exactly the topology's levels.
    """
    missing = [name for name in topology.names if name not in level_tables]
    extra = [name for name in level_tables if name not in topology.names]
    if missing or extra:
        raise ValueError(f"level_tables: missing {missing}, unknown {extra} (levels: {list(topology.names)})")
    tol = config.qty_tolerance() if qty_tol is None else float(qty_tol)

    canonical = [canonical_level(level_tables[level.name], level) for level in topology]
    # last_by keeps natural_join's unique-key precondition true even if two adapters
    # (or a bad role filter) emit the same order twice -- the DAG must not die on it.
    nodes = merge(canonical).last_by(["NodeKey"]).view(
        list(config.CANONICAL_COLUMNS) + ["Level", "LevelNo", "System", "IsStreet", "NodeKey"]
    )
    levels = _levels_table(topology)
    id_index = _id_index(nodes)
    linked = _link(topology, nodes, levels, id_index)
    rooted = _with_roots(_walk(topology, linked))
    recon = _recon(rooted, _child_rollup(rooted), tol)
    exposure = _exposure(recon)

    red = _any_of("BreakKind", config.RED_BREAK_KINDS)
    tables: Dict[str, Any] = {
        "otr_nodes": nodes,
        "otr_id_index": id_index,
        "otr_levels": levels,
        "otr_recon": recon,
        "otr_tree_flat": flat_tree(recon),
        "otr_level_ladder": _ladder(recon, exposure),
        "otr_exposure": exposure,
        "otr_breaks": recon.where(red).sort(["RootMoniker", "RootSymbol", "RootSide", "TreePath"]),
        "otr_break_summary": recon.where(f"BreakKind != `NONE`").count_by("Count", by=["Level", "BreakKind"]),
        "otr_moniker_list": recon.where("RootMoniker != ``").select_distinct(["RootMoniker"]).sort(["RootMoniker"]),
        "otr_symbol_list": recon.select_distinct(["RootSymbol"]).sort(["RootSymbol"]),
        "otr_side_list": recon.select_distinct(["RootSide"]).sort(["RootSide"]),
    }
    tree = _tree(recon)
    if tree is not None:
        tables["otr_tree"] = tree
    return {name: tables[name] for name in TABLE_NAMES if name in tables}
