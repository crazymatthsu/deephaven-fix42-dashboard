"""Pure-python reference of the doc 14 DAG -- the oracle the engine is tested against.

:mod:`order_tree_recon.dag` expresses linking, the root walk and the per-edge
reconciliation as Deephaven joins and aggregations; this module computes the very same
columns with dicts and loops. The unit suite pins the semantics here (every mock
scenario's expected break, the exposure identities), and the embedded-engine test
asserts the DAG produces identical rows. Change a formula in one place only and that
test fails.

Inputs are **canonical** rows (``config.CANONICAL_COLUMNS`` plus ``Level``): the output
of the per-level adapters, not the native system tables.
"""

from __future__ import annotations

from collections import OrderedDict, defaultdict
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from order_tree_recon import config
from order_tree_recon.config import Topology

__all__ = [
    "PATH_SEP",
    "LADDER_BY",
    "SUMMARY_BY",
    "LADDER_SUMS",
    "SUMMARY_SUMS",
    "canonicalize",
    "reconcile",
    "level_ladder",
    "exposure_summary",
]

#: ``TreePath`` separator. It starts with a space, which sorts before every printable
#: character, so a lexicographic sort on ``TreePath`` is a depth-first pre-order walk
#: (a parent sorts immediately before its own subtree, never after a sibling's).
PATH_SEP = " > "

LADDER_BY: Tuple[str, ...] = ("RootMoniker", "RootSymbol", "RootSide", "LevelNo", "Level", "System")
SUMMARY_BY: Tuple[str, ...] = ("RootMoniker", "RootSymbol", "RootSide")

#: Columns summed by ``level_ladder`` (plus ``Orders`` = row count).
LADDER_SUMS: Tuple[str, ...] = (
    "OrderQty",
    "CumQty",
    "LeavesQty",
    "ExecNotional",
    "HeldQty",
    "OverRoutedQty",
    "UnbookedQty",
    "PhantomQty",
    "Breaks",
)
#: Columns summed by ``exposure_summary`` (plus ``Orders`` = row count).
SUMMARY_SUMS: Tuple[str, ...] = (
    "Trees",
    "UnlinkedTrees",
    "ClientOrderQty",
    "ClientCumQty",
    "ClientLeavesQty",
    "StreetCumQty",
    "StreetLeavesQty",
    "HeldQty",
    "OverRoutedQty",
    "UnbookedQty",
    "PhantomQty",
    "Breaks",
)


def _num(value: Any) -> float:
    return 0.0 if value is None else float(value)


def canonicalize(level: str, row: Mapping[str, Any]) -> Dict[str, Any]:
    """Apply the DAG's canonicalization to one adapter row.

    Strings: ``None`` -> ``""``. Quantities: ``None`` -> ``0.0``. ``AvgPx`` keeps its
    null (an order with no fills has no average price).
    """
    out: Dict[str, Any] = {"Level": level}
    for column in config.STRING_COLUMNS:
        value = row.get(column)
        out[column] = "" if value is None else str(value)
    for column in config.QTY_COLUMNS:
        out[column] = _num(row.get(column))
    avg = row.get("AvgPx")
    out["AvgPx"] = None if avg is None else float(avg)
    return out


def reconcile(
    topology: Topology,
    rows: Iterable[Mapping[str, Any]],
    qty_tol: float = config.DEFAULT_QTY_TOL,
) -> "OrderedDict[str, Dict[str, Any]]":
    """Link, root and reconcile canonical rows; one output row per ``NodeKey``.

    Args:
        topology: The level topology.
        rows: Canonical rows, each carrying ``Level`` (later duplicates of a
            ``NodeKey`` replace earlier ones, as the DAG's ``last_by`` does).
        qty_tol: Absolute quantity tolerance.

    Returns:
        ``{NodeKey: row}`` with every column ``orders_recon`` carries.
    """
    levels = {level.name: level for level in topology}
    nodes: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    for raw in rows:
        level = levels[raw["Level"]]
        node = canonicalize(level.name, raw)
        node["LevelNo"] = level.level_no
        node["System"] = level.system
        node["IsStreet"] = level.street
        node["NodeKey"] = f"{level.system}|{node['OrderId']}"
        nodes[node["NodeKey"]] = node  # last_by: the last writer wins

    # (System, Id) -> NodeKey over OrderId, then AltId (AltId wins a collision, as the
    # DAG's merge([order ids, alt ids]).last_by does).
    index: Dict[Tuple[str, str], str] = {}
    for node in nodes.values():
        index[(node["System"], node["OrderId"])] = node["NodeKey"]
    for node in nodes.values():
        if node["AltId"]:
            index[(node["System"], node["AltId"])] = node["NodeKey"]

    # -- parent resolution ------------------------------------------------------------
    for node in nodes.values():
        level = levels[node["Level"]]
        parent: Optional[str] = None
        for system in level.parents:
            parent = index.get((system, node["LinkId"]))
            if parent is not None:
                break
        node["ParentKey"] = parent
        if level.is_root:
            node["LinkState"] = "ROOT"
        elif node["LinkId"] == "":
            node["LinkState"] = "NO_LINK"
        elif parent is None:
            node["LinkState"] = "ORPHAN"
        else:
            node["LinkState"] = "LINKED"

    # -- root walk: RootKey, Depth, TreePath, RootResolved -------------------------------
    for node in nodes.values():
        root = node["NodeKey"]
        path = node["NodeKey"]
        depth = 0
        up = node["ParentKey"]
        for _ in range(topology.max_depth):
            if up is None:
                break
            path = up + PATH_SEP + path
            root = up
            depth += 1
            up = nodes[up]["ParentKey"]
        node["RootKey"] = root
        node["Depth"] = depth
        node["TreePath"] = path
        node["RootResolved"] = up is None

    # -- parent and root attributes ----------------------------------------------------
    for node in nodes.values():
        parent = nodes.get(node["ParentKey"]) if node["ParentKey"] else None
        node["ParentSymbol"] = parent["Symbol"] if parent else None
        node["ParentSide"] = parent["Side"] if parent else None
        root = nodes[node["RootKey"]]
        node["RootMoniker"] = root["Moniker"]
        node["RootSymbol"] = root["Symbol"]
        node["RootSide"] = root["Side"]
        node["RootLevel"] = root["Level"]
        node["RootOrderId"] = root["OrderId"]
        node["RootLinkState"] = root["LinkState"]

    # -- direct-children rollup --------------------------------------------------------
    children: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for node in nodes.values():
        if node["ParentKey"] is not None:
            children[node["ParentKey"]].append(node)

    for node in nodes.values():
        kids = children.get(node["NodeKey"], [])
        node["ExecNotional"] = (node["AvgPx"] or 0.0) * node["CumQty"]
        if kids:
            node["ChildCount"] = len(kids)
            node["ChildOrderQty"] = sum(k["OrderQty"] for k in kids)
            node["ChildCumQty"] = sum(k["CumQty"] for k in kids)
            node["ChildLeavesQty"] = sum(k["LeavesQty"] for k in kids)
        else:
            node["ChildCount"] = None
            node["ChildOrderQty"] = None
            node["ChildCumQty"] = None
            node["ChildLeavesQty"] = None
        _gaps(node, qty_tol)

    # -- second pass: the child side of a broken edge, per-tree break counts -----------
    tree_breaks: Dict[str, int] = defaultdict(int)
    for node in nodes.values():
        tree_breaks[node["RootKey"]] += node["Breaks"]
    for node in nodes.values():
        parent = nodes.get(node["ParentKey"]) if node["ParentKey"] else None
        parent_edge = parent is not None and parent["BreakKind"] in config.EDGE_BREAK_KINDS
        node["OnBrokenEdge"] = node["BreakKind"] in config.RED_BREAK_KINDS or parent_edge
        node["TreeBreaks"] = tree_breaks[node["RootKey"]]
    return nodes


def _gaps(node: MutableMapping[str, Any], tol: float) -> None:
    """Per-edge gaps, exposure columns and ``BreakKind`` for one node (doc 14 s5)."""
    has_children = node["ChildCount"] is not None
    node["HasChildren"] = has_children
    if node["IsStreet"]:
        fill_gap = 0.0
        route_gap = 0.0
    else:
        fill_gap = node["CumQty"] - (node["ChildCumQty"] if has_children else 0.0)
        route_gap = node["LeavesQty"] - (node["ChildLeavesQty"] if has_children else 0.0)
    node["FillGap"] = fill_gap
    node["RouteGap"] = route_gap
    node["HeldQty"] = max(route_gap, 0.0)
    node["OverRoutedQty"] = max(-route_gap, 0.0)
    node["UnbookedQty"] = max(-fill_gap, 0.0)
    node["PhantomQty"] = max(fill_gap, 0.0)

    is_client_root = node["LinkState"] == "ROOT"
    node["ClientOrderQty"] = node["OrderQty"] if is_client_root else 0.0
    node["ClientCumQty"] = node["CumQty"] if is_client_root else 0.0
    node["ClientLeavesQty"] = node["LeavesQty"] if is_client_root else 0.0
    node["StreetCumQty"] = node["CumQty"] if node["IsStreet"] else 0.0
    node["StreetLeavesQty"] = node["LeavesQty"] if node["IsStreet"] else 0.0
    node["Trees"] = 1 if node["Depth"] == 0 else 0
    node["UnlinkedTrees"] = 1 if node["Depth"] == 0 and not is_client_root else 0

    has_parent = node["ParentKey"] is not None
    if node["LinkState"] in ("ORPHAN", "NO_LINK"):
        kind = node["LinkState"]
    elif not node["RootResolved"]:
        kind = "TOO_DEEP"
    elif has_parent and (node["Symbol"] != node["ParentSymbol"] or node["Side"] != node["ParentSide"]):
        kind = "MISMATCH"
    elif node["CumQty"] > node["OrderQty"] + tol:
        kind = "OVERFILL"
    elif fill_gap < -tol:
        kind = "UNBOOKED_FILL"
    elif fill_gap > tol:
        kind = "PHANTOM_FILL"
    elif route_gap < -tol and node["LeavesQty"] <= tol:
        kind = "CHILD_STILL_OPEN"
    elif route_gap < -tol:
        kind = "OVER_ROUTED"
    elif route_gap > tol:
        kind = "HELD"
    else:
        kind = "NONE"
    node["BreakKind"] = kind
    node["Breaks"] = 1 if kind in config.RED_BREAK_KINDS else 0


def _group(nodes: Iterable[Mapping[str, Any]], by: Tuple[str, ...], sums: Tuple[str, ...]) -> List[Dict[str, Any]]:
    groups: "OrderedDict[Tuple[Any, ...], Dict[str, Any]]" = OrderedDict()
    for node in nodes:
        key = tuple(node[column] for column in by)
        group = groups.get(key)
        if group is None:
            group = {column: node[column] for column in by}
            group["Orders"] = 0
            for column in sums:
                group[column] = 0
            groups[key] = group
        group["Orders"] += 1
        for column in sums:
            group[column] += node[column]
    return [groups[key] for key in sorted(groups)]


def level_ladder(nodes: Mapping[str, Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """Per (moniker, symbol, side, level) totals, plus each level vs the client level.

    ``CumVsClient`` / ``LeavesVsClient`` compare a level's total with the bucket's client
    totals. They are only expected to be zero / non-positive when **all** flow passes
    through that level; DMA flow legitimately skips the algo levels, which is why the
    per-edge columns (``HeldQty`` ... ``PhantomQty``) are the attribution, not these.
    """
    ladder = _group(nodes.values(), LADDER_BY, LADDER_SUMS)
    client = {tuple(row[c] for c in SUMMARY_BY): row for row in exposure_summary(nodes)}
    for row in ladder:
        bucket = client[tuple(row[c] for c in SUMMARY_BY)]
        row["CumVsClient"] = row["CumQty"] - bucket["ClientCumQty"]
        row["LeavesVsClient"] = row["LeavesQty"] - bucket["ClientLeavesQty"]
    return ladder


def exposure_summary(nodes: Mapping[str, Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """One row per (moniker, symbol, side): client vs street, and where the gap sits.

    ``NetUnbookedQty = StreetCumQty - ClientCumQty``: shares executed on venues that the
    client order does not show -- the firm's own position until booked.
    ``StreetOpenVsClient = StreetLeavesQty - ClientLeavesQty``: negative while shares are
    held upstream; positive means more is working on the street than the client wants.
    """
    summary = _group(nodes.values(), SUMMARY_BY, SUMMARY_SUMS)
    for row in summary:
        row["NetUnbookedQty"] = row["StreetCumQty"] - row["ClientCumQty"]
        row["StreetOpenVsClient"] = row["StreetLeavesQty"] - row["ClientLeavesQty"]
    return summary
