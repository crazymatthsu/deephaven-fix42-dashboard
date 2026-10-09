"""The reconciliation semantics, pinned on the python reference (doc 14 sections 4-5).

The embedded-engine test asserts the Deephaven DAG matches this reference row for row,
so everything proven here holds for the DAG too.
"""

from __future__ import annotations

import pytest

from conftest import row, tree_of
from order_tree_recon import config, mockdata, reference


# --------------------------------------------------------------------------------------
# The scenario catalog
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("scenario", mockdata.SCENARIOS)
def test_scenario_breaks(nodes, book, scenario):
    system, order_id = book.scenario_roots[scenario]
    got = sorted(
        (n["Level"], n["BreakKind"]) for n in tree_of(nodes, f"{system}|{order_id}") if n["BreakKind"] != "NONE"
    )
    assert got == mockdata.EXPECTED_BREAKS[scenario]


def test_random_families_are_healthy(nodes, book):
    scenario_roots = {f"{s}|{o}" for s, o in book.scenario_roots.values()}
    kinds = {n["BreakKind"] for n in nodes.values() if n["RootKey"] not in scenario_roots}
    assert kinds <= {"NONE", "HELD"}


def test_every_scenario_node_is_linked_except_the_orphan(nodes, book):
    for scenario, (system, order_id) in book.scenario_roots.items():
        tree = tree_of(nodes, f"{system}|{order_id}")
        roots = [n for n in tree if n["Depth"] == 0]
        assert len(roots) == 1
        expected_root_state = "ORPHAN" if scenario == "orphan" else "ROOT"
        assert roots[0]["LinkState"] == expected_root_state
        assert all(n["LinkState"] == "LINKED" for n in tree if n["Depth"] > 0)


# --------------------------------------------------------------------------------------
# Linking and the tree
# --------------------------------------------------------------------------------------


def test_moniker_and_attributes_are_inherited_from_the_root(nodes, book):
    system, order_id = book.scenario_roots["clean_filled"]
    tree = tree_of(nodes, f"{system}|{order_id}")
    assert {n["Level"] for n in tree} == set(config.default_topology().names)
    assert {n["RootMoniker"] for n in tree} == {"ACME"}
    # only the Raptor order itself carries the moniker
    assert [n["Level"] for n in tree if n["Moniker"]] == ["CLIENT"]


def test_dma_links_through_raptor_alt_id(nodes, book):
    system, order_id = book.scenario_roots["dma"]
    tree = tree_of(nodes, f"{system}|{order_id}")
    router = [n for n in tree if n["Level"] == "OR_PARENT"]
    assert len(router) == 1
    client = nodes[f"{system}|{order_id}"]
    assert router[0]["LinkId"] == client["AltId"] != client["OrderId"]
    assert router[0]["ParentKey"] == client["NodeKey"]
    assert router[0]["Depth"] == 1  # no algo hop
    assert {n["Level"] for n in tree} == {"CLIENT", "OR_PARENT", "OR_CHILD"}


def test_nested_algo_resolves_within_max_depth(nodes, book):
    system, order_id = book.scenario_roots["nested_algo"]
    tree = tree_of(nodes, f"{system}|{order_id}")
    assert max(n["Depth"] for n in tree) == 5
    assert all(n["RootResolved"] for n in tree)


def test_tree_path_sorts_depth_first(nodes, book):
    system, order_id = book.scenario_roots["clean_filled"]
    ordered = sorted(tree_of(nodes, f"{system}|{order_id}"), key=lambda n: n["TreePath"])
    seen = set()
    for node in ordered:
        # pre-order: every node's parent has already been visited
        assert node["ParentKey"] is None or node["ParentKey"] in seen
        seen.add(node["NodeKey"])
    # and each subtree is contiguous: a node's descendants directly follow it
    for i, node in enumerate(ordered):
        size = sum(1 for n in ordered if n["TreePath"].startswith(node["TreePath"] + reference.PATH_SEP))
        block = ordered[i + 1 : i + 1 + size]
        assert all(n["TreePath"].startswith(node["TreePath"] + reference.PATH_SEP) for n in block)


def test_parent_candidates_are_tried_in_order(topology):
    # an id that exists in BOTH the algo and the Raptor namespace resolves to ALGO
    rows = [
        row("CLIENT", "X1"),
        row("ALGO_PARENT", "P1", "X1"),
        row("ALGO_CHILD", "X1", "P1"),
        row("OR_PARENT", "R1", "X1"),
    ]
    nodes = reference.reconcile(topology, rows)
    assert nodes["OR|R1"]["ParentKey"] == "ALGO|X1"


def test_link_states(topology):
    nodes = reference.reconcile(
        topology,
        [
            row("CLIENT", "C1"),
            row("ALGO_PARENT", "P1", "C1"),
            row("ALGO_PARENT", "P2", "nope"),
            row("ALGO_PARENT", "P3", ""),
        ],
    )
    assert [nodes[k]["LinkState"] for k in ("RAPTOR|C1", "ALGO|P1", "ALGO|P2", "ALGO|P3")] == [
        "ROOT", "LINKED", "ORPHAN", "NO_LINK",
    ]
    assert nodes["ALGO|P2"]["BreakKind"] == "ORPHAN"
    assert nodes["ALGO|P3"]["BreakKind"] == "NO_LINK"
    assert nodes["ALGO|P2"]["RootKey"] == "ALGO|P2"  # an orphan is its own root


def test_too_deep_and_cycles_are_flagged_not_misrooted():
    topo = config.parse_topology(
        [{"name": "ROOT", "system": "S"}, {"name": "NODE", "system": "T", "parents": ["T", "S"]}]
    )  # max_depth = 2
    chain = [row("ROOT", "r"), row("NODE", "a", "r"), row("NODE", "b", "a"), row("NODE", "c", "b")]
    nodes = reference.reconcile(topo, chain)
    assert nodes["T|b"]["RootResolved"] and nodes["T|b"]["RootKey"] == "S|r"
    assert not nodes["T|c"]["RootResolved"]
    assert nodes["T|c"]["BreakKind"] == "TOO_DEEP"
    cycle = reference.reconcile(topo, [row("NODE", "x", "y"), row("NODE", "y", "x")])
    assert {n["BreakKind"] for n in cycle.values()} == {"TOO_DEEP"}


def test_canonicalize_normalizes_nulls():
    out = reference.canonicalize("CLIENT", {"OrderId": "A", "CumQty": None, "Symbol": None, "AvgPx": None})
    assert out["CumQty"] == 0.0 and out["OrderQty"] == 0.0
    assert out["Symbol"] == "" and out["LinkId"] == ""
    assert out["AvgPx"] is None


def test_last_writer_wins(topology):
    nodes = reference.reconcile(topology, [row("CLIENT", "C1", CumQty=10.0), row("CLIENT", "C1", CumQty=20.0)])
    assert len(nodes) == 1 and nodes["RAPTOR|C1"]["CumQty"] == 20.0


# --------------------------------------------------------------------------------------
# Per-edge gaps and the taxonomy
# --------------------------------------------------------------------------------------


def _pair(topology, parent, children):
    rows = [row("OR_PARENT", "P", "", **parent)] + [
        row("OR_CHILD", f"P.{i}", "P", **child) for i, child in enumerate(children, 1)
    ]
    # make the parent a linked node so only the edge below it matters
    rows.insert(0, row("CLIENT", "C", OrderQty=parent.get("OrderQty", 100.0),
                       CumQty=parent.get("CumQty", 0.0), LeavesQty=parent.get("LeavesQty", 100.0)))
    rows[1]["LinkId"] = "C"
    nodes = reference.reconcile(topology, rows)
    return nodes["OR|P"], [nodes[f"OR|P.{i}"] for i in range(1, len(children) + 1)]


@pytest.mark.parametrize(
    "parent, children, kind",
    [
        # clean: parent's own numbers equal the sum over its children
        (dict(OrderQty=300.0, CumQty=200.0, LeavesQty=100.0),
         [dict(OrderQty=150.0, CumQty=100.0, LeavesQty=50.0), dict(OrderQty=150.0, CumQty=100.0, LeavesQty=50.0)], "NONE"),
        # a fill on the venue the parent never booked
        (dict(OrderQty=300.0, CumQty=200.0, LeavesQty=100.0),
         [dict(OrderQty=300.0, CumQty=300.0, LeavesQty=0.0)], "UNBOOKED_FILL"),
        # the parent shows fills no child explains
        (dict(OrderQty=300.0, CumQty=300.0, LeavesQty=0.0),
         [dict(OrderQty=300.0, CumQty=200.0, LeavesQty=0.0, Status="CANCELED")], "PHANTOM_FILL"),
        # more open downstream than upstream
        (dict(OrderQty=300.0, CumQty=0.0, LeavesQty=300.0),
         [dict(OrderQty=200.0, LeavesQty=200.0), dict(OrderQty=200.0, LeavesQty=200.0)], "OVER_ROUTED"),
        # parent closed, a child still working
        (dict(OrderQty=300.0, CumQty=0.0, LeavesQty=0.0, Status="CANCELED"),
         [dict(OrderQty=300.0, LeavesQty=100.0, CumQty=0.0)], "CHILD_STILL_OPEN"),
        # open upstream, not yet routed down
        (dict(OrderQty=300.0, CumQty=0.0, LeavesQty=300.0),
         [dict(OrderQty=100.0, LeavesQty=100.0)], "HELD"),
        # overfill beats the fill gap it also causes
        (dict(OrderQty=300.0, CumQty=400.0, LeavesQty=0.0),
         [dict(OrderQty=400.0, CumQty=400.0, LeavesQty=0.0)], "OVERFILL"),
    ],
)
def test_edge_taxonomy(topology, parent, children, kind):
    node, kids = _pair(topology, parent, children)
    assert node["BreakKind"] == kind
    on_edge = kind in config.EDGE_BREAK_KINDS
    assert all(k["OnBrokenEdge"] == on_edge for k in kids)
    assert all(k["BreakKind"] == "NONE" for k in kids)


def test_gaps_are_signed(topology):
    node, _ = _pair(topology, dict(OrderQty=300.0, CumQty=0.0, LeavesQty=300.0),
                    [dict(OrderQty=200.0, LeavesQty=200.0), dict(OrderQty=200.0, LeavesQty=200.0)])
    assert node["RouteGap"] == -100.0
    assert node["OverRoutedQty"] == 100.0 and node["HeldQty"] == 0.0
    node, _ = _pair(topology, dict(OrderQty=300.0, CumQty=200.0, LeavesQty=100.0),
                    [dict(OrderQty=300.0, CumQty=300.0, LeavesQty=0.0)])
    assert node["FillGap"] == -100.0 and node["UnbookedQty"] == 100.0


def test_street_orders_carry_no_gap(nodes):
    street = [n for n in nodes.values() if n["IsStreet"]]
    assert street and all(n["FillGap"] == 0.0 and n["RouteGap"] == 0.0 for n in street)


def test_mismatch_on_side(nodes, book):
    system, order_id = book.scenario_roots["side_mismatch"]
    bad = [n for n in tree_of(nodes, f"{system}|{order_id}") if n["BreakKind"] == "MISMATCH"]
    assert len(bad) == 1 and bad[0]["Side"] == "SELL" and bad[0]["ParentSide"] == "BUY"


def test_tree_breaks_count_red_nodes_only(nodes, book):
    working = f"{book.scenario_roots['working'][0]}|{book.scenario_roots['working'][1]}"
    overfill = f"{book.scenario_roots['overfill'][0]}|{book.scenario_roots['overfill'][1]}"
    assert {n["TreeBreaks"] for n in tree_of(nodes, working)} == {0}  # HELD is amber
    assert {n["TreeBreaks"] for n in tree_of(nodes, overfill)} == {2}


# --------------------------------------------------------------------------------------
# Exposure identities -- what the ladder and the summary promise
# --------------------------------------------------------------------------------------


def test_open_qty_identity_on_complete_buckets(nodes):
    """Client open = held at every level - over-routed + live on the street."""
    for bucket in reference.exposure_summary(nodes):
        if bucket["UnlinkedTrees"]:
            continue
        lhs = bucket["ClientLeavesQty"]
        rhs = bucket["HeldQty"] - bucket["OverRoutedQty"] + bucket["StreetLeavesQty"]
        assert lhs == pytest.approx(rhs), bucket


def test_fill_identity_on_complete_buckets(nodes):
    """Street executed - client booked = unbooked fills - phantom fills."""
    for bucket in reference.exposure_summary(nodes):
        if bucket["UnlinkedTrees"]:
            continue
        assert bucket["NetUnbookedQty"] == pytest.approx(bucket["UnbookedQty"] - bucket["PhantomQty"]), bucket


def _bucket(nodes, moniker, symbol, side):
    return next(b for b in reference.exposure_summary(nodes)
                if (b["RootMoniker"], b["RootSymbol"], b["RootSide"]) == (moniker, symbol, side))


def test_exposure_tells_the_scenario_story(nodes):
    # unbooked_fill: 100 shares executed on the street that the client order does not show
    assert _bucket(nodes, "CEDAR", "JPM", "BUY")["NetUnbookedQty"] == 100.0
    # cancel_not_propagated: client fully closed, 100 still working on a venue
    dune = _bucket(nodes, "DUNE", "MSFT", "BUY")
    assert dune["ClientLeavesQty"] == 0.0 and dune["StreetLeavesQty"] == 100.0
    assert dune["StreetOpenVsClient"] == 100.0 and dune["OverRoutedQty"] == 100.0
    # working: 1200 held in the algo, 1200 on the street, 2400 open for the client
    acme = _bucket(nodes, "ACME", "MSFT", "BUY")
    assert (acme["ClientLeavesQty"], acme["HeldQty"], acme["StreetLeavesQty"]) == (2400.0, 1200.0, 1200.0)
    # orphan: an unattributed bucket with live street quantity and no client order
    orphan = _bucket(nodes, "", "NVDA", "BUY")
    assert orphan["UnlinkedTrees"] == 1 and orphan["ClientLeavesQty"] == 0.0 and orphan["StreetLeavesQty"] == 200.0


def test_ladder_level_totals(nodes):
    ladder = [r for r in reference.level_ladder(nodes)
              if (r["RootMoniker"], r["RootSymbol"], r["RootSide"]) == ("CEDAR", "JPM", "BUY")]
    assert [r["Level"] for r in ladder] == list(config.default_topology().names)
    by_level = {r["Level"]: r for r in ladder}
    assert by_level["OR_CHILD"]["CumVsClient"] == 100.0  # the street is 100 ahead of the client
    assert by_level["OR_PARENT"]["UnbookedQty"] == 100.0  # ...and this is the edge that lost it
    assert all(by_level[lvl]["CumVsClient"] == 0.0 for lvl in ("ALGO_PARENT", "ALGO_CHILD", "OR_PARENT"))
