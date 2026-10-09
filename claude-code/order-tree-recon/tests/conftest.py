"""Shared fixtures: the default topology, the default mock book and its reconciliation."""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from order_tree_recon import config, mockdata, reference


@pytest.fixture(scope="session")
def topology() -> config.Topology:
    return config.default_topology()


@pytest.fixture(scope="session")
def book() -> mockdata.MockData:
    return mockdata.generate(seed=42, families=24)


@pytest.fixture(scope="session")
def nodes(topology, book) -> Dict[str, Dict[str, Any]]:
    return reference.reconcile(topology, mockdata.canonical_rows(book))


def tree_of(nodes: Dict[str, Dict[str, Any]], root_key: str) -> List[Dict[str, Any]]:
    return [node for node in nodes.values() if node["RootKey"] == root_key]


def row(level: str, order_id: str, link_id: str = "", **values: Any) -> Dict[str, Any]:
    """A canonical row with sane defaults -- for hand-built reference cases."""
    base = {
        "Level": level, "OrderId": order_id, "AltId": "", "LinkId": link_id, "Moniker": "",
        "Symbol": "AAPL", "Side": "BUY", "OrderQty": 100.0, "CumQty": 0.0, "LeavesQty": 100.0,
        "AvgPx": None, "Status": "NEW", "Destination": "",
    }
    base.update(values)
    return base
