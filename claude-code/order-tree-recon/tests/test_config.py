"""Topology validation, the break taxonomy constants and ``OTR_*`` parsing."""

from __future__ import annotations

import math

import pytest

from order_tree_recon import config


def test_default_topology_shape():
    topo = config.default_topology()
    assert topo.names == ("CLIENT", "ALGO_PARENT", "ALGO_CHILD", "OR_PARENT", "OR_CHILD")
    assert topo.systems == ("RAPTOR", "ALGO", "OR")
    assert topo.root_levels == ("CLIENT",)
    assert topo.street_levels == ("OR_CHILD",)
    assert topo.max_parents == 2  # OR_PARENT: ALGO, then RAPTOR (DMA)
    assert topo["OR_PARENT"].parents == ("ALGO", "RAPTOR")
    assert [level.level_no for level in topo] == [0, 1, 2, 3, 4]
    # one walk step per level: room for one nested algo level beyond the default chain
    assert topo.max_depth == 5


def test_describe_lists_every_level():
    lines = config.default_topology().describe()
    assert len(lines) == 5
    assert "parent in: ALGO | RAPTOR" in lines[3]
    assert "[street]" in lines[4]


@pytest.mark.parametrize(
    "levels, message",
    [
        ([], "at least one level"),
        (["CLIENT"], "expected an object"),
        ([{"name": "A", "system": "S", "colour": "red"}], "unknown key"),
        ([{"name": "1A", "system": "S"}], "'name' must be an identifier"),
        ([{"name": "A", "system": "S"}, {"name": "A", "system": "S"}], "duplicate level name"),
        ([{"name": "A", "system": ""}], "'system' must be an identifier"),
        ([{"name": "A", "system": "S", "parents": "S"}], "'parents' must be a list"),
        ([{"name": "A", "system": "S"}, {"name": "B", "system": "T", "parents": ["S", "S"]}], "lists a system twice"),
        ([{"name": "A", "system": "S"}, {"name": "B", "system": "T", "parents": ["X"]}], "not declared by any level"),
        ([{"name": "A", "system": "S", "parents": ["S"]}], "at least one root level"),
        ([{"name": "A", "system": "S", "street": True}], "root level cannot be a street level"),
        ([{"name": "A", "system": "S"}, {"name": "B", "system": "T", "parents": ["S"], "street": "yes"}], "true/false"),
    ],
)
def test_parse_topology_rejects(levels, message):
    with pytest.raises(ValueError, match=message):
        config.parse_topology(levels)


def test_max_depth_must_cover_the_levels():
    with pytest.raises(ValueError, match="max_depth"):
        config.parse_topology(config.DEFAULT_LEVELS, max_depth=3)
    assert config.parse_topology(config.DEFAULT_LEVELS, max_depth=7).max_depth == 7


def test_break_taxonomy_partitions():
    kinds = set(config.BREAK_KINDS)
    assert set(config.RED_BREAK_KINDS) | set(config.AMBER_BREAK_KINDS) | {"NONE"} == kinds
    assert not set(config.RED_BREAK_KINDS) & set(config.AMBER_BREAK_KINDS)
    assert set(config.EDGE_BREAK_KINDS) <= set(config.RED_BREAK_KINDS)
    assert config.BREAK_KINDS[-1] == "NONE"


def test_canonical_columns_partition():
    columns = set(config.CANONICAL_COLUMNS)
    assert set(config.STRING_COLUMNS) | set(config.QTY_COLUMNS) | {"AvgPx"} == columns


def test_env_defaults():
    assert config.qty_tolerance({}) == config.DEFAULT_QTY_TOL
    assert config.seed({}) == config.DEFAULT_SEED
    assert config.mock_families({}) == config.DEFAULT_MOCK_FAMILIES


def test_env_values():
    assert config.qty_tolerance({"OTR_QTY_TOL": "0.5"}) == 0.5
    assert config.seed({"OTR_SEED": " 7 "}) == 7
    assert config.mock_families({"OTR_MOCK_FAMILIES": "0"}) == 0


@pytest.mark.parametrize("raw", ["abc", "-1", "nan", "inf"])
def test_qty_tolerance_rejects(raw):
    with pytest.raises(ValueError, match="OTR_QTY_TOL"):
        config.qty_tolerance({"OTR_QTY_TOL": raw})


@pytest.mark.parametrize("name, fn", [("OTR_SEED", config.seed), ("OTR_MOCK_FAMILIES", config.mock_families)])
@pytest.mark.parametrize("raw", ["x", "1.5", "-3"])
def test_int_env_rejects(name, fn, raw):
    with pytest.raises(ValueError, match=name):
        fn({name: raw})
