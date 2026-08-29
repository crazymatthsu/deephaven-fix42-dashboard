"""Topology parsing/validation and the tuning knobs -- doc 09 section 3.

Runs on a bare host python: ``multi_oms.config`` is pure stdlib, which is the whole
point of keeping the validation rules out of the Deephaven-importing modules.
"""

from __future__ import annotations

import json

import pytest

from multi_oms import config
from multi_oms.config import HubConfig, Topology, page_bounds, parse_topology


# --------------------------------------------------------------------------------------
# Defaults
# --------------------------------------------------------------------------------------


def test_default_topology_is_the_todo_four_hub_chain():
    topology = parse_topology(None)
    assert topology.names == ("OMS-A", "OMS-B-parent", "OMS-B-child", "OMS-C")
    assert topology.topics == (
        "fix42.oms-a",
        "fix42.oms-b-parent",
        "fix42.oms-b-child",
        "fix42.oms-c",
    )
    assert [hub.link_tag for hub in topology] == [0, 16666, 16667, 16668]
    assert [hub.upstream for hub in topology] == ["", "OMS-A", "OMS-B-parent", "OMS-B-child"]
    assert len(topology) == 4


def test_default_topology_json_is_valid_json_matching_the_doc():
    parsed = json.loads(config.DEFAULT_HUBS_JSON)
    assert [entry["name"] for entry in parsed] == [
        "OMS-A",
        "OMS-B-parent",
        "OMS-B-child",
        "OMS-C",
    ]


@pytest.mark.parametrize("raw", [None, "", "   ", "\n"])
def test_blank_env_falls_back_to_the_default_topology(raw):
    assert parse_topology(raw).names == parse_topology(None).names


def test_load_topology_reads_the_env(monkeypatch):
    payload = json.dumps([{"name": "SOLO", "topic": "fix42.solo"}])
    assert config.load_topology({config.HUBS_ENV: payload}).names == ("SOLO",)
    monkeypatch.setenv(config.HUBS_ENV, payload)
    assert config.load_topology().names == ("SOLO",)


# --------------------------------------------------------------------------------------
# Depth
# --------------------------------------------------------------------------------------


def test_depth_is_distance_from_the_root():
    topology = parse_topology(None)
    assert [hub.depth for hub in topology] == [0, 1, 2, 3]
    assert topology.depth_of("OMS-C") == 3
    assert topology.max_depth == 3


def test_depth_of_a_forest_with_several_roots():
    payload = json.dumps(
        [
            {"name": "R1", "topic": "t1"},
            {"name": "R2", "topic": "t2"},
            {"name": "C1", "topic": "t3", "upstream": "R1", "link_tag": 100},
            {"name": "C2", "topic": "t4", "upstream": "C1", "link_tag": 101},
        ]
    )
    topology = parse_topology(payload)
    assert {hub.name: hub.depth for hub in topology} == {"R1": 0, "R2": 0, "C1": 1, "C2": 2}
    assert [hub.name for hub in topology.roots] == ["R1", "R2"]
    assert [hub.name for hub in topology.linked_hubs] == ["C1", "C2"]


def test_depth_when_a_child_is_declared_before_its_parent():
    payload = json.dumps(
        [
            {"name": "C", "topic": "tc", "upstream": "B", "link_tag": 2},
            {"name": "B", "topic": "tb", "upstream": "A", "link_tag": 1},
            {"name": "A", "topic": "ta"},
        ]
    )
    topology = parse_topology(payload)
    assert {hub.name: hub.depth for hub in topology} == {"A": 0, "B": 1, "C": 2}


def test_children_of_reports_hub_graph_fanout():
    payload = json.dumps(
        [
            {"name": "A", "topic": "ta"},
            {"name": "B", "topic": "tb", "upstream": "A", "link_tag": 1},
            {"name": "C", "topic": "tc", "upstream": "A", "link_tag": 2},
        ]
    )
    topology = parse_topology(payload)
    assert [hub.name for hub in topology.children_of("A")] == ["B", "C"]
    assert topology.children_of("B") == ()


# --------------------------------------------------------------------------------------
# Validation -- every rule of doc 09 section 3
# --------------------------------------------------------------------------------------


def _error(payload) -> str:
    with pytest.raises(ValueError) as excinfo:
        parse_topology(payload if isinstance(payload, str) else json.dumps(payload))
    message = str(excinfo.value)
    assert message.startswith("MULTIOMS_HUBS:"), message
    return message


def test_malformed_json_is_rejected():
    assert "not valid JSON" in _error("{not json")


def test_non_array_json_is_rejected():
    assert "must be a JSON array" in _error({"name": "A", "topic": "t"})


def test_empty_array_is_rejected():
    assert "at least one hub" in _error([])


def test_non_object_entry_is_rejected():
    assert "must be a JSON object" in _error(["OMS-A"])


def test_unknown_key_is_rejected():
    message = _error([{"name": "A", "topic": "t", "linkTag": 1}])
    assert "unknown key(s)" in message and "linkTag" in message


@pytest.mark.parametrize("field", ["name", "topic"])
def test_missing_required_field_is_rejected(field):
    entry = {"name": "A", "topic": "t"}
    del entry[field]
    assert f"non-empty {field!r}" in _error([entry])


@pytest.mark.parametrize("field", ["name", "topic"])
def test_blank_required_field_is_rejected(field):
    entry = {"name": "A", "topic": "t"}
    entry[field] = "   "
    assert f"non-empty {field!r}" in _error([entry])


def test_non_string_name_is_rejected():
    assert "expected a string" in _error([{"name": 7, "topic": "t"}])


def test_duplicate_names_are_rejected():
    message = _error([{"name": "A", "topic": "t1"}, {"name": "A", "topic": "t2"}])
    assert "used more than once" in message


def test_duplicate_topics_are_rejected():
    message = _error([{"name": "A", "topic": "t"}, {"name": "B", "topic": "t"}])
    assert "claimed by both" in message and "own tape" in message


def test_unknown_upstream_is_rejected():
    message = _error([{"name": "A", "topic": "t", "upstream": "GHOST", "link_tag": 1}])
    assert "not a configured" in message and "GHOST" in message


def test_upstream_without_link_tag_is_rejected():
    message = _error(
        [{"name": "A", "topic": "t1"}, {"name": "B", "topic": "t2", "upstream": "A"}]
    )
    assert "no 'link_tag'" in message


def test_link_tag_without_upstream_is_rejected():
    assert "no 'upstream'" in _error([{"name": "A", "topic": "t", "link_tag": 16666}])


@pytest.mark.parametrize("value", [0, -1, "0", "-5"])
def test_non_positive_link_tag_is_rejected(value):
    message = _error(
        [
            {"name": "A", "topic": "t1"},
            {"name": "B", "topic": "t2", "upstream": "A", "link_tag": value},
        ]
    )
    assert "positive integer" in message


@pytest.mark.parametrize("value", ["abc", 1.5, True, [16666]])
def test_non_integer_link_tag_is_rejected(value):
    message = _error(
        [
            {"name": "A", "topic": "t1"},
            {"name": "B", "topic": "t2", "upstream": "A", "link_tag": value},
        ]
    )
    assert "positive integer" in message


def test_string_link_tag_is_accepted_and_coerced():
    topology = parse_topology(
        json.dumps(
            [
                {"name": "A", "topic": "t1"},
                {"name": "B", "topic": "t2", "upstream": "A", "link_tag": " 16666 "},
            ]
        )
    )
    assert topology.get("B").link_tag == 16666


def test_self_upstream_is_rejected():
    message = _error([{"name": "A", "topic": "t", "upstream": "A", "link_tag": 1}])
    assert "its own upstream" in message


def test_two_hub_cycle_is_rejected():
    message = _error(
        [
            {"name": "A", "topic": "t1", "upstream": "B", "link_tag": 1},
            {"name": "B", "topic": "t2", "upstream": "A", "link_tag": 2},
        ]
    )
    assert "cycle" in message


def test_three_hub_cycle_is_rejected():
    message = _error(
        [
            {"name": "A", "topic": "t1", "upstream": "C", "link_tag": 1},
            {"name": "B", "topic": "t2", "upstream": "A", "link_tag": 2},
            {"name": "C", "topic": "t3", "upstream": "B", "link_tag": 3},
        ]
    )
    assert "cycle" in message
    # A cycle means no root, so the root check can never be the one that fires.
    assert "no root hub" not in message


def test_single_root_hub_is_valid():
    topology = parse_topology(json.dumps([{"name": "SOLO", "topic": "fix42.solo"}]))
    assert topology.roots[0].name == "SOLO"
    assert topology.linked_hubs == ()
    assert topology.max_depth == 0


# --------------------------------------------------------------------------------------
# HubConfig / Topology surface
# --------------------------------------------------------------------------------------


def test_hub_describe_distinguishes_roots_from_linked_hubs():
    topology = parse_topology(None)
    assert topology.get("OMS-A").describe().endswith("(root)")
    assert "tag 16667" in topology.get("OMS-B-child").describe()
    assert topology.describe().count("\n") == 3


def test_topology_membership_and_lookup():
    topology = parse_topology(None)
    assert "OMS-A" in topology
    assert "NOPE" not in topology
    assert topology.get("NOPE") is None
    with pytest.raises(KeyError):
        topology.depth_of("NOPE")


def test_hub_config_equality_and_is_root():
    left = HubConfig("A", "t")
    right = HubConfig("A", "t")
    assert left == right
    assert left.is_root
    assert not HubConfig("B", "t", upstream="A", link_tag=1).is_root
    assert (left == "A") is False


def test_topology_is_iterable_and_sized():
    topology = Topology([HubConfig("A", "t")])
    assert len(topology) == 1
    assert [hub.name for hub in topology] == ["A"]


# --------------------------------------------------------------------------------------
# Tuning knobs
# --------------------------------------------------------------------------------------


def test_bootstrap_default_and_override():
    assert config.kafka_bootstrap({}) == config.DEFAULT_BOOTSTRAP == "kafka:9092"
    assert config.kafka_bootstrap({config.BOOTSTRAP_ENV: "  broker:1234 "}) == "broker:1234"
    assert config.kafka_bootstrap({config.BOOTSTRAP_ENV: "  "}) == config.DEFAULT_BOOTSTRAP


def test_tolerance_defaults():
    assert config.qty_tolerance({}) == config.DEFAULT_QTY_TOL == 1e-6
    assert config.notional_tolerance({}) == config.DEFAULT_NOTIONAL_TOL == 0.01


def test_tolerance_overrides():
    assert config.qty_tolerance({config.QTY_TOL_ENV: "0.5"}) == 0.5
    assert config.notional_tolerance({config.NOTIONAL_TOL_ENV: "1e-3"}) == 1e-3
    assert config.qty_tolerance({config.QTY_TOL_ENV: "0"}) == 0.0


@pytest.mark.parametrize("value", ["abc", "", " nan ", "-1"])
def test_bad_tolerance_is_rejected_or_defaulted(value):
    env = {config.QTY_TOL_ENV: value}
    if not value.strip():
        assert config.qty_tolerance(env) == config.DEFAULT_QTY_TOL
        return
    with pytest.raises(ValueError) as excinfo:
        config.qty_tolerance(env)
    assert config.QTY_TOL_ENV in str(excinfo.value)


def test_infinite_tolerance_is_rejected():
    with pytest.raises(ValueError, match="finite"):
        config.notional_tolerance({config.NOTIONAL_TOL_ENV: "inf"})


def test_page_size_default_and_override():
    assert config.page_size({}) == config.DEFAULT_PAGE_SIZE == 200
    assert config.page_size({config.PAGE_SIZE_ENV: " 50 "}) == 50


@pytest.mark.parametrize("value", ["0", "-1", "many", "1.5"])
def test_bad_page_size_is_rejected(value):
    with pytest.raises(ValueError) as excinfo:
        config.page_size({config.PAGE_SIZE_ENV: value})
    assert config.PAGE_SIZE_ENV in str(excinfo.value)


def test_page_size_reads_os_environ(monkeypatch):
    monkeypatch.setenv(config.PAGE_SIZE_ENV, "17")
    assert config.page_size() == 17


# --------------------------------------------------------------------------------------
# Paging math
# --------------------------------------------------------------------------------------


def test_page_bounds_first_page():
    bounds = page_bounds(0, 200, 1000)
    assert bounds == {
        "page": 0,
        "pages": 5,
        "start": 0,
        "end": 200,
        "first_row": 1,
        "last_row": 200,
        "total": 1000,
    }


def test_page_bounds_last_partial_page():
    bounds = page_bounds(2, 200, 450)
    assert (bounds["page"], bounds["pages"]) == (2, 3)
    assert (bounds["start"], bounds["end"]) == (400, 600)
    assert (bounds["first_row"], bounds["last_row"]) == (401, 450)


def test_page_bounds_clamps_past_the_end():
    bounds = page_bounds(99, 200, 450)
    assert bounds["page"] == 2
    assert bounds["start"] == 400


def test_page_bounds_negative_page_clamps_to_zero():
    assert page_bounds(-3, 200, 450)["page"] == 0


def test_page_bounds_empty_table():
    bounds = page_bounds(0, 200, 0)
    assert (bounds["pages"], bounds["first_row"], bounds["last_row"]) == (1, 0, 0)
    # The caption reads "0-0", but the slice window stays a full page wide: the
    # blotter is live, and clamping `end` to the current size would freeze the
    # window instead of letting arriving rows fill it.
    assert (bounds["start"], bounds["end"]) == (0, 200)


def test_page_bounds_slice_window_is_never_clamped_to_the_current_size():
    for total in (0, 5, 199):
        bounds = page_bounds(0, 200, total)
        assert bounds["end"] - bounds["start"] == 200


def test_page_bounds_exact_multiple():
    bounds = page_bounds(1, 100, 200)
    assert (bounds["pages"], bounds["first_row"], bounds["last_row"]) == (2, 101, 200)


def test_page_bounds_unknown_total_keeps_the_requested_page():
    bounds = page_bounds(3, 25, None)
    assert (bounds["page"], bounds["pages"], bounds["total"]) == (3, 1, -1)
    assert (bounds["start"], bounds["end"]) == (75, 100)


def test_page_bounds_zero_size_clamps_to_one():
    bounds = page_bounds(0, 0, 3)
    assert bounds["pages"] == 3
    assert (bounds["start"], bounds["end"]) == (0, 1)
