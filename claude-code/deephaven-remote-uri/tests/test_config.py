"""``REMOTEURI_*`` parsing and validation -- doc 10 section 4.

Runs on a bare host python: :mod:`remote_uri.config` is pure stdlib (over the equally
pure ``multi_oms.config``), which is the whole point of keeping the fleet's
validation rules out of the Deephaven-importing modules.

Every rule here is a *startup* error by contract. A fleet that comes up
misconfigured does not fail loudly at query time -- it silently reports the wrong
exposure, which is the failure mode this suite exists to prevent.
"""

from __future__ import annotations

import json

import pytest

from remote_uri import config


# --------------------------------------------------------------------------------------
# Role
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("value,expected", [("leaf", "leaf"), ("collector", "collector")])
def test_role_reads_the_env(value, expected):
    assert config.role({config.ROLE_ENV: value}) == expected


@pytest.mark.parametrize("value", ["LEAF", " Collector ", "Leaf"])
def test_role_is_case_and_space_insensitive(value):
    assert config.role({config.ROLE_ENV: value}) in config.ROLES


@pytest.mark.parametrize("env", [{}, {config.ROLE_ENV: ""}, {config.ROLE_ENV: "   "}])
def test_missing_role_is_a_startup_error(env):
    with pytest.raises(ValueError) as excinfo:
        config.role(env)
    assert config.ROLE_ENV in str(excinfo.value)


def test_unknown_role_names_the_valid_ones():
    with pytest.raises(ValueError) as excinfo:
        config.role({config.ROLE_ENV: "hub"})
    message = str(excinfo.value)
    assert "leaf" in message and "collector" in message


# --------------------------------------------------------------------------------------
# Topology
# --------------------------------------------------------------------------------------


def test_default_topology_is_doc_09s_four_hubs():
    topology = config.load_topology({})
    assert topology.names == ("OMS-A", "OMS-B-parent", "OMS-B-child", "OMS-C")
    assert [hub.depth for hub in topology] == [0, 1, 2, 3]


def test_topology_comes_from_remoteuri_hubs():
    payload = json.dumps([{"name": "SOLO", "topic": "fix42.solo"}])
    assert config.load_topology({config.HUBS_ENV: payload}).names == ("SOLO",)


def test_a_malformed_topology_is_rejected_by_doc_09s_validator():
    with pytest.raises(ValueError):
        config.load_topology({config.HUBS_ENV: "[{}]"})


# --------------------------------------------------------------------------------------
# restrict_topology
# --------------------------------------------------------------------------------------


def test_restrict_keeps_link_tags_and_depths():
    full = config.load_topology({})
    local = config.restrict_topology(full, ["OMS-B-child", "OMS-C"])
    assert local.names == ("OMS-B-child", "OMS-C")
    assert [hub.link_tag for hub in local] == [16667, 16668]
    # Depth stays the *fleet* depth, not a re-rooted one: OMS-B-child is still the
    # third hop of the chain even on a leaf that does not fold the first two.
    assert [hub.depth for hub in local] == [2, 3]
    assert [hub.upstream for hub in local] == ["OMS-B-parent", "OMS-B-child"]


def test_restrict_follows_topology_order_not_argument_order():
    full = config.load_topology({})
    assert config.restrict_topology(full, ["OMS-C", "OMS-A"]).names == ("OMS-A", "OMS-C")


def test_restrict_deduplicates():
    full = config.load_topology({})
    assert config.restrict_topology(full, ["OMS-A", "OMS-A"]).names == ("OMS-A",)


def test_restrict_rejects_an_unknown_hub():
    full = config.load_topology({})
    with pytest.raises(ValueError) as excinfo:
        config.restrict_topology(full, ["OMS-A", "OMS-Z"])
    message = str(excinfo.value)
    assert "OMS-Z" in message and "OMS-A" in message


@pytest.mark.parametrize("names", [[], ["", "  "]])
def test_restrict_rejects_an_empty_selection(names):
    # A leaf with no hubs comes up healthy and exports four empty tables -- the
    # fleet would silently lose a tape.
    full = config.load_topology({})
    with pytest.raises(ValueError) as excinfo:
        config.restrict_topology(full, names)
    assert config.LEAF_HUBS_ENV in str(excinfo.value)


def test_restrict_message_names_the_caller_variable():
    full = config.load_topology({})
    with pytest.raises(ValueError) as excinfo:
        config.restrict_topology(full, ["NOPE"], env_name="SOMETHING_ELSE")
    assert "SOMETHING_ELSE" in str(excinfo.value)


# --------------------------------------------------------------------------------------
# REMOTEURI_LEAVES
# --------------------------------------------------------------------------------------


def _leaves(payload, topology=None):
    return config.parse_leaves(
        payload if isinstance(payload, (str, type(None))) else json.dumps(payload),
        topology or config.load_topology({}),
    )


def _leaves_error(payload) -> str:
    with pytest.raises(ValueError) as excinfo:
        _leaves(payload)
    message = str(excinfo.value)
    assert message.startswith(f"{config.LEAVES_ENV}:"), message
    return message


def test_default_leaves_are_the_demo_compose():
    leaves = _leaves(None)
    assert [leaf.name for leaf in leaves] == ["DH1", "DH2"]
    assert leaves[0].uri == "dh+plain://dh1:10000"
    assert leaves[0].hubs == ("OMS-A",)
    assert leaves[1].hubs == ("OMS-B-parent", "OMS-B-child", "OMS-C")


def test_default_leaves_cover_every_hub_exactly_once():
    topology = config.load_topology({})
    leaves = _leaves(None, topology)
    owned = [hub for leaf in leaves for hub in leaf.hubs]
    assert sorted(owned) == sorted(topology.names)
    assert config.unassigned_hubs(topology, leaves) == ()


def test_default_leaves_json_is_valid_json():
    assert [entry["name"] for entry in json.loads(config.DEFAULT_LEAVES_JSON)] == ["DH1", "DH2"]


def test_leaf_global_names_and_host_ports():
    leaves = _leaves(None)
    assert leaves[0].global_name("rx_orders") == "rx_orders_dh1"
    assert leaves[1].global_name("rx_leaf_stats") == "rx_leaf_stats_dh2"
    assert leaves[0].host_port == ("dh1", 10000)
    assert leaves[0].scope_uri("rx_orders") == "dh+plain://dh1:10000/scope/rx_orders"


def test_a_hub_on_two_leaves_is_refused_at_startup():
    # THE rule of doc 10 section 4.3: two leaves folding one tape make orders_all
    # non-unique on GlobalKey, and doc 09's linking natural_join fails at *runtime*
    # with a message about the join rather than about the configuration.
    message = _leaves_error(
        [
            {"name": "DH1", "uri": "dh+plain://dh1:10000", "hubs": ["OMS-A", "OMS-C"]},
            {"name": "DH2", "uri": "dh+plain://dh2:10000", "hubs": ["OMS-C"]},
        ]
    )
    assert "OMS-C" in message
    assert "DH1" in message and "DH2" in message
    assert "GlobalKey" in message


def test_duplicate_leaf_names_are_refused():
    message = _leaves_error(
        [
            {"name": "DH1", "uri": "dh+plain://dh1:10000", "hubs": ["OMS-A"]},
            {"name": "DH1", "uri": "dh+plain://dh2:10000", "hubs": ["OMS-C"]},
        ]
    )
    assert "DH1" in message


def test_leaf_names_that_sanitise_to_one_global_are_refused():
    message = _leaves_error(
        [
            {"name": "DH-1", "uri": "dh+plain://dh1:10000", "hubs": ["OMS-A"]},
            {"name": "DH_1", "uri": "dh+plain://dh2:10000", "hubs": ["OMS-C"]},
        ]
    )
    assert "rx_orders_dh_1" in message


def test_an_unknown_hub_is_refused():
    message = _leaves_error([{"name": "DH1", "uri": "dh+plain://dh1:10000", "hubs": ["OMS-Z"]}])
    assert "OMS-Z" in message


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        "{}",
        "[]",
        '[["DH1"]]',
        '[{"name": "DH1", "uri": "dh+plain://dh1:10000", "hubs": ["OMS-A"], "extra": 1}]',
        '[{"uri": "dh+plain://dh1:10000", "hubs": ["OMS-A"]}]',
        '[{"name": "", "uri": "dh+plain://dh1:10000", "hubs": ["OMS-A"]}]',
        '[{"name": "DH1", "hubs": ["OMS-A"]}]',
        '[{"name": "DH1", "uri": "http://dh1:10000", "hubs": ["OMS-A"]}]',
        '[{"name": "DH1", "uri": "dh+plain://dh1:10000", "hubs": []}]',
        '[{"name": "DH1", "uri": "dh+plain://dh1:10000", "hubs": [1]}]',
        '[{"name": "DH1", "uri": "dh+plain://dh1:10000", "hubs": ["OMS-A", "OMS-A"]}]',
    ],
)
def test_malformed_leaves_are_refused(payload):
    _leaves_error(payload)


def test_hubs_may_be_written_as_a_comma_list():
    leaves = _leaves(
        [{"name": "DH2", "uri": "dh+plain://dh2:10000", "hubs": "OMS-B-parent, OMS-C"}]
    )
    assert leaves[0].hubs == ("OMS-B-parent", "OMS-C")


def test_an_unassigned_hub_is_a_warning_not_an_error():
    topology = config.load_topology({})
    leaves = _leaves([{"name": "DH1", "uri": "dh+plain://dh1:10000", "hubs": ["OMS-A"]}], topology)
    # Legal: the downstream hops simply show as DANGLING/NO_LINK, which is a true
    # statement about a fleet that is not folding those tapes.
    assert config.unassigned_hubs(topology, leaves) == ("OMS-B-parent", "OMS-B-child", "OMS-C")


# --------------------------------------------------------------------------------------
# Leaf settings
# --------------------------------------------------------------------------------------


def _leaf_env(**overrides):
    env = {config.LEAF_NAME_ENV: "DH1", config.LEAF_HUBS_ENV: "OMS-A"}
    env.update(overrides)
    return env


def test_leaf_defaults_match_doc_10_section_4_2():
    settings = config.load_leaf_settings(_leaf_env())
    assert settings.role == "leaf"
    assert settings.name == "DH1"
    assert settings.hub_names == ("OMS-A",)
    assert settings.amps_uris == ("tcp://amps:9007/amps/fix",)
    assert settings.bookmark == "epoch"
    assert settings.amps_filter == ""
    assert settings.max_pending == 250_000
    assert settings.exec_ring == 0
    assert settings.stats_period_ms == 5000
    assert settings.qty_tol == 1e-6
    assert settings.notional_tol == 0.01


def test_leaf_client_name_is_unique_per_leaf_and_hub():
    settings = config.load_leaf_settings(_leaf_env(**{config.LEAF_HUBS_ENV: "OMS-A"}))
    # AMPS displaces the OLDER connection on a duplicate logon name (doc 10 s2.6).
    assert settings.client_name("OMS-B-parent") == "dh-dh1-oms-b-parent"


def test_leaf_reads_every_knob():
    settings = config.load_leaf_settings(
        _leaf_env(
            **{
                config.LEAF_HUBS_ENV: "OMS-B-parent,OMS-C",
                config.AMPS_URI_ENV: "tcp://a:9007/amps/fix, tcp://b:9007/amps/fix",
                config.AMPS_BOOKMARK_ENV: "now",
                config.AMPS_FILTER_ENV: "/35 IN ('D','8')",
                config.AMPS_MAX_PENDING_ENV: "1000",
                config.EXEC_RING_ENV: "2000000",
                config.STATS_PERIOD_ENV: "250",
                config.QTY_TOL_ENV: "0.5",
                config.NOTIONAL_TOL_ENV: "2",
            }
        )
    )
    assert settings.hub_names == ("OMS-B-parent", "OMS-C")
    assert settings.amps_uris == ("tcp://a:9007/amps/fix", "tcp://b:9007/amps/fix")
    assert settings.bookmark == "now"
    assert settings.amps_filter == "/35 IN ('D','8')"
    assert settings.max_pending == 1000
    assert settings.exec_ring == 2_000_000
    assert settings.stats_period_ms == 250
    assert settings.qty_tol == 0.5
    assert settings.notional_tol == 2.0


@pytest.mark.parametrize(
    "env_key",
    [config.LEAF_NAME_ENV, config.LEAF_HUBS_ENV],
)
def test_leaf_requires_its_identity(env_key):
    env = _leaf_env()
    env.pop(env_key)
    with pytest.raises(ValueError) as excinfo:
        config.load_leaf_settings(env)
    assert env_key in str(excinfo.value)


@pytest.mark.parametrize(
    "key,value",
    [
        (config.AMPS_MAX_PENDING_ENV, "0"),
        (config.AMPS_MAX_PENDING_ENV, "many"),
        (config.EXEC_RING_ENV, "-1"),
        (config.STATS_PERIOD_ENV, "0"),
        (config.QTY_TOL_ENV, "-1"),
        (config.NOTIONAL_TOL_ENV, "nan"),
        (config.AMPS_URI_ENV, " , "),
    ],
)
def test_leaf_rejects_an_unusable_knob(key, value):
    with pytest.raises(ValueError) as excinfo:
        config.load_leaf_settings(_leaf_env(**{key: value}))
    assert key in str(excinfo.value)


# --------------------------------------------------------------------------------------
# Collector settings
# --------------------------------------------------------------------------------------


def test_collector_defaults_match_doc_10_section_4_3():
    settings = config.load_collector_settings({})
    assert settings.role == "collector"
    assert settings.leaf_names == ("DH1", "DH2")
    assert settings.resolver == "uri"
    assert settings.connect_timeout == 300.0
    assert settings.connect_interval == 5.0
    assert settings.md_period_ms == 1000
    assert settings.md_spread_bps == 5.0
    assert settings.md_seed == 42
    assert list(settings.md_universe) == [
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "TSLA",
        "META",
        "GOOGL",
        "JPM",
    ]
    assert settings.md_universe["AAPL"] == 190.0
    assert settings.unassigned == ()


def test_collector_leaf_lookups():
    settings = config.load_collector_settings({})
    assert settings.leaf("DH2").hubs == ("OMS-B-parent", "OMS-B-child", "OMS-C")
    assert settings.leaf("nope") is None
    assert settings.leaf_of("OMS-C").name == "DH2"
    assert settings.leaf_of("OMS-A").name == "DH1"
    assert settings.leaf_of("OMS-Z") is None


@pytest.mark.parametrize("value", ["uri", "barrage", "BARRAGE"])
def test_collector_accepts_both_resolvers(value):
    assert config.load_collector_settings({config.RESOLVER_ENV: value}).resolver in config.RESOLVERS


def test_collector_rejects_an_unknown_resolver():
    with pytest.raises(ValueError) as excinfo:
        config.load_collector_settings({config.RESOLVER_ENV: "flight"})
    assert config.RESOLVER_ENV in str(excinfo.value)


def test_collector_rejects_a_malformed_universe():
    with pytest.raises(ValueError) as excinfo:
        config.load_collector_settings({config.MD_SYMBOLS_ENV: "AAPL"})
    assert config.MD_SYMBOLS_ENV in str(excinfo.value)


@pytest.mark.parametrize(
    "key,value",
    [
        (config.CONNECT_TIMEOUT_ENV, "-1"),
        (config.CONNECT_INTERVAL_ENV, "0"),
        (config.MD_PERIOD_ENV, "0"),
        (config.MD_SPREAD_ENV, "-1"),
        (config.MD_SEED_ENV, "seedy"),
    ],
)
def test_collector_rejects_an_unusable_knob(key, value):
    with pytest.raises(ValueError) as excinfo:
        config.load_collector_settings({key: value})
    assert key in str(excinfo.value)


def test_collector_carries_a_restricted_leaves_list():
    payload = json.dumps(
        [{"name": "SOLO", "uri": "dh+plain://solo", "hubs": ["OMS-A", "OMS-B-parent"]}]
    )
    settings = config.load_collector_settings({config.LEAVES_ENV: payload})
    assert settings.leaf_names == ("SOLO",)
    assert settings.unassigned == ("OMS-B-child", "OMS-C")
    assert settings.leaves[0].host_port == ("solo", 10000)
