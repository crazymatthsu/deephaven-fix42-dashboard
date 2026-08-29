"""Per-hub ingest configuration -- doc 09 section 4.

``multi_oms.ingest`` keeps its ``deephaven`` imports inside :func:`build_hub_raw`, so
everything except the ``kc.consume`` call itself is exercised here without a
Deephaven server, the same way ``dh_app.ingest`` is (doc 05 section 4).
"""

from __future__ import annotations

import sys

import pytest

from multi_oms import config, ingest


def test_ingest_imports_without_deephaven():
    assert "deephaven" not in sys.modules


@pytest.mark.parametrize(
    "hub_name,expected",
    [
        ("OMS-A", "dh-multi-oms-oms-a"),
        ("OMS-B-parent", "dh-multi-oms-oms-b-parent"),
        ("OMS-B-child", "dh-multi-oms-oms-b-child"),
        ("OMS-C", "dh-multi-oms-oms-c"),
    ],
)
def test_group_id_is_the_lowercased_hub_name(hub_name, expected):
    assert ingest.group_id(hub_name) == expected


def test_group_ids_are_distinct_per_hub():
    topology = config.parse_topology(None)
    ids = [ingest.group_id(name) for name in topology.names]
    assert len(set(ids)) == len(ids)
    assert all(gid.startswith(ingest.GROUP_ID_PREFIX) for gid in ids)


def test_bootstrap_default_and_override():
    assert ingest.kafka_bootstrap({}) == "kafka:9092"
    assert ingest.kafka_bootstrap({config.BOOTSTRAP_ENV: "broker:19092"}) == "broker:19092"


def test_bootstrap_delegates_to_config(monkeypatch):
    monkeypatch.setenv(config.BOOTSTRAP_ENV, "host:9092")
    assert ingest.kafka_bootstrap() == config.kafka_bootstrap()


def test_source_description_names_every_topic():
    topology = config.parse_topology(None)
    description = ingest.source_description(topology, {})
    assert description.startswith("kafka: kafka:9092")
    assert "seek to beginning" in description
    for topic in topology.topics:
        assert topic in description
