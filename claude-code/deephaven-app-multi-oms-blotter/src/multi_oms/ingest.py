"""Per-hub ingestion: one ``kc.consume`` blink table per hub topic -- doc 09 section 4.

Same settings as doc 03 section 2.1, once per hub: ``ALL_PARTITIONS_SEEK_TO_BEGINNING``,
key -> ``ChainKey``, value -> ``RawFix``, blink table type.  The topic *is* the
journal, so replaying every hub tape from offset 0 on every boot makes a Deephaven
restart rebuild the identical multi-hub cache -- including the cross-hub links,
which are recomputed from the same tapes rather than persisted anywhere.

Kafka only for v1.  :func:`build_hub_raw` is the seam an AMPS bookmark subscription
would slot into exactly as ``dh_app.amps_ingest`` does for the single-hub app: the
rest of the module reads one column, ``RawFix``, and never sees this table again.

``deephaven`` is imported **inside** the build functions so the configuration
helpers stay unit-testable on a bare host python.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from multi_oms import config
from multi_oms.config import HubConfig, Topology

__all__ = [
    "GROUP_ID_PREFIX",
    "group_id",
    "kafka_bootstrap",
    "source_description",
    "build_hub_raw",
    "build_all_raw",
]

#: Consumer-group prefix; the hub name (lowercased) is appended (doc 09 section 4).
GROUP_ID_PREFIX = "dh-multi-oms-"


def group_id(hub_name: str) -> str:
    """Return the Kafka consumer group id for one hub.

    Args:
        hub_name: The configured hub name, e.g. ``"OMS-B-parent"``.

    Returns:
        ``"dh-multi-oms-oms-b-parent"``. One group per hub keeps the tapes
        independent: a hub can be re-consumed without disturbing the others.
    """
    return GROUP_ID_PREFIX + str(hub_name).lower()


def kafka_bootstrap(env: Optional[Mapping[str, str]] = None) -> str:
    """Return the bootstrap servers every hub topic is consumed from.

    Thin re-export of :func:`multi_oms.config.kafka_bootstrap` so callers that only
    care about ingestion do not have to reach into the config module.
    """
    return config.kafka_bootstrap(env)


def source_description(
    topology: Topology, env: Optional[Mapping[str, str]] = None
) -> str:
    """One-line summary of where the hub tapes are read from, for the banner."""
    return (
        f"kafka: {kafka_bootstrap(env)} "
        f"topics={list(topology.topics)} (seek to beginning)"
    )


def build_hub_raw(
    hub: HubConfig,
    bootstrap: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
) -> Any:
    """Build one hub's raw blink table.

    Args:
        hub: The hub whose topic to consume.
        bootstrap: Bootstrap servers; defaults to :func:`kafka_bootstrap`.
        env: Environment used for the default bootstrap lookup.

    Returns:
        A blink :class:`~deephaven.table.Table` with the Kafka bookkeeping columns
        (``KafkaPartition``, ``KafkaOffset``, ``KafkaTimestamp``) plus ``ChainKey``
        (message key) and ``RawFix`` (the SOH-delimited FIX 4.2 message).
    """
    from deephaven import dtypes as dht
    from deephaven.stream.kafka import consumer as kc

    servers = bootstrap or kafka_bootstrap(env)
    return kc.consume(
        {"bootstrap.servers": servers, "group.id": group_id(hub.name)},
        topic=hub.topic,
        # Replay from offset 0 => deterministic rebuild of the whole multi-hub cache.
        offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
        key_spec=kc.simple_spec("ChainKey", dht.string),
        value_spec=kc.simple_spec("RawFix", dht.string),
        table_type=kc.TableType.blink(),
    )


def build_all_raw(
    topology: Topology,
    bootstrap: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Build one raw blink table per configured hub.

    Args:
        topology: The validated hub graph.
        bootstrap: Bootstrap servers; defaults to :func:`kafka_bootstrap`.
        env: Environment used for the default bootstrap lookup.

    Returns:
        ``{hub name: blink Table}`` in configuration order.
    """
    servers = bootstrap or kafka_bootstrap(env)
    return {hub.name: build_hub_raw(hub, bootstrap=servers) for hub in topology}
