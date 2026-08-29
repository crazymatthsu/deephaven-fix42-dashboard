"""Ingestion: the ``fix_raw`` blink source table (doc 03 section 2.1).

The source is selectable with ``FIX42_SOURCE``:

``kafka`` (default)
    ``kc.consume`` over the ``fix42.messages`` topic, seeking to the beginning.
``amps``
    An AMPS ``bookmark_subscribe`` replaying the transaction log from ``EPOCH``
    (:mod:`dh_app.amps_ingest`).

Both are the same contract, which is the point of the switch: the topic *is* the
journal, so replaying it from the start on every boot makes a Deephaven restart
rebuild the identical cache (doc 03 section 3.3).  Neither path retains anything --
``fix_raw`` is a blink table either way (doc 02 section 2).

Downstream is source-agnostic: :class:`dh_app.pipeline.Pipeline` reads exactly one
column, ``RawFix``, and the rest of the DAG never sees this table at all.  The other
columns differ per source and are bookkeeping only (``KafkaOffset`` vs
``AmpsBookmark`` both being "where in the journal this row came from").

This module imports ``deephaven`` lazily so source selection stays unit-testable on a
bare python (``tests/test_ingest_source.py``).
"""

from __future__ import annotations

import os
from typing import Any, Mapping, Optional

from dh_app import amps_ingest

__all__ = [
    "SOURCE_ENV",
    "BOOTSTRAP_ENV",
    "TOPIC_ENV",
    "SOURCE_KAFKA",
    "SOURCE_AMPS",
    "SOURCES",
    "DEFAULT_SOURCE",
    "DEFAULT_BOOTSTRAP",
    "DEFAULT_TOPIC",
    "DEFAULT_GROUP_ID",
    "fix_source",
    "kafka_bootstrap",
    "kafka_topic",
    "source_description",
    "build_fix_raw",
    "build_kafka_fix_raw",
]

#: Environment variable selecting the source (doc 05 section 4).
SOURCE_ENV = "FIX42_SOURCE"
#: Environment variable holding the Kafka bootstrap servers (doc 05 section 4).
BOOTSTRAP_ENV = "FIX42_KAFKA_BOOTSTRAP"
#: Environment variable holding the source topic (doc 05 section 4).
TOPIC_ENV = "FIX42_TOPIC"

#: Kafka source name.
SOURCE_KAFKA = "kafka"
#: AMPS transaction-log source name.
SOURCE_AMPS = "amps"
#: Every accepted :data:`SOURCE_ENV` value.
SOURCES = (SOURCE_KAFKA, SOURCE_AMPS)
#: The source used when :data:`SOURCE_ENV` is unset.
DEFAULT_SOURCE = SOURCE_KAFKA

#: Compose-network default -- the Kafka service name (doc 04 section 7).
DEFAULT_BOOTSTRAP = "kafka:9092"
#: Project-wide topic convention (doc 00 section 5).
DEFAULT_TOPIC = "fix42.messages"
#: Consumer group id (doc 03 section 2.1).
DEFAULT_GROUP_ID = "dh-fix42-dashboard"


def fix_source(env: Optional[Mapping[str, str]] = None) -> str:
    """Return the configured source name, normalized to one of :data:`SOURCES`.

    Args:
        env: Environment to read; defaults to :data:`os.environ`.

    Returns:
        ``"kafka"`` or ``"amps"``.

    Raises:
        ValueError: If :data:`SOURCE_ENV` is set to anything else.  Failing loudly
            beats silently falling back to Kafka: a deployment that meant to read
            AMPS and got Kafka would look healthy while rebuilding the wrong cache.
    """
    source = os.environ if env is None else env
    value = (source.get(SOURCE_ENV) or DEFAULT_SOURCE).strip().lower()
    if value not in SOURCES:
        raise ValueError(
            f"{SOURCE_ENV}={value!r} is not a known source; expected one of {list(SOURCES)}"
        )
    return value


def kafka_bootstrap(env: Optional[Mapping[str, str]] = None) -> str:
    """Return the configured Kafka bootstrap servers."""
    source = os.environ if env is None else env
    return source.get(BOOTSTRAP_ENV) or DEFAULT_BOOTSTRAP


def kafka_topic(env: Optional[Mapping[str, str]] = None) -> str:
    """Return the configured source topic."""
    source = os.environ if env is None else env
    return source.get(TOPIC_ENV) or DEFAULT_TOPIC


def source_description(env: Optional[Mapping[str, str]] = None) -> str:
    """One-line summary of where ``fix_raw`` is reading from, for the startup banner."""
    if fix_source(env) == SOURCE_AMPS:
        return f"amps: {amps_ingest.AmpsConfig.from_env(env).describe()}"
    return f"kafka: {kafka_bootstrap(env)} topic={kafka_topic(env)} (seek to beginning)"


def build_fix_raw(
    bootstrap: Optional[str] = None,
    topic: Optional[str] = None,
    group_id: str = DEFAULT_GROUP_ID,
) -> Any:
    """Build the ``fix_raw`` blink table from the configured source.

    Args:
        bootstrap: Kafka bootstrap servers; ignored when the source is AMPS.
        topic: Kafka source topic; ignored when the source is AMPS.
        group_id: Kafka consumer group id; ignored when the source is AMPS.

    Returns:
        A blink :class:`~deephaven.table.Table` carrying ``RawFix`` plus per-source
        bookkeeping columns.

    Raises:
        ValueError: If :data:`SOURCE_ENV` names an unknown source.
    """
    if fix_source() == SOURCE_AMPS:
        return amps_ingest.build_amps_fix_raw()
    return build_kafka_fix_raw(bootstrap=bootstrap, topic=topic, group_id=group_id)


def build_kafka_fix_raw(
    bootstrap: Optional[str] = None,
    topic: Optional[str] = None,
    group_id: str = DEFAULT_GROUP_ID,
) -> Any:
    """Build the ``fix_raw`` blink table consuming raw FIX 4.2 strings from Kafka.

    Args:
        bootstrap: Bootstrap servers; defaults to :func:`kafka_bootstrap`.
        topic: Source topic; defaults to :func:`kafka_topic`.
        group_id: Kafka consumer group id.

    Returns:
        A blink :class:`~deephaven.table.Table` with the Kafka bookkeeping columns
        (``KafkaPartition``, ``KafkaOffset``, ``KafkaTimestamp``) plus ``ChainKey``
        (message key) and ``RawFix`` (the SOH-delimited FIX 4.2 message).
    """
    from deephaven import dtypes as dht
    from deephaven.stream.kafka import consumer as kc

    servers = bootstrap or kafka_bootstrap()
    src_topic = topic or kafka_topic()
    return kc.consume(
        {"bootstrap.servers": servers, "group.id": group_id},
        topic=src_topic,
        # Replay from offset 0 => deterministic rebuild of the whole cache.
        offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
        key_spec=kc.simple_spec("ChainKey", dht.string),
        value_spec=kc.simple_spec("RawFix", dht.string),
        table_type=kc.TableType.blink(),
    )
