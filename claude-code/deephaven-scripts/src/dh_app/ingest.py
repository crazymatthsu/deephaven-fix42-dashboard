"""Kafka ingestion: the ``fix_raw`` blink source table (doc 03 section 2.1).

The topic is the journal: consuming from the beginning on every start makes a
Deephaven restart rebuild the identical cache (doc 03 section 3.3).  Per-order
ordering is guaranteed because the generator keys every message by its chain key.
"""

from __future__ import annotations

import os

from deephaven import dtypes as dht
from deephaven.stream.kafka import consumer as kc
from deephaven.table import Table

__all__ = [
    "BOOTSTRAP_ENV",
    "TOPIC_ENV",
    "DEFAULT_BOOTSTRAP",
    "DEFAULT_TOPIC",
    "DEFAULT_GROUP_ID",
    "kafka_bootstrap",
    "kafka_topic",
    "build_fix_raw",
]

#: Environment variable holding the Kafka bootstrap servers (doc 05 section 4).
BOOTSTRAP_ENV = "FIX42_KAFKA_BOOTSTRAP"
#: Environment variable holding the source topic (doc 05 section 4).
TOPIC_ENV = "FIX42_TOPIC"

#: Compose-network default -- the Kafka service name (doc 04 section 7).
DEFAULT_BOOTSTRAP = "kafka:9092"
#: Project-wide topic convention (doc 00 section 5).
DEFAULT_TOPIC = "fix42.messages"
#: Consumer group id (doc 03 section 2.1).
DEFAULT_GROUP_ID = "dh-fix42-dashboard"


def kafka_bootstrap() -> str:
    """Return the configured Kafka bootstrap servers."""
    return os.environ.get(BOOTSTRAP_ENV, DEFAULT_BOOTSTRAP)


def kafka_topic() -> str:
    """Return the configured source topic."""
    return os.environ.get(TOPIC_ENV, DEFAULT_TOPIC)


def build_fix_raw(
    bootstrap: str | None = None,
    topic: str | None = None,
    group_id: str = DEFAULT_GROUP_ID,
) -> Table:
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
