"""One AMPS bookmark subscription per local hub -- doc 10 section 5.1.

The leaf's ingest half. ``dh_app.amps_ingest.AmpsRawSource`` is reused **unchanged**
(doc 05 section 8's module-ownership rule: it is the single-hub app's contract), with
one instance per hub built from an explicit :class:`~dh_app.amps_ingest.AmpsConfig`.

``AmpsConfig.from_env()`` is never called: it reads ``FIX42_*``, which on this stack
would point every hub at one topic and one client name. The whole configuration
comes from :class:`remote_uri.config.LeafSettings` instead, and the client name is
per (leaf, hub) because a duplicate AMPS logon *displaces the older connection*
(doc 10 section 2.6) -- two leaves sharing a name would silently take turns.

``deephaven`` and ``AMPS`` are imported inside the functions, so the naming helpers
here stay importable on a bare host python.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from multi_oms.config import HubConfig, Topology

from remote_uri.config import LeafSettings
from remote_uri.uris import raw_global_name

__all__ = [
    "raw_global_name",
    "amps_config_for",
    "build_hub_raw",
    "build_leaf_raw",
    "source_description",
    "pending_rows",
    "published_rows",
    "failed_batches",
    "topology_topics",
]


def amps_config_for(hub: HubConfig, settings: LeafSettings) -> Any:
    """Build the :class:`~dh_app.amps_ingest.AmpsConfig` for one local hub.

    Args:
        hub: The hub whose topic to replay.
        settings: This leaf's configuration.

    Returns:
        An ``AmpsConfig`` with an explicit topic, filter, client name, bookmark and
        buffer bound -- never :meth:`AmpsConfig.from_env`.
    """
    from dh_app.amps_ingest import AmpsConfig

    return AmpsConfig(
        uris=settings.amps_uris,
        topic=hub.topic,
        filter=settings.amps_filter or None,
        client_name=settings.client_name(hub.name),
        bookmark=settings.bookmark,
        max_pending=settings.max_pending,
    )


def build_hub_raw(hub: HubConfig, settings: LeafSettings) -> Tuple[Any, Any]:
    """Start one hub's AMPS subscription.

    Args:
        hub: The hub to subscribe.
        settings: This leaf's configuration.

    Returns:
        ``(blink table, source)``. The blink table carries ``RawFix``,
        ``AmpsBookmark`` and ``IngestTs``; the **source must be kept alive** -- an
        ``AmpsRawSource`` that is garbage collected takes its AMPS client, and the
        stream, with it.
    """
    from dh_app.amps_ingest import AmpsRawSource

    source = AmpsRawSource(config=amps_config_for(hub, settings))
    table = source.start()
    return table, source


def build_leaf_raw(settings: LeafSettings) -> Tuple[Dict[str, Any], List[Any]]:
    """Start one subscription per hub this leaf folds.

    Args:
        settings: This leaf's configuration (``settings.local`` names the hubs).

    Returns:
        ``({hub name: blink Table}, [AmpsRawSource, ...])`` in topology order. On a
        partial failure every already-started source is stopped before the exception
        propagates: a half-subscribed leaf would export a silently incomplete
        ``rx_orders`` that the collector cannot tell from a slow one.
    """
    raw: Dict[str, Any] = {}
    sources: List[Any] = []
    try:
        for hub in settings.local:
            table, source = build_hub_raw(hub, settings)
            raw[hub.name] = table
            sources.append(source)
    except Exception:
        for source in sources:
            try:
                source.stop()
            except Exception:  # noqa: BLE001 - cleanup must not mask the real failure
                pass
        raise
    return raw, sources


def source_description(settings: LeafSettings) -> str:
    """One-line summary of where this leaf's tapes come from, for the banner."""
    topics = [hub.topic for hub in settings.local]
    filtered = f" filter={settings.amps_filter!r}" if settings.amps_filter else ""
    return (
        f"amps: {','.join(settings.amps_uris)} topics={topics} "
        f"bookmark={settings.bookmark}{filtered}"
    )


def pending_rows(sources: List[Any]) -> int:
    """Rows buffered between the AMPS reader threads and the update graph."""
    total = 0
    for source in sources:
        try:
            total += int(source.buffer.pending)
        except Exception:  # noqa: BLE001 - a stats read must never raise
            continue
    return total


def published_rows(sources: List[Any]) -> int:
    """Rows this leaf's AMPS subscriptions have handed to the update graph."""
    total = 0
    for source in sources:
        try:
            total += int(source.published)
        except Exception:  # noqa: BLE001 - a stats read must never raise
            continue
    return total


def failed_batches(sources: List[Any]) -> int:
    """Publish batches that failed on the update-graph thread (should stay ``0``)."""
    total = 0
    for source in sources:
        try:
            total += int(source.failed_batches)
        except Exception:  # noqa: BLE001 - a stats read must never raise
            continue
    return total


def topology_topics(topology: Topology) -> Tuple[str, ...]:
    """The AMPS topics a (sub-)topology subscribes, in configuration order."""
    return topology.topics
