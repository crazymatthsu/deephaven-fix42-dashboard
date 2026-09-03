"""The leaf app: fold the local hub tapes and export the four globals -- doc 10 section 5.

A leaf is doc 09's blotter with the linking half removed and four exports bolted on.
Everything stateful is reused unchanged: ``fix42cache`` folds each tape,
``multi_oms.pipeline.MultiOmsPipeline`` adapts the folds to Deephaven publishers, and
``multi_oms.dag.build_id_index`` builds the id index. This module adds only the
per-leaf DAG (section 5.2) and the export projection (section 5.3).

Two rules from the design review are load-bearing here and must not be "optimised":

1. **``rx_orders`` is never filtered by order state.** Doc 09's per-edge rollup sums
   a parent's *direct* children; dropping a filled child while its parent is still
   open fabricates ``QTY_BREAK``/``UNROUTED`` on the collector and breaks ``RootKey``
   walks. Pruning, when it is built, prunes whole *families* by age, after linking.
2. **``rx_orders`` carries no ``Leaf`` column.** Every leaf's schema must be
   byte-identical for the collector's ``merge``; the ``Leaf`` value is attached
   there by a ``natural_join`` on ``leaf_config``.

``oms_fix_messages`` is deliberately not built (doc 10 section 2.4): at 400M messages
it is by some distance the largest table, and AMPS is the audit trail.
"""

from __future__ import annotations

import traceback
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Tuple

from deephaven import agg, new_table
from deephaven.column import long_col, string_col
from deephaven.stream import blink_to_append_only
from deephaven.table_factory import function_generated_table

try:  # server 42.x exports the Instant column factory as datetime_col
    from deephaven.column import datetime_col as instant_col
except ImportError:  # pragma: no cover - other versions export instant_col directly
    from deephaven.column import instant_col

try:  # doc 04 flags this helper's location as version-drifting
    from deephaven.time import to_j_instant
except ImportError:  # pragma: no cover - older servers exposed it as to_datetime
    from deephaven.time import to_datetime as to_j_instant

from multi_oms.dag import build_id_index
from multi_oms.pipeline import MultiOmsPipeline

from remote_uri import ingest
from remote_uri.config import LeafSettings
from remote_uri.uris import LEAF_EXPORTS, raw_global_name

__all__ = [
    "RX_ORDERS_COLUMNS",
    "RX_EXPOSURE_BY",
    "RX_EXPOSURE_SUMS",
    "NOTIONAL_FORMULA",
    "LEAF_DAG_NAMES",
    "LeafRuntime",
    "build_leaf",
]

#: ``rx_orders`` -- the frozen projection of ``oms_orders_latest`` (doc 10 section 5.3).
#: No state filter, no ``Leaf`` column: identical on every leaf, so ``merge`` works.
RX_ORDERS_COLUMNS: Tuple[str, ...] = (
    "Oms",
    "GlobalKey",
    "ExtOrdID",
    "OrderKey",
    "OrderID",
    "ClOrdID",
    "Account",
    "Symbol",
    "Side",
    "OrdStatus",
    "OrderQty",
    "Price",
    "CumQty",
    "LeavesQty",
    "AvgPx",
    "LastUpdateTs",
    "Terminal",
)

#: ``rx_exposure`` grouping -- per-hub totals over *all* orders, no rows on the wire.
RX_EXPOSURE_BY: Tuple[str, ...] = ("Oms", "Account", "Symbol")
#: ``rx_exposure`` sums.
RX_EXPOSURE_SUMS: Tuple[str, ...] = ("OrderQty", "CumQty", "LeavesQty", "Notional")

#: doc 09's null-safe notional, recomputed on the leaf so ``rx_exposure`` needs no
#: order rows to travel.
NOTIONAL_FORMULA = "Notional = (isNull(AvgPx) ? 0.0 : AvgPx) * (isNull(CumQty) ? 0.0 : CumQty)"

#: The leaf's own DAG globals (doc 10 section 5.2), in dependency order.
LEAF_DAG_NAMES: Tuple[str, ...] = (
    "oms_orders_latest",
    "oms_executions",
    "oms_events",
    "id_index",
)

#: ``rx_leaf_stats`` columns, in order (doc 10 section 5.3).
_STATS_LONGS: Tuple[str, ...] = (
    "Orders",
    "Executions",
    "Processed",
    "Failed",
    "Pending",
    "HeapUsedMb",
)


def _utcnow() -> datetime:
    """Current time as a tz-aware UTC datetime."""
    return datetime.now(timezone.utc)


def _heap_used_mb() -> int:
    """JVM heap in use, in MiB (``0`` if jpy ever stops exposing ``Runtime``)."""
    try:
        import jpy

        runtime = jpy.get_type("java.lang.Runtime").getRuntime()
        return int((runtime.totalMemory() - runtime.freeMemory()) // (1024 * 1024))
    except Exception:  # noqa: BLE001 - a stats read must never break the table
        return 0


def _size_of(table: Any) -> int:
    """``table.size`` as an int, or ``0`` when the table refuses to answer."""
    try:
        return int(table.size)
    except Exception:  # noqa: BLE001 - a stats read must never break the table
        return 0


class LeafRuntime:
    """Everything the leaf wiring produced; kept alive by a module-level global.

    Deliberately a plain class rather than a ``dataclass``: Application Mode may exec
    ``app.py`` with a ``__name__`` that is not registered in ``sys.modules``, which
    breaks ``dataclasses``' type introspection at class-creation time.

    The instance is also the **strong reference** keeping the AMPS sources and the
    pipeline's listener handles alive; dropping it stops every stream.
    """

    def __init__(
        self,
        settings: LeafSettings,
        raw: Dict[str, Any],
        sources: List[Any],
        pipeline: MultiOmsPipeline,
        streams: Dict[str, Any],
        tables: Dict[str, Any],
    ) -> None:
        """Store the wired objects."""
        self.role = "leaf"
        self.settings = settings
        self.raw = raw
        self.sources = sources
        self.pipeline = pipeline
        self.streams = streams
        self.tables = tables
        self.ready = True

    @property
    def name(self) -> str:
        """This leaf's name (the ``Leaf`` value the collector attaches)."""
        return self.settings.name

    @property
    def id_index_route(self) -> str:
        """Which source ``id_index`` was built from (blink, or the fallback)."""
        return str(self.tables.get("_id_index_route", "blink"))

    def bind_namespace(self, namespace: Any) -> None:
        """No-op: a leaf never re-exports (only the collector's ``reconnect`` does)."""

    def exports(self) -> Dict[str, Any]:
        """Every global this runtime publishes (doc 10 section 5.3)."""
        published: Dict[str, Any] = {}
        for hub_name, table in self.raw.items():
            published[raw_global_name(hub_name)] = table
        published.update(self.streams)
        published.update(
            {name: value for name, value in self.tables.items() if not name.startswith("_")}
        )
        published["remote_uri_pipeline"] = self.pipeline
        return published

    @property
    def table_names(self) -> List[str]:
        """Every global table name this runtime exports, sorted."""
        return sorted(name for name in self.exports() if name != "remote_uri_pipeline")

    def banner_lines(self) -> List[str]:
        """The startup banner body (``app.py`` adds the rule lines and the title)."""
        settings = self.settings
        return [
            f"  leaf            : {settings.name}",
            f"  source          : {ingest.source_description(settings)}",
            f"  hubs            : {list(settings.hub_names)}",
            settings.local.describe(),
            f"  raw blinks      : {', '.join(raw_global_name(n) for n in settings.hub_names)}",
            f"  id_index        : over the {self.id_index_route}",
            f"  exports         : {', '.join(LEAF_EXPORTS)} (+ leaf_config)",
            f"  tables          : {', '.join(self.table_names)}",
        ]



def _is_blink(table: Any) -> bool:
    """``Table.is_blink`` is a property on 42.x and a method on some other builds."""
    flag = getattr(table, "is_blink")
    return bool(flag() if callable(flag) else flag)

def _build_id_index_source(
    events_blink: Any, oms_events: Any
) -> Tuple[Any, str]:
    """Choose the source ``id_index`` is built from (doc 10 section 5.2).

    The blink stream is the right answer: ``last_by`` over a filtered blink keeps
    per-key state without retaining rows, so the index costs O(#ids) rather than
    O(#events) -- which is the difference between a few MB and tens of GB at 400M
    messages. ``where``/``view`` preserve the blink attribute (doc 02 section 1.1),
    but if a server ever disagrees the append-only table is the correct-but-costly
    fallback rather than a silently wrong index.

    Returns:
        ``(table, route)`` where ``route`` names the choice for the banner.
    """
    try:
        if _is_blink(events_blink.where("ClOrdID != ``")):
            return events_blink, "blink"
        print(
            "[remote-uri] the filtered oms_order_events_blink lost its blink attribute; "
            "building id_index over the append-only oms_events instead (correct, but "
            "O(#events) memory -- see doc 10 section 5.2)"
        )
    except Exception as exc:  # noqa: BLE001 - is_blink is the version-fragile bit
        print(
            f"[remote-uri] could not confirm the blink attribute "
            f"({type(exc).__name__}: {exc}); building id_index over oms_events"
        )
    return oms_events, "append-only fallback"


def _build_stats_table(
    settings: LeafSettings,
    pipeline: MultiOmsPipeline,
    sources: List[Any],
    orders: Any,
    executions: Any,
) -> Any:
    """``rx_leaf_stats``: one row, refreshed every ``REMOTEURI_STATS_PERIOD_MS``.

    A ``function_generated_table`` rather than a publisher: the row is a *snapshot*
    of counters that live in python, and the generator returns a fresh one-row table
    with an unchanging definition every refresh (doc 10 section 5.3). Passing
    ``source_tables`` as well would tie it to the update graph's cycles instead of
    to the wall clock -- the fleet-health panel must tick even when nothing arrives.
    """
    leaf_name = settings.name
    hubs = ",".join(settings.hub_names)

    def generate() -> Any:
        """Build the single stats row (never raises: a stall would stop the table)."""
        values = {
            "Orders": _size_of(orders),
            "Executions": _size_of(executions),
            "Processed": _safe_int(lambda: pipeline.processed_count),
            "Failed": _safe_int(lambda: pipeline.failed_count),
            "Pending": _safe_int(lambda: ingest.pending_rows(sources)),
            "HeapUsedMb": _heap_used_mb(),
        }
        return new_table(
            [
                string_col("Leaf", [leaf_name]),
                string_col("Hubs", [hubs]),
            ]
            + [long_col(name, [values[name]]) for name in _STATS_LONGS]
            + [instant_col("AsOf", [to_j_instant(_utcnow())])]
        )

    return function_generated_table(generate, refresh_interval_ms=settings.stats_period_ms)


def _safe_int(reader: Callable[[], Any]) -> int:
    """Call ``reader`` for an int, degrading to ``0`` rather than raising."""
    try:
        return int(reader())
    except Exception:  # noqa: BLE001 - a stats read must never break the table
        traceback.print_exc()
        return 0


def _leaf_config_table(settings: LeafSettings) -> Any:
    """The leaf's static ``leaf_config`` (``Leaf, Oms, Topic``) -- doc 10 section 5.3.

    Note the column set differs from the *collector's* ``leaf_config``
    (``Oms, Leaf, Uri``): here it documents which tapes this server folds; there it
    is the join that attaches ``Leaf`` to merged rows.
    """
    hubs = list(settings.local)
    return new_table(
        [
            string_col("Leaf", [settings.name] * len(hubs)),
            string_col("Oms", [hub.name for hub in hubs]),
            string_col("Topic", [hub.topic for hub in hubs]),
        ]
    )


def build_leaf(settings: LeafSettings) -> LeafRuntime:
    """Wire one leaf: AMPS -> folds -> leaf DAG -> the four exports.

    Args:
        settings: The validated leaf configuration (doc 10 section 4.2).

    Returns:
        The populated :class:`LeafRuntime`.

    Raises:
        Exception: Anything AMPS, ``fix42cache`` or Deephaven raises during wiring.
            ``app.py`` logs it and re-raises; the loader keeps the server up so the
            failure can be debugged from the IDE console.
    """
    raw, sources = ingest.build_leaf_raw(settings)
    pipeline = MultiOmsPipeline(settings.local)
    streams = pipeline.start(raw)

    order_state_blink = streams["oms_order_state_blink"]
    executions_blink = streams["oms_executions_blink"]
    events_blink = streams["oms_order_events_blink"]

    # -- 5.2 the per-leaf DAG ----------------------------------------------------
    oms_orders_latest = order_state_blink.last_by(["Oms", "OrderKey"])
    oms_executions = _retain(executions_blink, settings.exec_ring)
    oms_events = _retain(events_blink, settings.exec_ring)
    id_source, id_route = _build_id_index_source(events_blink, oms_events)
    id_index = build_id_index(id_source, oms_orders_latest)

    # -- 5.3 the frozen exports ---------------------------------------------------
    rx_orders = oms_orders_latest.view(list(RX_ORDERS_COLUMNS))
    rx_id_index = id_index
    rx_exposure = oms_orders_latest.update_view([NOTIONAL_FORMULA]).agg_by(
        [agg.count_("Orders"), agg.sum_(list(RX_EXPOSURE_SUMS))],
        by=list(RX_EXPOSURE_BY),
    )
    rx_leaf_stats = _build_stats_table(
        settings, pipeline, sources, oms_orders_latest, oms_executions
    )
    leaf_config = _leaf_config_table(settings)

    tables: Dict[str, Any] = {
        "oms_orders_latest": oms_orders_latest,
        "oms_executions": oms_executions,
        "oms_events": oms_events,
        "id_index": id_index,
        "rx_orders": rx_orders,
        "rx_id_index": rx_id_index,
        "rx_exposure": rx_exposure,
        "rx_leaf_stats": rx_leaf_stats,
        "leaf_config": leaf_config,
        "_id_index_route": id_route,
    }
    return LeafRuntime(
        settings=settings,
        raw=raw,
        sources=sources,
        pipeline=pipeline,
        streams=dict(streams),
        tables=tables,
    )


def _retain(blink: Any, ring: int) -> Any:
    """History retention for one blink stream (doc 10 sections 2.4 and 4.2).

    Args:
        blink: The publisher's blink table.
        ring: ``REMOTEURI_EXEC_RING``; ``0`` keeps everything.

    Returns:
        ``ring_table(blink, ring)`` when a capacity is configured -- executions are
        ~70% of the message count, so an unbounded append-only table is the one
        structure guaranteed to blow a leaf's heap at scale -- else
        ``blink_to_append_only(blink)``.
    """
    if ring and ring > 0:
        from deephaven.table_factory import ring_table

        return ring_table(blink, int(ring))
    return blink_to_append_only(blink)
