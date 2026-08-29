"""The stateful nodes: one FIX fold per hub tape -> one shared set of publishers.

Doc 09 section 4.  A deliberate, documented extension of doc 00's "exactly one
stateful node": it is one stateful node **per tape**.  The machines share nothing,
each is the same single-writer fold as the single-hub app, and the only state this
module adds beyond ``fix42cache`` is the per-hub sticky link map
(:class:`multi_oms.linking.LinkTracker`).

Per raw message on a hub's tape (doc 09 section 4, steps 1-5):

1. ``fields = parse_fix(raw)`` -- parse **once**;
2. ``ext = fields.get(link_tag, "")`` -- only hubs with an upstream extract anything;
3. ``result = machine.process_fields(fields, raw)`` -- the unchanged fix42cache fold;
4. sticky-map ``{OrderKey: first non-empty ExtOrdID}``;
5. publish, every row augmented with ``Oms`` / ``GlobalKey`` (state rows also with
   the sticky ``ExtOrdID``).

Invariants (doc 04 section 9 gotchas), unchanged from ``dh_app.pipeline``:

1. Strong references to every listener handle, machine, tracker, publisher and blink
   table are held by the :class:`MultiOmsPipeline` instance (which ``app.py`` keeps
   in a global).
2. Table construction inside a listener happens under the execution context captured
   at construction time (``with self._ctx:``).
3. One batch per publisher per listener callback -- never one ``add()`` per row. The
   publishers are shared across hubs, so a cycle in which all four hubs tick
   produces at most one batch per hub per stream; each hub's listener fires
   separately and that is the finest granularity ``listen()`` offers.
4. Nothing raises out of a callback: per-message failures land in
   ``oms_ingest_errors`` (tagged with the hub) and are printed to the server log.

This module contains **no FIX business logic**; ``fix42cache`` is used unmodified.
The coercion/publisher helpers below are duplicated from ``dh_app.pipeline`` rather
than imported (doc 05 section 8 module ownership, doc 09 section 4.1).
"""

from __future__ import annotations

import traceback
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from deephaven import dtypes as dht
from deephaven import new_table
from deephaven.column import bool_col, double_col, long_col, string_col

try:  # server 42.x exports the Instant column factory as datetime_col
    from deephaven.column import datetime_col as instant_col
except ImportError:  # pragma: no cover - other versions export instant_col directly
    from deephaven.column import instant_col
from deephaven.execution_context import get_exec_ctx
from deephaven.stream.table_publisher import table_publisher
from deephaven.table import Table
from deephaven.table_listener import listen

from multi_oms import schemas
from multi_oms.config import HubConfig, Topology
from multi_oms.linking import LinkTracker, augment_hub_row, augment_row, augment_state_row

try:  # doc 04 flags this helper's location as version-drifting
    from deephaven.time import to_j_instant
except ImportError:  # pragma: no cover - older servers exposed it as to_datetime
    from deephaven.time import to_datetime as to_j_instant

try:
    from deephaven.constants import NULL_DOUBLE, NULL_LONG
except ImportError:  # pragma: no cover - keep working if the module ever moves
    NULL_DOUBLE = -1.7976931348623157e308
    NULL_LONG = -(2**63)

__all__ = ["MultiOmsPipeline"]


# --------------------------------------------------------------------------------------
# Value coercion helpers -- python values from fix42cache -> Deephaven cell values.
# --------------------------------------------------------------------------------------


def _as_string(value: Any) -> str:
    """Coerce to a non-null string ("" when absent, per doc 01 section 6)."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _as_double(value: Any) -> float:
    """Coerce to a double, mapping ``None``/garbage to Deephaven's null double."""
    if value is None:
        return NULL_DOUBLE
    try:
        return float(value)
    except (TypeError, ValueError):
        return NULL_DOUBLE


def _as_long(value: Any) -> int:
    """Coerce to a long, mapping ``None``/garbage to Deephaven's null long."""
    if value is None:
        return NULL_LONG
    try:
        return int(value)
    except (TypeError, ValueError):
        return NULL_LONG


def _as_bool(value: Any, nullable: bool) -> Optional[bool]:
    """Coerce to a Boolean; ``None`` stays null only for tri-state columns."""
    if value is None:
        return None if nullable else False
    if isinstance(value, bool):
        return value
    return bool(value)


def _as_instant(value: Any) -> Any:
    """Convert a tz-aware ``datetime`` (or anything ``to_j_instant`` groks) to Instant.

    Returns ``None`` (a null Instant cell) for missing or unconvertible values so a
    single bad timestamp can never poison an entire batch.
    """
    if value is None:
        return None
    try:
        return to_j_instant(value)
    except Exception:  # noqa: BLE001 - defensive: never fail a batch on a timestamp
        return None


def _column_factory(name: str, dtype: "dht.DType") -> Callable[[Sequence[Any]], Any]:
    """Resolve, once at startup, the column factory + coercion for one column.

    Args:
        name: Column name (used to decide boolean nullability).
        dtype: The Deephaven dtype declared in :mod:`multi_oms.schemas`.

    Returns:
        A callable turning a list of raw python values into a Deephaven input column.

    Raises:
        ValueError: If the schema declares a dtype this module cannot build.
    """
    if dtype is dht.string:
        return lambda values: string_col(name, [_as_string(v) for v in values])
    if dtype is dht.double:
        return lambda values: double_col(name, [_as_double(v) for v in values])
    if dtype is dht.long:
        return lambda values: long_col(name, [_as_long(v) for v in values])
    if dtype is dht.bool_:
        nullable = name in schemas.NULLABLE_BOOLEAN_COLUMNS
        return lambda values: bool_col(name, [_as_bool(v, nullable) for v in values])
    if dtype is dht.Instant:
        return lambda values: instant_col(name, [_as_instant(v) for v in values])
    raise ValueError(f"unsupported dtype {dtype!r} for column {name!r}")


def _utcnow() -> datetime:
    """Current time as a tz-aware UTC datetime."""
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------------------
# Stream wrapper
# --------------------------------------------------------------------------------------


class _Stream:
    """One publisher: its blink table, schema-bound column builders and row buffer."""

    def __init__(self, name: str, schema: Mapping[str, "dht.DType"]) -> None:
        """Create the publisher pair for ``name`` using ``schema``."""
        self.name = name
        self.columns: Tuple[str, ...] = tuple(schema)
        self._factories = [_column_factory(col, schema[col]) for col in self.columns]
        self.table, self.publisher = table_publisher(name, dict(schema))

    def build(self, rows: Sequence[Mapping[str, Any]]) -> Table:
        """Build a single static batch table from ``rows`` (missing keys -> null)."""
        cols = []
        for col_name, factory in zip(self.columns, self._factories):
            cols.append(factory([row.get(col_name) for row in rows]))
        return new_table(cols)

    def publish(self, rows: Sequence[Mapping[str, Any]]) -> None:
        """Publish ``rows`` as one batch. Raises only on unrecoverable schema errors."""
        if not rows:
            return
        self.publisher.add(self.build(rows))


# --------------------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------------------


class MultiOmsPipeline:
    """Wires one FIX state machine per hub into Deephaven's shared publishers.

    Usage::

        pipeline = MultiOmsPipeline(topology)
        streams = pipeline.start(raw_tables)   # {hub name: blink table} -> streams

    The instance must be kept alive (a module global in ``app.py``): dropping it
    would let the listener handles be garbage collected and every stream would stop.
    """

    def __init__(self, topology: Topology, machines: Optional[Mapping[str, Any]] = None) -> None:
        """Create the shared publishers, per-hub machines/trackers and capture the context.

        Args:
            topology: The validated hub graph (doc 09 section 3).
            machines: Optional pre-built ``{hub name: OrderStateMachine}``; by default
                one is constructed per hub from :mod:`fix42cache.state_machine`.
        """
        from fix42cache.parser import parse_fix
        from fix42cache.state_machine import OrderStateMachine

        self._parse_fix = parse_fix
        self._topology = topology
        # Captured here (the setup thread), entered inside the listener callbacks so
        # tables can be created on the update-graph thread. Doc 04 section 1.
        self._ctx = get_exec_ctx()
        self._streams: Dict[str, _Stream] = {
            name: _Stream(name, schema) for name, schema in schemas.ALL_SCHEMAS.items()
        }
        supplied = dict(machines or {})
        self._machines: Dict[str, Any] = {
            hub.name: supplied.get(hub.name) or OrderStateMachine() for hub in topology
        }
        self._trackers: Dict[str, LinkTracker] = {hub.name: LinkTracker() for hub in topology}
        self._handles: Dict[str, Any] = {}
        self._sources: Dict[str, Table] = {}
        self._processed: Dict[str, int] = {hub.name: 0 for hub in topology}
        self._failed: Dict[str, int] = {hub.name: 0 for hub in topology}

    # -- accessors ---------------------------------------------------------------

    @property
    def topology(self) -> Topology:
        """The hub graph this pipeline folds."""
        return self._topology

    @property
    def machines(self) -> Dict[str, Any]:
        """The ``OrderStateMachine`` instances, keyed by hub name."""
        return dict(self._machines)

    @property
    def trackers(self) -> Dict[str, LinkTracker]:
        """The sticky link maps, keyed by hub name."""
        return dict(self._trackers)

    @property
    def processed_counts(self) -> Dict[str, int]:
        """Messages successfully folded, per hub."""
        return dict(self._processed)

    @property
    def failed_counts(self) -> Dict[str, int]:
        """Messages routed to ``oms_ingest_errors``, per hub."""
        return dict(self._failed)

    @property
    def processed_count(self) -> int:
        """Messages successfully folded across every hub."""
        return sum(self._processed.values())

    @property
    def failed_count(self) -> int:
        """Messages routed to ``oms_ingest_errors`` across every hub."""
        return sum(self._failed.values())

    @property
    def tables(self) -> Dict[str, Table]:
        """The blink tables keyed by their doc 09 section 4.1 names."""
        return {name: stream.table for name, stream in self._streams.items()}

    # -- lifecycle ---------------------------------------------------------------

    def start(self, raw_tables: Mapping[str, Table]) -> Dict[str, Table]:
        """Subscribe one listener per hub and begin publishing.

        Args:
            raw_tables: ``{hub name: blink Table}`` from
                :func:`multi_oms.ingest.build_all_raw`.

        Returns:
            The blink tables keyed by their doc 09 section 4.1 names
            (``oms_order_state_blink``, ``oms_executions_blink``,
            ``oms_order_events_blink``, ``oms_fix_messages_blink``,
            ``oms_ingest_errors``).

        Raises:
            RuntimeError: If the pipeline was already started.
            KeyError: If a configured hub has no raw table.
        """
        if self._handles:
            raise RuntimeError("MultiOmsPipeline.start() called twice")
        for hub in self._topology:
            source = raw_tables[hub.name]
            self._sources[hub.name] = source
            self._handles[hub.name] = listen(
                source,
                self._make_listener(hub),
                description=f"multi-oms-{hub.name}",
            )
        return self.tables

    def stop(self) -> None:
        """Unsubscribe every listener (best effort; used by tests / reloads)."""
        handles = self._handles
        self._handles = {}
        for handle in handles.values():
            try:
                handle.stop()
            except Exception:  # noqa: BLE001 - shutdown must never raise
                traceback.print_exc()

    # -- the fold ----------------------------------------------------------------

    def _make_listener(self, hub: HubConfig) -> Callable[..., None]:
        """Build the table-listener callback for one hub (closes over its config)."""

        def on_update(update: Any, is_replay: bool = False) -> None:
            """Fold this cycle's added rows for ``hub``; never raises."""
            try:
                raw_values = self._added_raw(update)
                if raw_values is None or len(raw_values) == 0:
                    return
                with self._ctx:
                    self._process_batch(hub, raw_values)
            except Exception:  # noqa: BLE001 - a listener exception would kill the stream
                traceback.print_exc()

        return on_update

    @staticmethod
    def _added_raw(update: Any) -> Optional[Sequence[Any]]:
        """Extract the ``RawFix`` column of the cycle's added rows."""
        added = update.added()
        if not added:
            return None
        return added.get("RawFix")

    def _process_batch(self, hub: HubConfig, raw_values: Sequence[Any]) -> None:
        """Fold ``raw_values`` through ``hub``'s machine and publish the rows."""
        machine = self._machines[hub.name]
        tracker = self._trackers[hub.name]
        link_tag = hub.link_tag

        state_rows: List[Mapping[str, Any]] = []
        execution_rows: List[Mapping[str, Any]] = []
        event_rows: List[Mapping[str, Any]] = []
        message_rows: List[Mapping[str, Any]] = []
        error_rows: List[Mapping[str, Any]] = []

        for value in raw_values:
            raw = _as_string(value)
            if not raw:
                continue
            try:
                # 1. parse once -- the state machine is fed the already-parsed fields
                #    so the link tag can be read without a second pass over the wire.
                fields = self._parse_fix(raw)
            except Exception as exc:  # noqa: BLE001 - parse_fix is total, but never trust it
                error_rows.append(self._error_row(hub, raw, f"{type(exc).__name__}: {exc}"))
                self._failed[hub.name] += 1
                traceback.print_exc()
                continue
            if not fields:
                error_rows.append(self._error_row(hub, raw, "unparseable: no FIX fields found"))
                self._failed[hub.name] += 1
                continue

            # 2. only hubs with an upstream carry a link tag.
            ext = _as_string(fields.get(link_tag, "")) if link_tag else ""

            try:
                # 3. the unchanged fix42cache fold.
                result = machine.process_fields(fields, raw)
            except Exception as exc:  # noqa: BLE001 - malformed input must not stop us
                error_rows.append(self._error_row(hub, raw, f"{type(exc).__name__}: {exc}"))
                self._failed[hub.name] += 1
                traceback.print_exc()
                continue

            error = getattr(result, "error", None)
            if error:
                error_rows.append(self._error_row(hub, raw, str(error)))
                self._failed[hub.name] += 1
                continue

            # 4./5. sticky link map + row augmentation.
            self._collect(
                hub, tracker, ext, result, state_rows, execution_rows, event_rows, message_rows
            )
            self._processed[hub.name] += 1

        self._publish(hub, "oms_order_state_blink", state_rows, error_rows)
        self._publish(hub, "oms_executions_blink", execution_rows, error_rows)
        self._publish(hub, "oms_order_events_blink", event_rows, error_rows)
        self._publish(hub, "oms_fix_messages_blink", message_rows, error_rows)
        # Errors last: it may have grown while publishing the other four streams.
        self._publish(hub, "oms_ingest_errors", error_rows, None)

    def _collect(
        self,
        hub: HubConfig,
        tracker: LinkTracker,
        ext: str,
        result: Any,
        state_rows: List[Mapping[str, Any]],
        execution_rows: List[Mapping[str, Any]],
        event_rows: List[Mapping[str, Any]],
        message_rows: List[Mapping[str, Any]],
    ) -> None:
        """Append one ``Result``'s augmented rows to the per-cycle accumulators."""
        oms = hub.name
        state = getattr(result, "state", None)
        if state is not None:
            row = state.to_row()
            # The OrderKey is only known after the fold, so the sticky map is written
            # here rather than at extraction time.
            sticky = tracker.record(row.get("OrderKey", ""), ext)
            state_rows.append(augment_state_row(row, oms, sticky))
        for execution in getattr(result, "executions", None) or ():
            execution_rows.append(augment_row(execution.to_row(), oms))
        for event in getattr(result, "events", None) or ():
            event_rows.append(augment_row(event.to_row(), oms))
        message = getattr(result, "message", None)
        if message is not None:
            message_rows.append(augment_hub_row(message.to_row(), oms))

    def _publish(
        self,
        hub: HubConfig,
        stream_name: str,
        rows: Sequence[Mapping[str, Any]],
        error_sink: Optional[List[Mapping[str, Any]]],
    ) -> None:
        """Publish one batch, diverting build/add failures to ``error_sink``."""
        if not rows:
            return
        stream = self._streams[stream_name]
        try:
            stream.publish(rows)
        except Exception as exc:  # noqa: BLE001 - one bad batch must not stop ingest
            traceback.print_exc()
            message = f"publish to {stream_name} failed: {type(exc).__name__}: {exc}"
            if error_sink is not None:
                error_sink.append(self._error_row(hub, "", message))
            else:
                print(f"[multi-oms] {message}")

    def _error_row(self, hub: HubConfig, raw: str, error: str) -> Dict[str, Any]:
        """Build one ``oms_ingest_errors`` row (and echo it to the server log)."""
        print(f"[multi-oms] {hub.name} ingest error: {error} | raw={raw[:200]!r}")
        return {"Oms": hub.name, "RawFix": raw, "Error": error, "IngestTs": _utcnow()}
