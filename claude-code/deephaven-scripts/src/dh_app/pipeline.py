"""The single stateful DAG node: FIX state-machine listener + publishers.

Doc 03 sections 2.2/2.3.  A :class:`~deephaven.table_listener.TableListenerHandle`
folds every ``RawFix`` value of the ``fix_raw`` blink table through one
``fix42cache.state_machine.OrderStateMachine`` instance and republishes the
resulting normalized rows through five ``TablePublisher``s.

Invariants (doc 04 section 9 gotchas):

1. Strong references to the listener handle, publishers and blink tables are held
   by the :class:`Pipeline` instance (which ``app.py`` keeps in a global).
2. Table construction inside the listener happens under the execution context
   captured at construction time (``with self._ctx:``).
3. One batch per publisher per update cycle -- never one ``add()`` per row.
4. Nothing raises out of the callback: per-message failures land in
   ``ingest_errors`` and are printed to the server log.

This module contains **no FIX business logic**; it only adapts ``fix42cache`` row
dicts to Deephaven columns.
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

from dh_app import schemas

try:  # doc 04 flags this helper's location as version-drifting
    from deephaven.time import to_j_instant
except ImportError:  # pragma: no cover - older servers exposed it as to_datetime
    from deephaven.time import to_datetime as to_j_instant

try:
    from deephaven.constants import NULL_DOUBLE, NULL_LONG
except ImportError:  # pragma: no cover - keep working if the module ever moves
    NULL_DOUBLE = -1.7976931348623157e308
    NULL_LONG = -(2**63)

__all__ = ["Pipeline"]


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


def _column_factory(name: str, dtype: dht.DType) -> Callable[[Sequence[Any]], Any]:
    """Resolve, once at startup, the column factory + coercion for one column.

    Args:
        name: Column name (used to decide boolean nullability).
        dtype: The Deephaven dtype declared in :mod:`dh_app.schemas`.

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

    def __init__(self, name: str, schema: Mapping[str, dht.DType]) -> None:
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


class Pipeline:
    """Wires the FIX state machine into Deephaven as the DAG's one stateful node.

    Usage::

        pipeline = Pipeline()
        streams = pipeline.start(fix_raw)     # dict of doc 03 section 2.3 blink tables

    The instance must be kept alive (a module global in ``app.py``): dropping it
    would let the listener handle be garbage collected and the stream would stop.
    """

    def __init__(self, machine: Any = None) -> None:
        """Create the publishers and capture the execution context.

        Args:
            machine: Optional pre-built ``OrderStateMachine``; by default one is
                constructed from :mod:`fix42cache.state_machine`.
        """
        if machine is None:
            from fix42cache.state_machine import OrderStateMachine

            machine = OrderStateMachine()
        self._machine = machine
        # Captured here (the setup thread), entered inside the listener callback so
        # tables can be created on the update-graph thread. Doc 04 section 1.
        self._ctx = get_exec_ctx()
        self._streams: Dict[str, _Stream] = {
            name: _Stream(name, schema) for name, schema in schemas.ALL_SCHEMAS.items()
        }
        self._handle: Any = None
        self._source: Optional[Table] = None
        self._processed = 0
        self._failed = 0

    # -- accessors ---------------------------------------------------------------

    @property
    def machine(self) -> Any:
        """The single ``OrderStateMachine`` instance driving the fold."""
        return self._machine

    @property
    def processed_count(self) -> int:
        """Messages successfully folded into the cache."""
        return self._processed

    @property
    def failed_count(self) -> int:
        """Messages routed to ``ingest_errors``."""
        return self._failed

    @property
    def tables(self) -> Dict[str, Table]:
        """The blink tables keyed by their doc 03 section 2.3 names."""
        return {name: stream.table for name, stream in self._streams.items()}

    # -- lifecycle ---------------------------------------------------------------

    def start(self, fix_raw: Table) -> Dict[str, Table]:
        """Subscribe to ``fix_raw`` and begin publishing.

        Args:
            fix_raw: The Kafka blink table from :func:`dh_app.ingest.build_fix_raw`.

        Returns:
            The blink tables keyed by their doc 03 section 2.3 names
            (``order_state_blink``, ``executions_blink``, ``order_events_blink``,
            ``fix_messages_blink``, ``ingest_errors``).

        Raises:
            RuntimeError: If the pipeline was already started.
        """
        if self._handle is not None:
            raise RuntimeError("Pipeline.start() called twice")
        self._source = fix_raw
        self._handle = listen(fix_raw, self._on_update, description="fix42-state-machine")
        return self.tables

    def stop(self) -> None:
        """Unsubscribe the listener (best effort; used by tests / reloads)."""
        handle = self._handle
        self._handle = None
        if handle is None:
            return
        try:
            handle.stop()
        except Exception:  # noqa: BLE001 - shutdown must never raise
            traceback.print_exc()

    # -- the fold ----------------------------------------------------------------

    def _on_update(self, update: Any, is_replay: bool = False) -> None:
        """Table-listener callback: fold added rows, publish one batch per stream.

        Runs on the update-graph thread; it is O(added rows) and never raises.

        Args:
            update: The ``TableUpdate`` for this cycle.
            is_replay: True during initial-snapshot replay (always empty for blink).
        """
        try:
            raw_values = self._added_raw(update)
            if raw_values is None or len(raw_values) == 0:
                return
            with self._ctx:
                self._process_batch(raw_values)
        except Exception:  # noqa: BLE001 - a listener exception would kill the stream
            traceback.print_exc()

    @staticmethod
    def _added_raw(update: Any) -> Optional[Sequence[Any]]:
        """Extract the ``RawFix`` column of the cycle's added rows."""
        added = update.added()
        if not added:
            return None
        return added.get("RawFix")

    def _process_batch(self, raw_values: Sequence[Any]) -> None:
        """Run the state machine over ``raw_values`` and publish the accumulated rows."""
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
                result = self._machine.process(raw)
            except Exception as exc:  # noqa: BLE001 - malformed input must not stop us
                error_rows.append(self._error_row(raw, f"{type(exc).__name__}: {exc}"))
                self._failed += 1
                traceback.print_exc()
                continue

            error = getattr(result, "error", None)
            if error:
                error_rows.append(self._error_row(raw, str(error)))
                self._failed += 1
                continue

            self._collect(result, state_rows, execution_rows, event_rows, message_rows)
            self._processed += 1

        self._publish("order_state_blink", state_rows, error_rows)
        self._publish("executions_blink", execution_rows, error_rows)
        self._publish("order_events_blink", event_rows, error_rows)
        self._publish("fix_messages_blink", message_rows, error_rows)
        # Errors last: it may have grown while publishing the other four streams.
        self._publish("ingest_errors", error_rows, None)

    def _collect(
        self,
        result: Any,
        state_rows: List[Mapping[str, Any]],
        execution_rows: List[Mapping[str, Any]],
        event_rows: List[Mapping[str, Any]],
        message_rows: List[Mapping[str, Any]],
    ) -> None:
        """Append a ``Result``'s rows to the per-cycle accumulators."""
        state = getattr(result, "state", None)
        if state is not None:
            state_rows.append(state.to_row())
        for execution in getattr(result, "executions", None) or ():
            execution_rows.append(execution.to_row())
        for event in getattr(result, "events", None) or ():
            event_rows.append(event.to_row())
        message = getattr(result, "message", None)
        if message is not None:
            message_rows.append(message.to_row())

    def _publish(
        self,
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
                error_sink.append(self._error_row("", message))
            else:
                print(f"[fix42] {message}")

    def _error_row(self, raw: str, error: str) -> Dict[str, Any]:
        """Build one ``ingest_errors`` row (and echo it to the server log)."""
        print(f"[fix42] ingest error: {error} | raw={raw[:200]!r}")
        return {"RawFix": raw, "Error": error, "IngestTs": _utcnow()}
