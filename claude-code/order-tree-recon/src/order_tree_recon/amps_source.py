"""The three native order tables, read from AMPS SOW topics -- doc 14 section 13.

Each system publishes the current state of every order to one SOW topic
(``otr.raptor_orders``, ``otr.algo_orders``, ``otr.router_orders``; JSON in the native
shape of :mod:`order_tree_recon.native`). This module turns each topic into the same
current-state table the mock source builds, so the adapters, the DAG, the API and the
dashboard run on it unchanged::

    AMPS receive thread                      TopicFeed                update-graph thread
    sow_and_subscribe(oof) --sow/publish-->  decode -> (row, False) --> on_flush -> publisher.add
                           --oof---------->  (key,  True)                 |
                                                                          v
                                    <table>_blink --last_by(key)--> where !Deleted --> <table>

``sow`` and ``publish`` messages are upserts, an ``oof`` (the record was ``sow_delete``d) is
a delete row; ``last_by`` on the blink table keeps the latest row per order id and the
``Deleted`` filter drops the deleted ones. Everything downstream is incremental, so one
publish recomputes one tree.

**Resync.** After a disconnect the client logs on again and re-issues the
``sow_and_subscribe``, which delivers a fresh snapshot. Any order that was in the table but
is missing from that snapshot was deleted while the connection was down (its OOF never
arrived), so :class:`TopicFeed` deletes it when the snapshot ends. Without that, a delete
during a failover would leave a stale order in the book for good.

``deephaven`` and ``AMPS`` are imported lazily, so :class:`TopicFeed` -- every message rule
-- is unit-tested on a bare host python.
"""

from __future__ import annotations

import threading
import traceback
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from order_tree_recon import amps_codec
from order_tree_recon.config import AmpsSettings
from order_tree_recon.native import DOUBLE, LONG, NATIVE_SCHEMAS, NativeSchema

__all__ = [
    "DELETED_COLUMN",
    "STATUS_COLUMNS",
    "DEFAULT_MAX_PENDING",
    "HEARTBEAT_SECONDS",
    "TopicFeed",
    "AmpsOrderSource",
    "start",
    "active_sources",
]

#: Blink-table flag: ``True`` on the row an OOF (a ``sow_delete``) produces.
DELETED_COLUMN = "Deleted"
#: ``otr_amps_status`` columns, one row per topic.
STATUS_COLUMNS: Tuple[str, ...] = (
    "Table", "Topic", "Connected", "State", "Snapshots", "Received", "Upserts", "Deletes",
    "Resynced", "Rejected", "Published", "FailedBatches", "Pending", "Live", "LastError",
)
#: Rows buffered between two update-graph cycles before the AMPS receive thread blocks.
DEFAULT_MAX_PENDING = 100_000
#: Records per batch in the SOW snapshot.
SOW_BATCH_SIZE = 100
#: Seconds between AMPS heartbeats. Without them a half-open connection (a dropped
#: firewall session, a frozen peer) stalls the feed silently while the status still says
#: connected; with them both sides drop it within a couple of intervals and the HA client
#: reconnects and re-snapshots.
HEARTBEAT_SECONDS = 5


class TopicFeed:
    """One topic's message rules: decode, track keys, buffer rows for the next cycle.

    Called from the AMPS receive thread (:meth:`handle`) and the update-graph thread
    (:meth:`drain`). Rows are ``schema.columns`` values plus a trailing ``Deleted`` flag.
    When the buffer is full :meth:`handle` blocks: a dropped message would silently
    leave a wrong order in the book, while a blocked receive thread is ordinary TCP
    backpressure.
    """

    def __init__(self, schema: NativeSchema, topic: str, max_pending: int = DEFAULT_MAX_PENDING) -> None:
        self.schema = schema
        self.topic = topic
        self._key_at = schema.column_names.index(schema.key)
        self._width = len(schema.columns)
        self._max = max(1, int(max_pending))
        self._cv = threading.Condition()
        self._rows: List[Tuple[Any, ...]] = []
        self._closed = False
        #: order id -> its AMPS SOW key, for every order currently in the table.
        self._live: Dict[str, str] = {}
        #: AMPS SOW key -> order id: an OOF names the record by SOW key.
        self._by_sow_key: Dict[str, str] = {}
        #: Order ids seen in the snapshot being received, or ``None`` outside one.
        self._snapshot: Optional[Set[str]] = None
        self.received = 0
        self.upserts = 0
        self.deletes = 0
        self.resynced = 0
        self.rejected = 0
        self.published = 0
        self.failed_batches = 0
        self.snapshots = 0
        self.last_error = ""

    # -- AMPS receive thread ------------------------------------------------------------

    def handle(self, command: str, data: Any, sow_key: Any) -> None:
        """Apply one AMPS message. Never raises on bad data (counted in ``rejected``)."""
        command = (command or "").lower()
        try:
            if command in ("sow", "publish", "p"):
                self.received += 1
                self._upsert(data, sow_key or "")
            elif command == "oof":
                self.received += 1
                self._oof(data, sow_key or "")
            elif command == "group_begin":
                self._snapshot = set()
            elif command == "group_end":
                self._end_snapshot()
        except amps_codec.DecodeError as exc:
            self.rejected += 1
            self.last_error = str(exc)[:200]

    def _upsert(self, data: Any, sow_key: str) -> None:
        values = amps_codec.decode(self.schema, data)
        order_id = values[self._key_at]
        self._live[order_id] = sow_key
        if sow_key:
            self._by_sow_key[sow_key] = order_id
        if self._snapshot is not None:
            self._snapshot.add(order_id)
        self._offer(values + (False,))
        self.upserts += 1

    def _oof(self, data: Any, sow_key: str) -> None:
        order_id = self._by_sow_key.get(sow_key) if sow_key else None
        if order_id is None:
            order_id = amps_codec.key_of(self.schema, data)
        if not order_id:
            raise amps_codec.DecodeError(f"{self.schema.name}: oof for an unknown record (sow key {sow_key!r})")
        self._delete(order_id)
        self.deletes += 1

    def _delete(self, order_id: str) -> None:
        sow_key = self._live.pop(order_id, None)
        if sow_key:
            self._by_sow_key.pop(sow_key, None)
        row: List[Any] = [None] * self._width
        row[self._key_at] = order_id
        self._offer(tuple(row) + (True,))

    def _end_snapshot(self) -> None:
        seen, self._snapshot = self._snapshot, None
        self.snapshots += 1
        if seen is None:
            return
        # A re-snapshot after a reconnect: whatever it no longer holds was deleted while
        # the connection was down, and that OOF is never coming.
        for order_id in sorted(set(self._live) - seen):
            self._delete(order_id)
            self.resynced += 1

    def _offer(self, row: Tuple[Any, ...]) -> None:
        with self._cv:
            if len(self._rows) >= self._max:
                self._cv.wait_for(lambda: self._closed or len(self._rows) < self._max)
            if self._closed:
                return
            self._rows.append(row)

    # -- update-graph thread -------------------------------------------------------------

    def drain(self) -> List[Tuple[Any, ...]]:
        """Take every buffered row (oldest first), unblocking the receive thread."""
        with self._cv:
            rows, self._rows = self._rows, []
            self._cv.notify_all()
            return rows

    def close(self) -> None:
        """Refuse further rows and release a blocked receive thread."""
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    @property
    def pending(self) -> int:
        with self._cv:
            return len(self._rows)

    @property
    def live(self) -> int:
        """Orders currently in the table, as far as this feed has seen."""
        return len(self._live)

    def status(self) -> Dict[str, Any]:
        """Counters for ``otr_amps_status`` (connection columns are the source's)."""
        return {
            "Table": self.schema.name,
            "Topic": self.topic,
            "Snapshots": self.snapshots,
            "Received": self.received,
            "Upserts": self.upserts,
            "Deletes": self.deletes,
            "Resynced": self.resynced,
            "Rejected": self.rejected,
            "Published": self.published,
            "FailedBatches": self.failed_batches,
            "Pending": self.pending,
            "Live": self.live,
            "LastError": self.last_error,
        }


def _default_client_factory(settings: AmpsSettings) -> Any:
    """An ``AMPS.HAClient`` over ``settings.uris``.

    The HA client reconnects (and fails over across the URI list) by itself and re-issues
    every subscription after it logs on again; :class:`TopicFeed` handles the re-snapshot.
    """
    import AMPS

    client = AMPS.HAClient(settings.client_name)
    chooser = AMPS.DefaultServerChooser()
    for uri in settings.uris:
        chooser.add(uri)
    client.set_server_chooser(chooser)
    client.set_heartbeat(HEARTBEAT_SECONDS)
    return client


class AmpsOrderSource:
    """One AMPS client, one ``sow_and_subscribe`` per native table, one table each.

    Keep the instance reachable for as long as its tables are in use:
    :func:`start` parks it in a module-level list for that reason.
    """

    def __init__(
        self,
        settings: AmpsSettings,
        schemas: Optional[Iterable[NativeSchema]] = None,
        client_factory: Callable[[AmpsSettings], Any] = _default_client_factory,
        max_pending: int = DEFAULT_MAX_PENDING,
    ) -> None:
        self.settings = settings
        self.feeds: Dict[str, TopicFeed] = {
            schema.name: TopicFeed(schema, amps_codec.topic(schema, settings.topic_prefix), max_pending)
            for schema in (schemas if schemas is not None else NATIVE_SCHEMAS.values())
        }
        self._client_factory = client_factory
        self._client: Any = None
        self._sub_ids: List[str] = []
        self._ctx: Any = None
        self._publishers: List[Any] = []
        self.state = "NotStarted"
        self.connected = False
        self.blinks: Dict[str, Any] = {}
        self.tables: Dict[str, Any] = {}
        self.status_table: Any = None

    # -- tables ---------------------------------------------------------------------------

    def _build_tables(self) -> None:
        from deephaven import dtypes as dht
        from deephaven.execution_context import get_exec_ctx
        from deephaven.stream.table_publisher import table_publisher

        kinds = {LONG: dht.int64, DOUBLE: dht.double}
        self._ctx = get_exec_ctx()
        for name, feed in self.feeds.items():
            col_defs = {column: kinds.get(kind, dht.string) for column, kind in feed.schema.columns}
            col_defs[DELETED_COLUMN] = dht.bool_
            blink, publisher = table_publisher(
                f"{name}_amps",
                col_defs,
                on_flush_callback=self._flusher(feed),
                on_shutdown_callback=self.stop,
            )
            self._publishers.append(publisher)
            self.blinks[name] = blink
            self.tables[name] = (
                blink.last_by([feed.schema.key]).where(f"!{DELETED_COLUMN}").drop_columns([DELETED_COLUMN])
            )

        status_defs = {
            column: (dht.bool_ if column == "Connected" else dht.string if column in ("Table", "Topic", "State", "LastError")
                     else dht.int64)
            for column in STATUS_COLUMNS
        }
        status_blink, status_publisher = table_publisher(
            "otr_amps_status", status_defs, on_flush_callback=self._flush_status
        )
        self._publishers.append(status_publisher)
        self.status_table = status_blink.last_by(["Table"])

    def _flusher(self, feed: TopicFeed) -> Callable[[Any], None]:
        def on_flush(publisher: Any) -> None:
            # Runs on the update-graph thread at the start of every cycle: one drain, one
            # add, and never an exception (it would stall the cycle).
            try:
                rows = feed.drain()
                if not rows:
                    return
                with self._ctx:
                    publisher.add(_batch(feed.schema, rows))
                feed.published += len(rows)
            except Exception:  # noqa: BLE001
                feed.failed_batches += 1
                traceback.print_exc()

        return on_flush

    def _flush_status(self, publisher: Any) -> None:
        try:
            rows = []
            for feed in self.feeds.values():
                row = feed.status()
                row["Connected"] = self.connected
                row["State"] = self.state
                rows.append(row)
            with self._ctx:
                publisher.add(_status_table(rows))
        except Exception:  # noqa: BLE001 - a status row must never stall the cycle
            traceback.print_exc()

    # -- lifecycle ------------------------------------------------------------------------

    def start(self) -> Dict[str, Any]:
        """Build the tables, connect, subscribe every topic; returns ``{table name: table}``.

        Raises:
            RuntimeError: when called twice.
            Exception: whatever the AMPS client raises on connect/logon/subscribe (the
                client is closed first).
        """
        if self._client is not None:
            raise RuntimeError("AmpsOrderSource.start() called twice")
        if not self.tables:
            self._build_tables()
        client = self._client_factory(self.settings)
        self._client = client
        try:
            client.add_connection_state_listener(self._on_state)
            client.connect_and_logon()
            self.connected = True
            self.state = "LoggedOn"
            for feed in self.feeds.values():
                self._sub_ids.append(
                    client.sow_and_subscribe(_handler(feed), feed.topic, None, SOW_BATCH_SIZE, True)
                )
        except Exception:
            self._client = None
            self.connected = False
            _safe_close(client)
            raise
        print(f"[order-tree] AMPS subscribed: {self.settings.describe()}", flush=True)
        return self.tables

    def stop(self) -> None:
        """Unsubscribe and close the client (best effort; never raises)."""
        client, self._client = self._client, None
        sub_ids, self._sub_ids = self._sub_ids, []
        for feed in self.feeds.values():
            feed.close()
        self.connected = False
        if client is None:
            return
        for sub_id in sub_ids:
            try:
                client.unsubscribe(sub_id)
            except Exception:  # noqa: BLE001 - shutdown must never raise
                pass
        _safe_close(client)

    def _on_state(self, connected: Any) -> None:
        # The python client calls this with True on connect and False on disconnect (it
        # fires before logon and resubscription, so "Connected" can precede the re-snapshot).
        connected = bool(connected)
        if connected == self.connected and self.state != "NotStarted":
            return
        self.connected = connected
        self.state = "Connected" if connected else "Disconnected"
        print(f"[order-tree] AMPS {self.state.lower()}: {self.settings.describe()}", flush=True)


def _handler(feed: TopicFeed) -> Callable[[Any], None]:
    def on_message(message: Any) -> None:
        try:
            feed.handle(message.get_command(), message.get_data(), message.get_sow_key())
        except Exception:  # noqa: BLE001 - a raising handler would kill the subscription
            traceback.print_exc()

    return on_message


def _safe_close(client: Any) -> None:
    try:
        client.close()
    except Exception:  # noqa: BLE001 - shutdown must never raise
        traceback.print_exc()


def _batch(schema: NativeSchema, rows: List[Tuple[Any, ...]]) -> Any:
    """Buffered rows as one static table: the schema's columns plus ``Deleted``."""
    from deephaven import new_table
    from deephaven.column import bool_col, double_col, long_col, string_col
    from deephaven.constants import NULL_DOUBLE, NULL_LONG

    columns = []
    for i, (name, kind) in enumerate(schema.columns):
        values = [row[i] for row in rows]
        if kind == LONG:
            columns.append(long_col(name, [NULL_LONG if v is None else v for v in values]))
        elif kind == DOUBLE:
            columns.append(double_col(name, [NULL_DOUBLE if v is None else v for v in values]))
        else:
            columns.append(string_col(name, values))
    columns.append(bool_col(DELETED_COLUMN, [bool(row[-1]) for row in rows]))
    return new_table(columns)


def _status_table(rows: List[Dict[str, Any]]) -> Any:
    from deephaven import new_table
    from deephaven.column import bool_col, long_col, string_col

    columns = []
    for name in STATUS_COLUMNS:
        values = [row[name] for row in rows]
        if name == "Connected":
            columns.append(bool_col(name, values))
        elif name in ("Table", "Topic", "State", "LastError"):
            columns.append(string_col(name, [str(v) for v in values]))
        else:
            columns.append(long_col(name, [int(v) for v in values]))
    return new_table(columns)


#: Strong references to every started source: one that is garbage collected takes its
#: AMPS client, and the feed, with it.
_ACTIVE: List[AmpsOrderSource] = []


def start(settings: AmpsSettings) -> AmpsOrderSource:
    """Start an :class:`AmpsOrderSource` and keep it alive for the life of the server.

    Idempotent per settings: a second call (the app script re-run in the console after
    a failed build, say) returns the running source. Two HA clients under one name would
    displace each other's logon and reconnect forever, so a source with the same client
    name but other settings is stopped first.
    """
    for running in list(_ACTIVE):
        if running.settings == settings:
            print(f"[order-tree] AMPS source already running: {settings.describe()}", flush=True)
            return running
        if running.settings.client_name == settings.client_name:
            running.stop()
            _ACTIVE.remove(running)
    source = AmpsOrderSource(settings)
    source.start()
    _ACTIVE.append(source)
    return source


def active_sources() -> List[AmpsOrderSource]:
    return list(_ACTIVE)
