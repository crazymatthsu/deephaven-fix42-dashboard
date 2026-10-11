"""AMPS SOW topics -> live FIX order-state event tables (doc 14 section 12).

One AMPS client (an ``HAClient`` over every URI in ``OTR_AMPS_URI``) runs one
``sow_and_subscribe`` per SOW topic of every system, with ``oof`` and ``send_keys``. Each
system gets one blink event table (:data:`order_tree_recon.fixstate.EVENT_COLUMNS`) that
:func:`order_tree_recon.fixstate_dag.fix_level_tables` turns into the SOW mirror, the
latest order states and the order tree.

=====================================  ==============================================
AMPS message                           event
=====================================  ==============================================
``group_begin``                        the topic's ``Epoch`` += 1 (a new snapshot starts)
``sow``, ``publish``, ``delta_publish``  ``UPSERT`` (the record's new state)
``oof``, ``sow_delete``                ``DELETE`` (the record left the SOW)
``group_end``                          ``SNAPSHOT_END`` (the snapshot is complete)
``ack``, ``heartbeat``, ...            ignored
=====================================  ==============================================

After a disconnect the HA client logs on again and re-issues every ``sow_and_subscribe``; the
new snapshot (a new epoch) re-sends every live record, and at its ``group_end`` the mirror
drops whatever was not re-sent -- records deleted while disconnected, for which no ``oof``
could arrive.

The client asks for **heartbeats** (``OTR_AMPS_HEARTBEAT``, 5 s): without them a half-open
connection (a dropped firewall session, a frozen peer) stalls the feed silently, with no
disconnect for the HA client to act on. With them both sides drop it within a couple of
intervals and the re-snapshot above repairs the book. The integration test proves it by
pausing the Deephaven container past the timeout (``e2e/``, doc 14 section 12.7).

The bridge is the one ``dh_app/amps_ingest.py`` uses for the FIX pipeline::

    AMPS receive thread             EventBuffer (per system)     update-graph thread
    -------------------             ------------------------     -------------------
    handler(message) ---offer--->   [rows]       ---drain--->    on_flush -> publisher.add

Nothing at module scope imports ``deephaven`` or ``AMPS`` (``pip install
amps-python-client``; it is not in the Deephaven image), so the settings, the message
classification and the buffering are unit-tested on a bare python with a stub client.
"""

from __future__ import annotations

import os
import re
import threading
import traceback
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from order_tree_recon.fixstate import DELETE, SNAPSHOT_END, UPSERT, FixStateConfig

__all__ = [
    "URI_ENV",
    "CLIENT_NAME_ENV",
    "BATCH_SIZE_ENV",
    "TIMEOUT_ENV",
    "MAX_PENDING_ENV",
    "HEARTBEAT_ENV",
    "SUBSCRIBE_OPTIONS",
    "COMMAND_ACTIONS",
    "AmpsSettings",
    "EventBuffer",
    "AmpsSowSource",
    "start_source",
]

URI_ENV = "OTR_AMPS_URI"
CLIENT_NAME_ENV = "OTR_AMPS_CLIENT_NAME"
BATCH_SIZE_ENV = "OTR_AMPS_BATCH_SIZE"
TIMEOUT_ENV = "OTR_AMPS_TIMEOUT_MS"
MAX_PENDING_ENV = "OTR_AMPS_MAX_PENDING"
HEARTBEAT_ENV = "OTR_AMPS_HEARTBEAT"

DEFAULT_CLIENT_NAME = "dh-order-tree"
DEFAULT_BATCH_SIZE = 1000
DEFAULT_TIMEOUT_MS = 10_000
DEFAULT_MAX_PENDING = 250_000
#: Seconds between AMPS heartbeats; 0 turns them off.
DEFAULT_HEARTBEAT = 5
#: ``oof``: deletes / expiries arrive as ``oof``; ``send_keys``: publishes carry the SowKey.
SUBSCRIBE_OPTIONS = "oof,send_keys"

#: AMPS command -> event action (``group_begin`` is handled separately: it starts an epoch).
COMMAND_ACTIONS: Dict[str, str] = {
    "sow": UPSERT,
    "publish": UPSERT,
    "delta_publish": UPSERT,
    "oof": DELETE,
    "sow_delete": DELETE,
    "group_end": SNAPSHOT_END,
}

_CREDENTIALS = re.compile(r"(://[^:/@]+):[^@/]+@")


def _int_env(environ: Mapping[str, str], name: str, default: int, minimum: int) -> int:
    raw = environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name}={raw!r}: expected an integer") from None
    if value < minimum:
        raise ValueError(f"{name}={raw!r}: expected an integer >= {minimum}")
    return value


class AmpsSettings:
    """Connection settings, from ``OTR_AMPS_*`` (the topics come from the mapping config)."""

    def __init__(self, uris: Sequence[str], client_name: str = DEFAULT_CLIENT_NAME,
                 batch_size: int = DEFAULT_BATCH_SIZE, timeout_ms: int = DEFAULT_TIMEOUT_MS,
                 max_pending: int = DEFAULT_MAX_PENDING, heartbeat: int = DEFAULT_HEARTBEAT) -> None:
        if not uris:
            raise ValueError(f"{URI_ENV}: at least one AMPS URI is required, e.g. tcp://amps-host:9007/amps/fix")
        self.uris = tuple(uris)
        self.client_name = client_name
        self.batch_size = batch_size
        self.timeout_ms = timeout_ms
        self.max_pending = max_pending
        self.heartbeat = heartbeat

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AmpsSettings) and vars(self) == vars(other)

    __hash__ = None  # type: ignore[assignment]  # mutable attributes; equality only

    @classmethod
    def from_env(cls, environ: Optional[Mapping[str, str]] = None) -> "AmpsSettings":
        env = os.environ if environ is None else environ
        uris = [u for u in re.split(r"[\s,]+", env.get(URI_ENV, "").strip()) if u]
        return cls(
            uris,
            env.get(CLIENT_NAME_ENV, "").strip() or DEFAULT_CLIENT_NAME,
            _int_env(env, BATCH_SIZE_ENV, DEFAULT_BATCH_SIZE, 1),
            _int_env(env, TIMEOUT_ENV, DEFAULT_TIMEOUT_MS, 0),
            _int_env(env, MAX_PENDING_ENV, DEFAULT_MAX_PENDING, 1),
            _int_env(env, HEARTBEAT_ENV, DEFAULT_HEARTBEAT, 0),
        )

    def describe(self) -> str:
        """For the log: the URIs with any password masked."""
        uris = ", ".join(_CREDENTIALS.sub(r"\1:****@", u) for u in self.uris)
        heartbeat = f"heartbeat {self.heartbeat} s" if self.heartbeat else "no heartbeat"
        return f"{uris} as {self.client_name} (batch {self.batch_size}, timeout {self.timeout_ms} ms, {heartbeat})"


class EventBuffer:
    """Bounded hand-off from the AMPS receive thread to the update-graph thread.

    :meth:`offer` **blocks** when full rather than dropping: a lost ``DELETE`` would leave a
    phantom open order, and blocking the receive thread is ordinary TCP backpressure.
    """

    def __init__(self, max_pending: int = DEFAULT_MAX_PENDING) -> None:
        self._max = max(1, int(max_pending))
        self._cv = threading.Condition()
        self._rows: List[Tuple[Any, ...]] = []
        self._closed = False
        self.offered = 0
        self.dropped = 0

    def offer(self, row: Tuple[Any, ...], timeout: Optional[float] = None) -> bool:
        with self._cv:
            if not self._closed and len(self._rows) >= self._max:
                self._cv.wait_for(lambda: self._closed or len(self._rows) < self._max, timeout)
            if self._closed or len(self._rows) >= self._max:
                self.dropped += 1
                return False
            self._rows.append(row)
            self.offered += 1
            return True

    def drain(self) -> List[Tuple[Any, ...]]:
        with self._cv:
            rows, self._rows = self._rows, []
            if rows:
                self._cv.notify_all()
            return rows

    def close(self) -> None:
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    @property
    def pending(self) -> int:
        with self._cv:
            return len(self._rows)


def _default_client_factory(settings: AmpsSettings) -> Any:
    """An ``AMPS.HAClient`` over every configured URI (it reconnects and resubscribes)."""
    import AMPS

    client = AMPS.HAClient(settings.client_name)
    chooser = AMPS.DefaultServerChooser()
    for uri in settings.uris:
        chooser.add(uri)
    client.set_server_chooser(chooser)
    if settings.heartbeat:
        client.set_heartbeat(settings.heartbeat)
    return client


def _default_command_factory(topic: str, filter_: Optional[str], settings: AmpsSettings) -> Any:
    """The ``sow_and_subscribe`` command for one topic."""
    import AMPS

    command = AMPS.Command("sow_and_subscribe").set_topic(topic).set_options(SUBSCRIBE_OPTIONS)
    command = command.set_batch_size(settings.batch_size).set_timeout(settings.timeout_ms)
    if filter_:
        command = command.set_filter(filter_)
    return command


class AmpsSowSource:
    """``sow_and_subscribe`` on every topic of every system -> one blink event table per system.

    Keep the instance reachable while the tables are in use (:func:`start_source` does): it
    owns the AMPS client.
    """

    def __init__(
        self,
        config: FixStateConfig,
        settings: AmpsSettings,
        client_factory: Callable[[AmpsSettings], Any] = _default_client_factory,
        command_factory: Callable[[str, Optional[str], AmpsSettings], Any] = _default_command_factory,
        retry_seconds: float = 5.0,
    ) -> None:
        self.config = config
        self.settings = settings
        self._client_factory = client_factory
        self._command_factory = command_factory
        self._retry_seconds = retry_seconds
        self.buffers: Dict[str, EventBuffer] = {s.system: EventBuffer(settings.max_pending) for s in config.systems}
        self.tables: Dict[str, Any] = {}
        self._publishers: Dict[str, Any] = {}
        self._ctx: Any = None
        self._lock = threading.Lock()
        self._arrival: Dict[str, int] = {s.system: 0 for s in config.systems}
        self._epoch: Dict[Tuple[str, str], int] = {}
        #: Per ``(system, topic)``: message counts by action, snapshots completed, records
        #: in the last snapshot -- see :meth:`status`.
        self.stats: Dict[Tuple[str, str], Dict[str, int]] = {}
        self._client: Any = None
        self._sub_ids: List[Any] = []
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.connected = False
        self.last_error: Optional[str] = None

    # -- tables --------------------------------------------------------------------

    def build_tables(self) -> Dict[str, Any]:
        """One blink event table per system (no AMPS connection yet)."""
        from deephaven import dtypes as dht
        from deephaven.execution_context import get_exec_ctx
        from deephaven.stream.table_publisher import table_publisher

        col_defs = {"Topic": dht.string, "SowKey": dht.string, "Action": dht.string,
                    "Epoch": dht.int32, "Arrival": dht.int64, "RawFix": dht.string}
        self._ctx = get_exec_ctx()
        for system in self.config.systems:
            name = system.system
            table, publisher = table_publisher(
                f"amps_{name.lower()}_events", col_defs,
                on_flush_callback=self._flusher(name),
                on_shutdown_callback=self.stop,
            )
            self.tables[name] = table
            self._publishers[name] = publisher
        return self.tables

    def _flusher(self, system: str) -> Callable[[Any], None]:
        def on_flush(publisher: Any) -> None:
            """Update-graph thread: publish everything buffered since the last cycle; never raises."""
            try:
                rows = self.buffers[system].drain()
                if rows:
                    with self._ctx:
                        publisher.add(self._batch(rows))
            except Exception:  # noqa: BLE001 - a flush exception would stall the update graph
                traceback.print_exc()
        return on_flush

    @staticmethod
    def _batch(rows: Sequence[Tuple[Any, ...]]) -> Any:
        from deephaven import new_table
        from deephaven.column import int_col, long_col, string_col

        return new_table([
            string_col("Topic", [r[0] for r in rows]),
            string_col("SowKey", [r[1] for r in rows]),
            string_col("Action", [r[2] for r in rows]),
            int_col("Epoch", [r[3] for r in rows]),
            long_col("Arrival", [r[4] for r in rows]),
            string_col("RawFix", [r[5] for r in rows]),
        ])

    # -- AMPS ----------------------------------------------------------------------

    def subscriptions(self) -> List[Tuple[str, str, Optional[str]]]:
        """``(system, topic, filter)`` for every configured topic."""
        return [(s.system, topic, self.config.filters.get(s.system))
                for s in self.config.systems for topic in self.config.topics.get(s.system, ())]

    def handler(self, system: str, topic: str) -> Callable[[Any], None]:
        """The message handler for one topic (runs on the AMPS receive thread)."""
        key = (system, topic)
        self._epoch.setdefault(key, 0)
        stats = self.stats.setdefault(key, {UPSERT: 0, DELETE: 0, "snapshots": 0, "last_snapshot_records": 0})
        in_snapshot = {"records": 0}

        def on_message(message: Any) -> None:
            try:
                command = str(message.get_command() or "")
                if command == "group_begin":
                    with self._lock:
                        self._epoch[key] += 1
                    in_snapshot["records"] = 0
                    return
                action = COMMAND_ACTIONS.get(command)
                if action is None:
                    return
                if command == "sow":
                    in_snapshot["records"] += 1
                with self._lock:
                    self._arrival[system] += 1
                    row = (topic, str(message.get_sow_key() or ""), action, self._epoch[key],
                           self._arrival[system], "" if action == SNAPSHOT_END else str(message.get_data() or ""))
                self.buffers[system].offer(row)
                if action == SNAPSHOT_END:
                    stats["snapshots"] += 1
                    stats["last_snapshot_records"] = in_snapshot["records"]
                    print(f"[order-tree] AMPS {topic} ({system}): snapshot {self._epoch[key]} complete, "
                          f"{in_snapshot['records']} records", flush=True)
                else:
                    stats[action] += 1
            except Exception:  # noqa: BLE001 - a handler exception would kill the subscription
                traceback.print_exc()

        return on_message

    def connect(self) -> None:
        """Connect, log on and subscribe every topic (raises on failure)."""
        client = self._client_factory(self.settings)
        try:
            client.add_connection_state_listener(self._on_connection_state)
            client.connect_and_logon()
            for system, topic, filter_ in self.subscriptions():
                command = self._command_factory(topic, filter_, self.settings)
                self._sub_ids.append(client.execute_async(command, self.handler(system, topic)))
        except Exception:
            self._safe_close(client)
            raise
        self._client = client
        self.connected = True
        print(f"[order-tree] AMPS subscribed ({self.settings.describe()}): "
              + ", ".join(f"{t} -> {s}" for s, t, _ in self.subscriptions()), flush=True)

    def start(self, background: bool = True) -> Dict[str, Any]:
        """Build the tables and connect -- in a background thread by default, retrying until it
        succeeds, so the server comes up (with empty tables) even while AMPS is unreachable."""
        if not self.tables:
            self.build_tables()
        if not background:
            self.connect()
            return self.tables

        def run() -> None:
            while not self._stop.is_set():
                try:
                    self.connect()
                    return
                except Exception as exc:  # noqa: BLE001 - keep retrying; the reason is logged
                    self.last_error = f"{type(exc).__name__}: {exc}"
                    print(f"[order-tree] AMPS connect failed ({self.last_error}); retrying in "
                          f"{self._retry_seconds:g}s", flush=True)
                    self._stop.wait(self._retry_seconds)

        self._thread = threading.Thread(target=run, name="otr-amps-connect", daemon=True)
        self._thread.start()
        return self.tables

    def stop(self) -> None:
        """Unsubscribe and close (best effort; never raises)."""
        self._stop.set()
        client, self._client = self._client, None
        for buffer in self.buffers.values():
            buffer.close()
        if client is None:
            return
        for sub_id in self._sub_ids:
            try:
                client.unsubscribe(sub_id)
            except Exception:  # noqa: BLE001 - shutdown must never raise
                traceback.print_exc()
        self._sub_ids = []
        self._safe_close(client)
        self.connected = False

    def status(self) -> List[Dict[str, Any]]:
        """One row per topic: epoch, counts, snapshots completed (for the console)."""
        return [
            {"System": system, "Topic": topic, "Epoch": self._epoch.get((system, topic), 0), **stats}
            for (system, topic), stats in sorted(self.stats.items())
        ]

    @staticmethod
    def _safe_close(client: Any) -> None:
        try:
            client.close()
        except Exception:  # noqa: BLE001 - shutdown must never raise
            traceback.print_exc()

    def _on_connection_state(self, state: Any) -> None:
        # The python client passes True on connect and False on disconnect (before logon and
        # resubscription, so "connected" can precede the re-snapshot).
        connected = bool(state)
        if connected != self.connected:
            print(f"[order-tree] AMPS {'connected' if connected else 'disconnected'}: "
                  f"{self.settings.describe()}", flush=True)
        self.connected = connected


#: Strong references: a collected source takes its AMPS client -- and the streams -- with it.
_ACTIVE: List[AmpsSowSource] = []


def start_source(config: FixStateConfig, settings: AmpsSettings, background: bool = True, **kwargs: Any) -> AmpsSowSource:
    """Create, start and keep alive an :class:`AmpsSowSource`; its ``tables`` are ready at once.

    Idempotent: the app script re-run in a console (after a failed build, say) gets the
    running source back. Two HA clients under one client name would displace each other's
    logon and reconnect forever, so a running source with the same name but another
    mapping or other settings is stopped first.
    """
    for running in list(_ACTIVE):
        if running.settings.client_name != settings.client_name:
            continue
        if running.settings == settings and running.config == config:
            print(f"[order-tree] AMPS source already running: {settings.describe()}", flush=True)
            return running
        running.stop()
        _ACTIVE.remove(running)
    source = AmpsSowSource(config, settings, **kwargs)
    source.start(background=background)
    _ACTIVE.append(source)
    return source
