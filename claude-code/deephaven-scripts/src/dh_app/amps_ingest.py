"""AMPS transaction-log ingestion: the AMPS half of the ``fix_raw`` blink source.

The Kafka source (:mod:`dh_app.ingest`) replays ``fix42.messages`` from offset 0 on
every start so a Deephaven restart rebuilds the identical cache (doc 03 section 3.3).
This module is the same contract against an AMPS transaction log: a
``bookmark_subscribe`` from the ``EPOCH`` bookmark replays the whole journal and then
cuts over to live messages on the same subscription.

Shape of the bridge::

    AMPS client thread          RawBuffer          update-graph thread
    ------------------          ---------          -------------------
    on_message(msg)  --offer--> [rows]  --drain--> on_flush -> publisher.add(batch)

``TablePublisher``'s ``on_flush_callback`` fires once at the start of each update
graph cycle, which is exactly the batching hook this needs: the AMPS reader thread
only appends to a list, and one table per cycle is built on the update-graph thread.
The resulting table is a blink table with the same retention story as the Kafka one
(doc 02 section 2) -- rows are consumed by the state-machine listener and dropped.

Import discipline
-----------------
Nothing at module scope imports ``deephaven`` or ``AMPS``; both are pulled in lazily
inside the functions that need them.  That is load-bearing twice over:

1. ``dh_app.ingest`` imports this module unconditionally, and a Kafka deployment must
   not need the AMPS client installed.  ``amps-python-client`` is not in the Deephaven
   image, so on the Kafka path the import would simply fail (see
   ``docs/03-deephaven-dag.md`` section 2.1).
2. It keeps the configuration and buffering logic unit-testable on a bare python,
   like :mod:`dh_app.schemas` -- see ``tests/test_ingest_source.py``.
"""

from __future__ import annotations

import os
import threading
import traceback
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

__all__ = [
    "URI_ENV",
    "TOPIC_ENV",
    "TOPIC_FALLBACK_ENV",
    "FILTER_ENV",
    "CLIENT_NAME_ENV",
    "BOOKMARK_ENV",
    "MAX_PENDING_ENV",
    "DEFAULT_URI",
    "DEFAULT_TOPIC",
    "DEFAULT_CLIENT_NAME",
    "DEFAULT_BOOKMARK",
    "DEFAULT_MAX_PENDING",
    "BOOKMARK_ALIASES",
    "COLUMN_NAMES",
    "RAW_COLUMN",
    "AmpsConfig",
    "RawBuffer",
    "AmpsRawSource",
    "resolve_bookmark",
    "build_amps_fix_raw",
    "active_source",
]

# --------------------------------------------------------------------------------------
# Configuration (doc 05 section 4 -- every knob is a FIX42_* environment variable)
# --------------------------------------------------------------------------------------

#: AMPS URI, or several comma/whitespace separated for an HA pair.
URI_ENV = "FIX42_AMPS_URI"
#: AMPS topic to replay.
TOPIC_ENV = "FIX42_AMPS_TOPIC"
#: Falls back to the shared topic name when :data:`TOPIC_ENV` is unset.
TOPIC_FALLBACK_ENV = "FIX42_TOPIC"
#: Optional AMPS content filter applied server-side.
FILTER_ENV = "FIX42_AMPS_FILTER"
#: Client name AMPS sees; the analogue of the Kafka consumer group id.
CLIENT_NAME_ENV = "FIX42_AMPS_CLIENT_NAME"
#: Replay position: ``epoch`` (the default), ``now``, ``most_recent``, or a literal bookmark.
BOOKMARK_ENV = "FIX42_AMPS_BOOKMARK"
#: Upper bound on messages buffered between two update graph cycles.
MAX_PENDING_ENV = "FIX42_AMPS_MAX_PENDING"

#: Compose-network default, mirroring ``ingest.DEFAULT_BOOTSTRAP``.
DEFAULT_URI = "tcp://amps:9007/amps/fix"
#: Mirrors ``ingest.DEFAULT_TOPIC``; duplicated rather than imported to keep this
#: module free of a ``dh_app.ingest`` import (``ingest`` imports *this*).
#: ``tests/test_ingest_source.py`` pins the two together.
DEFAULT_TOPIC = "fix42.messages"
#: Same identity the Kafka path uses as its consumer group id.
DEFAULT_CLIENT_NAME = "dh-fix42-dashboard"
#: Replay the whole transaction log -- the AMPS analogue of seek-to-beginning.
DEFAULT_BOOKMARK = "epoch"
#: Enough for several seconds of a fast feed; see :class:`RawBuffer` on overflow.
DEFAULT_MAX_PENDING = 250_000

#: Friendly bookmark names -> the attribute to read off ``AMPS.Client.Bookmarks``.
#: Anything not listed here is passed to AMPS verbatim as a literal bookmark.
BOOKMARK_ALIASES: Dict[str, str] = {
    "epoch": "EPOCH",
    "beginning": "EPOCH",
    "now": "NOW",
    "most_recent": "MOST_RECENT",
    "recent": "MOST_RECENT",
}

#: The column the state machine folds; the only one :class:`dh_app.pipeline.Pipeline` reads.
RAW_COLUMN = "RawFix"
#: Columns of the AMPS ``fix_raw`` blink table.  ``AmpsBookmark`` is the transaction-log
#: position -- the AMPS analogue of ``KafkaOffset``, and the evidence for replay order.
COLUMN_NAMES: Tuple[str, ...] = (RAW_COLUMN, "AmpsBookmark", "IngestTs")


def resolve_bookmark(value: str, bookmarks: Any) -> str:
    """Resolve a configured bookmark name to the string AMPS expects.

    Args:
        value: A :data:`BOOKMARK_ALIASES` key (case/underscore insensitive) or a
            literal AMPS bookmark such as ``"3|1|"``.
        bookmarks: The ``AMPS.Client.Bookmarks`` class (injected so this stays
            testable without the AMPS client installed).

    Returns:
        The bookmark string to hand to ``bookmark_subscribe``.
    """
    key = (value or "").strip().lower().replace("-", "_")
    attribute = BOOKMARK_ALIASES.get(key)
    if attribute is None:
        return value
    return getattr(bookmarks, attribute)


class AmpsConfig:
    """Everything :class:`AmpsRawSource` needs, read from the environment."""

    def __init__(
        self,
        uris: Sequence[str],
        topic: str,
        filter: Optional[str] = None,
        client_name: str = DEFAULT_CLIENT_NAME,
        bookmark: str = DEFAULT_BOOKMARK,
        max_pending: int = DEFAULT_MAX_PENDING,
    ) -> None:
        """Store the resolved settings."""
        self.uris: Tuple[str, ...] = tuple(uris)
        self.topic = topic
        self.filter = filter or None
        self.client_name = client_name
        self.bookmark = bookmark
        self.max_pending = max_pending

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "AmpsConfig":
        """Build a config from ``env`` (defaults to :data:`os.environ`).

        The topic falls back to :data:`TOPIC_FALLBACK_ENV` so a deployment that
        already names its topic once does not have to name it twice.
        """
        source = os.environ if env is None else env
        uris = _split_uris(source.get(URI_ENV, "")) or (DEFAULT_URI,)
        topic = (
            source.get(TOPIC_ENV)
            or source.get(TOPIC_FALLBACK_ENV)
            or DEFAULT_TOPIC
        )
        return cls(
            uris=uris,
            topic=topic,
            filter=source.get(FILTER_ENV),
            client_name=source.get(CLIENT_NAME_ENV) or DEFAULT_CLIENT_NAME,
            bookmark=source.get(BOOKMARK_ENV) or DEFAULT_BOOKMARK,
            max_pending=_positive_int(source.get(MAX_PENDING_ENV), DEFAULT_MAX_PENDING),
        )

    def describe(self) -> str:
        """One-line summary for the startup banner."""
        parts = [f"{','.join(self.uris)} topic={self.topic} bookmark={self.bookmark}"]
        if self.filter:
            parts.append(f"filter={self.filter!r}")
        return " ".join(parts)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"AmpsConfig({self.describe()})"


def _split_uris(value: str) -> Tuple[str, ...]:
    """Split a comma/whitespace separated URI list, dropping empties."""
    return tuple(part for part in value.replace(",", " ").split() if part)


def _positive_int(value: Optional[str], default: int) -> int:
    """Parse a positive int, falling back to ``default`` on anything unusable."""
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


# --------------------------------------------------------------------------------------
# Thread hand-off
# --------------------------------------------------------------------------------------


class RawBuffer:
    """Bounded hand-off from the AMPS client thread to the update-graph thread.

    On overflow :meth:`offer` **blocks** rather than dropping.  A dropped FIX message
    would silently corrupt the fold -- the state machine would be missing a link in an
    amend chain with nothing to detect it -- while blocking the AMPS reader thread is
    ordinary TCP backpressure, which the server is built to absorb and which the HA
    client recovers from by resuming at its last bookmark.
    """

    def __init__(self, max_pending: int = DEFAULT_MAX_PENDING) -> None:
        """Create an empty buffer holding at most ``max_pending`` rows."""
        self._max = max(1, int(max_pending))
        self._cv = threading.Condition()
        self._rows: List[Tuple[Any, ...]] = []
        self._closed = False
        self.offered = 0
        self.dropped = 0
        self.waits = 0

    def offer(self, row: Tuple[Any, ...], timeout: Optional[float] = None) -> bool:
        """Append ``row``, waiting while the buffer is full.

        Args:
            row: The tuple to buffer, positionally matching :data:`COLUMN_NAMES`.
            timeout: Seconds to wait for space; ``None`` waits until drained or closed.

        Returns:
            True if buffered; False if the buffer was closed or the wait timed out
            (both counted in :attr:`dropped`).
        """
        with self._cv:
            if self._closed:
                self.dropped += 1
                return False
            if len(self._rows) >= self._max:
                self.waits += 1
                self._cv.wait_for(
                    lambda: self._closed or len(self._rows) < self._max, timeout
                )
                if self._closed or len(self._rows) >= self._max:
                    self.dropped += 1
                    return False
            self._rows.append(row)
            self.offered += 1
            return True

    def drain(self) -> List[Tuple[Any, ...]]:
        """Take every buffered row, freeing space for blocked producers."""
        with self._cv:
            if not self._rows:
                return []
            rows, self._rows = self._rows, []
            self._cv.notify_all()
            return rows

    def close(self) -> None:
        """Refuse further rows and release anything blocked in :meth:`offer`."""
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    @property
    def pending(self) -> int:
        """Rows buffered but not yet published."""
        with self._cv:
            return len(self._rows)

    @property
    def closed(self) -> bool:
        """True once :meth:`close` has been called."""
        with self._cv:
            return self._closed


# --------------------------------------------------------------------------------------
# Lazy deephaven access
# --------------------------------------------------------------------------------------

_DH: Optional[Dict[str, Any]] = None


def _deephaven() -> Dict[str, Any]:
    """Import and memoize the deephaven symbols this module needs.

    Deliberately not a module-scope import: see the module docstring.
    """
    global _DH
    if _DH is not None:
        return _DH

    from deephaven import dtypes as dht
    from deephaven import new_table
    from deephaven.column import string_col
    from deephaven.execution_context import get_exec_ctx
    from deephaven.stream.table_publisher import table_publisher

    try:  # server 42.x exports the Instant column factory as datetime_col
        from deephaven.column import datetime_col as instant_col
    except ImportError:  # pragma: no cover - other versions export instant_col directly
        from deephaven.column import instant_col

    try:  # doc 04 flags this helper's location as version-drifting
        from deephaven.time import to_j_instant
    except ImportError:  # pragma: no cover - older servers exposed it as to_datetime
        from deephaven.time import to_datetime as to_j_instant

    _DH = {
        "dht": dht,
        "new_table": new_table,
        "string_col": string_col,
        "instant_col": instant_col,
        "to_j_instant": to_j_instant,
        "get_exec_ctx": get_exec_ctx,
        "table_publisher": table_publisher,
    }
    return _DH


def _utcnow() -> datetime:
    """Current time as a tz-aware UTC datetime."""
    return datetime.now(timezone.utc)


def _default_client_factory(config: AmpsConfig) -> Any:
    """Build an ``AMPS.HAClient`` wired to ``config``'s server list.

    ``HAClient`` supplies a memory-backed bookmark store by default, which is what
    makes :meth:`AmpsRawSource.start`'s ``bookmark_subscribe`` replay from ``EPOCH``
    on a cold start and resume at the last bookmark after a mid-life disconnect.
    """
    import AMPS

    client = AMPS.HAClient(config.client_name)
    chooser = AMPS.DefaultServerChooser()
    for uri in config.uris:
        chooser.add(uri)
    client.set_server_chooser(chooser)
    return client


# --------------------------------------------------------------------------------------
# The source
# --------------------------------------------------------------------------------------


class AmpsRawSource:
    """One AMPS bookmark subscription feeding one ``fix_raw`` blink table.

    The instance owns the AMPS client and must stay reachable for as long as the table
    is live; :func:`build_amps_fix_raw` parks it in :data:`_ACTIVE` for that reason.
    """

    def __init__(
        self,
        config: Optional[AmpsConfig] = None,
        client_factory: Callable[[AmpsConfig], Any] = _default_client_factory,
        now_fn: Callable[[], datetime] = _utcnow,
    ) -> None:
        """Create an unstarted source.

        Args:
            config: Settings; defaults to :meth:`AmpsConfig.from_env`.
            client_factory: Builds the AMPS client (injected for tests).
            now_fn: Ingest clock (injected for tests).
        """
        self.config = config or AmpsConfig.from_env()
        self.buffer = RawBuffer(self.config.max_pending)
        self._client_factory = client_factory
        self._now = now_fn
        self._client: Any = None
        self._sub_id: Optional[str] = None
        self._ctx: Any = None
        self._publisher: Any = None
        self.table: Any = None
        self.published = 0
        self.failed_batches = 0

    # -- lifecycle ---------------------------------------------------------------

    def build_table(self) -> Any:
        """Create the blink table and its publisher (no AMPS connection yet)."""
        dh = _deephaven()
        dht = dh["dht"]
        col_defs = {
            RAW_COLUMN: dht.string,
            "AmpsBookmark": dht.string,
            "IngestTs": dht.Instant,
        }
        # Captured on the setup thread and re-entered inside the flush callback, which
        # runs on the update-graph thread and builds tables there (doc 04 section 1).
        self._ctx = dh["get_exec_ctx"]()
        self.table, self._publisher = dh["table_publisher"](
            "fix_raw_amps",
            col_defs,
            on_flush_callback=self._on_flush,
            on_shutdown_callback=self.stop,
        )
        return self.table

    def start(self) -> Any:
        """Connect, subscribe from the configured bookmark, and return the table.

        Raises:
            RuntimeError: If the source was already started.
        """
        if self._client is not None:
            raise RuntimeError("AmpsRawSource.start() called twice")
        if self.table is None:
            self.build_table()

        import AMPS

        client = self._client_factory(self.config)
        self._client = client
        try:
            client.add_connection_state_listener(self._on_connection_state)
            client.connect_and_logon()
            self._sub_id = client.bookmark_subscribe(
                self._on_message,
                self.config.topic,
                resolve_bookmark(self.config.bookmark, AMPS.Client.Bookmarks),
                self.config.filter,
            )
        except Exception:
            self._client = None
            self._safe_close(client)
            raise
        print(f"[fix42] AMPS subscribed: {self.config.describe()}")
        return self.table

    def stop(self) -> None:
        """Unsubscribe and close the client (best effort; never raises)."""
        client, self._client = self._client, None
        sub_id, self._sub_id = self._sub_id, None
        self.buffer.close()
        if client is None:
            return
        try:
            if sub_id is not None:
                client.unsubscribe(sub_id)
        except Exception:  # noqa: BLE001 - shutdown must never raise
            traceback.print_exc()
        self._safe_close(client)

    @staticmethod
    def _safe_close(client: Any) -> None:
        """Close ``client``, swallowing anything it throws."""
        try:
            client.close()
        except Exception:  # noqa: BLE001 - shutdown must never raise
            traceback.print_exc()

    # -- callbacks ---------------------------------------------------------------

    def _on_message(self, message: Any) -> None:
        """AMPS client thread: buffer one message's payload.

        Discards the bookmark once buffered.  Losing the buffer means the process
        died, and a fresh process starts from an empty memory bookmark store and
        replays from ``EPOCH`` anyway, so nothing is lost by discarding here.
        """
        try:
            raw = message.get_data()
            if not raw:
                return
            self.buffer.offer((raw, message.get_bookmark() or "", self._now()))
        except Exception:  # noqa: BLE001 - a handler exception would kill the subscription
            traceback.print_exc()
        finally:
            self._discard(message)

    def _discard(self, message: Any) -> None:
        """Release ``message`` from the local bookmark store."""
        client = self._client
        if client is None:
            return
        try:
            client.discard(message)
        except Exception:  # noqa: BLE001 - never fail the reader thread on bookkeeping
            traceback.print_exc()

    def _on_connection_state(self, state: Any) -> None:
        """Log AMPS connection transitions (visible in the Deephaven server log)."""
        print(f"[fix42] AMPS connection state: {state}")

    def _on_flush(self, publisher: Any) -> None:
        """Update-graph thread: publish everything buffered since the last cycle.

        This blocks the update cycle, so it does exactly one drain and one
        ``publisher.add`` and never raises.
        """
        try:
            rows = self.buffer.drain()
            if not rows:
                return
            with self._ctx:
                publisher.add(self._build_batch(rows))
            self.published += len(rows)
        except Exception:  # noqa: BLE001 - a flush exception would stall the update graph
            self.failed_batches += 1
            traceback.print_exc()

    def _build_batch(self, rows: Sequence[Tuple[Any, ...]]) -> Any:
        """Turn buffered tuples into one static table matching :data:`COLUMN_NAMES`."""
        dh = _deephaven()
        to_instant = dh["to_j_instant"]
        return dh["new_table"](
            [
                dh["string_col"](RAW_COLUMN, [str(row[0]) for row in rows]),
                dh["string_col"]("AmpsBookmark", [str(row[1]) for row in rows]),
                dh["instant_col"]("IngestTs", [to_instant(row[2]) for row in rows]),
            ]
        )


# Strong references to every live source: an ``AmpsRawSource`` that is garbage
# collected takes its AMPS client -- and the stream -- with it.  ``sys.modules`` keeps
# this module, and therefore the list, alive for the life of the server.
_ACTIVE: List[AmpsRawSource] = []


def build_amps_fix_raw(config: Optional[AmpsConfig] = None) -> Any:
    """Build the ``fix_raw`` blink table backed by an AMPS transaction-log replay.

    Args:
        config: Settings; defaults to :meth:`AmpsConfig.from_env`.

    Returns:
        A blink :class:`~deephaven.table.Table` with ``RawFix`` (the SOH-delimited
        FIX 4.2 message), ``AmpsBookmark`` (transaction-log position) and ``IngestTs``.
    """
    source = AmpsRawSource(config=config)
    table = source.start()
    _ACTIVE.append(source)
    return table


def active_source() -> Optional[AmpsRawSource]:
    """The most recently built source, or ``None`` if the AMPS path is not in use."""
    return _ACTIVE[-1] if _ACTIVE else None
