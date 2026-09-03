"""The three remote mechanisms -- doc 10 section 3.

All of it runs **inside** the collector server's python, against leaves using the
stack's anonymous auth. Nothing is installed: ``deephaven.uri`` and
``deephaven.barrage`` ship with server 42.4, and the Java client behind
``deephaven.barrage`` does the remote-call job without a second gRPC stack in the
JVM (``pydeephaven`` stays an e2e-only dependency).

=====================  ========  =========================================================
Mechanism              Direction  API
=====================  ========  =========================================================
Remote subscription    push       ``resolve("dh+plain://dh1:10000/scope/rx_orders")`` or
                                  ``barrage_session(host, port).subscribe(b"s/rx_orders")``
Remote snapshot        pull       ``barrage_session(...).snapshot(b"s/rx_leaf_stats")``
Remote query ("call")  pull       ``console("python").executeCode(...)`` on the leaf, then
                                  snapshot (static) or subscribe (live), then ``del``
=====================  ========  =========================================================

Lifecycle rules that are part of the contract:

* a **snapshot** query deletes its ``rx_q_<n>`` global on the leaf immediately;
* a **live** query leaves it there until :meth:`RemoteResolver.release_live`
  (which ``reconnect()`` calls) drops it -- otherwise the subscription's source
  would vanish under it;
* ``resolve()`` is not retried by Deephaven, so :meth:`RemoteResolver.wait_for_all`
  does the retrying: *healthy* (the gRPC probe compose uses) is not *exported*
  (Application Mode finished wiring).

Every jpy/Java call is wrapped: a failure becomes a :class:`ValueError` naming the
leaf, the export and the mechanism, because the raw Java exception says neither.
"""

from __future__ import annotations

import itertools
import threading
import time
import traceback
from typing import Any, Dict, List, Optional, Sequence, Tuple

from remote_uri.config import RESOLVER_BARRAGE, CollectorSettings, LeafRef
from remote_uri.uris import LEAF_EXPORTS, scope_ticket, scope_uri

__all__ = [
    "QUERY_GLOBAL_PREFIX",
    "RemoteError",
    "ResolveReport",
    "RemoteResolver",
]

#: Prefix of the transient globals a remote query binds on a leaf.
QUERY_GLOBAL_PREFIX = "rx_q_"

#: Process-wide counter behind ``rx_q_<n>``. Module scope on purpose: two collector
#: rebuilds must not hand out the same name while an older live query still holds it.
_QUERY_SEQUENCE = itertools.count(1)


class RemoteError(ValueError):
    """A remote mechanism failed.

    A ``ValueError`` subclass so callers that only catch ``ValueError`` (the repo's
    startup-error convention) still catch it, while the dashboard can tell a remote
    failure from a configuration one.
    """


class ResolveReport:
    """Outcome of one pass over every leaf.

    Attributes:
        resolved: ``{leaf name: {export: Table}}`` for leaves exposing **all four**
            exports. A partially-exported leaf is not usable: ``merge`` needs every
            leaf's ``rx_orders``, and half a fleet silently understates exposure.
        missing: One human-readable line per export that could not be resolved.
        elapsed: Seconds spent waiting.
        attempts: How many passes were made.
    """

    __slots__ = ("resolved", "missing", "elapsed", "attempts")

    def __init__(
        self,
        resolved: Dict[str, Dict[str, Any]],
        missing: Sequence[str],
        elapsed: float = 0.0,
        attempts: int = 0,
    ) -> None:
        """Store one pass's outcome."""
        self.resolved = resolved
        self.missing: Tuple[str, ...] = tuple(missing)
        self.elapsed = float(elapsed)
        self.attempts = int(attempts)

    @property
    def complete(self) -> bool:
        """True when every leaf exposed every export."""
        return not self.missing

    def describe(self) -> str:
        """One-line summary for the banner."""
        return (
            f"{len(self.resolved)} leaves resolved in {self.elapsed:.1f}s "
            f"({self.attempts} attempt(s)); {len(self.missing)} export(s) missing"
        )


class RemoteResolver:
    """Owns every remote connection the collector holds.

    One ``barrage_session`` per leaf, created lazily: in ``uri`` mode the standing
    subscriptions come from ``deephaven.uri.resolve`` (the server's
    ``BarrageTableResolver`` manages those connections itself) and the session is
    only needed for snapshots and remote calls; in ``barrage`` mode the same session
    also carries the subscriptions.

    The instance must stay reachable for as long as the tables it produced are live
    -- the collector runtime holds it.
    """

    def __init__(self, settings: CollectorSettings) -> None:
        """Create an unconnected resolver."""
        self._settings = settings
        self._lock = threading.RLock()
        self._sessions: Dict[str, Any] = {}
        self._consoles: Dict[str, Any] = {}
        #: ``[(leaf name, global name)]`` for live queries still bound on a leaf.
        self._live: List[Tuple[str, str]] = []

    # -- accessors ---------------------------------------------------------------

    @property
    def settings(self) -> CollectorSettings:
        """The collector configuration this resolver was built from."""
        return self._settings

    @property
    def live_queries(self) -> Tuple[Tuple[str, str], ...]:
        """``(leaf, global)`` pairs of live remote queries still bound on leaves."""
        with self._lock:
            return tuple(self._live)

    def _leaf(self, name: Any) -> LeafRef:
        """Look up a configured leaf, or fail with the list of known ones."""
        leaf = self._settings.leaf(name)
        if leaf is None:
            raise RemoteError(
                f"unknown leaf {name!r}; configured leaves are "
                f"{list(self._settings.leaf_names)}"
            )
        return leaf

    # -- sessions ----------------------------------------------------------------

    def session(self, leaf_name: Any) -> Any:
        """Return (creating if needed) the Barrage session for one leaf.

        Raises:
            RemoteError: If the leaf is down, or ``deephaven.barrage`` is missing.
        """
        leaf = self._leaf(leaf_name)
        with self._lock:
            existing = self._sessions.get(leaf.name)
            if existing is not None:
                return existing
            host, port = leaf.host_port
            try:
                from deephaven.barrage import barrage_session
            except ImportError as exc:  # pragma: no cover - stock 42.4 ships it
                raise RemoteError(
                    "deephaven.barrage is not available in this server image; the "
                    "collector cannot take remote snapshots or run remote queries "
                    f"({exc})"
                ) from exc
            try:
                session = barrage_session(host=host, port=port)
            except Exception as exc:  # noqa: BLE001 - network/auth failures
                raise RemoteError(
                    f"could not open a Barrage session to leaf {leaf.name!r} at "
                    f"{host}:{port} ({type(exc).__name__}: {exc}); is the container up, "
                    "and does it run with anonymous auth?"
                ) from exc
            self._sessions[leaf.name] = session
            return session

    def _console(self, leaf_name: str) -> Any:
        """Return (creating if needed) the leaf's python console session."""
        with self._lock:
            existing = self._consoles.get(leaf_name)
            if existing is not None:
                return existing
        session = self.session(leaf_name)
        try:
            console = session.j_barrage_session.session().console("python").get()
        except Exception as exc:  # noqa: BLE001 - jpy / Java client surface
            raise RemoteError(
                f"could not open a python console on leaf {leaf_name!r} "
                f"({type(exc).__name__}: {exc}); remote queries need the leaf to run "
                "with a python console (-Ddeephaven.console.type=python)"
            ) from exc
        with self._lock:
            self._consoles[leaf_name] = console
        return console

    def close(self) -> None:
        """Drop every live query and close every session (best effort, never raises)."""
        self.release_live()
        with self._lock:
            sessions, self._sessions = self._sessions, {}
            self._consoles = {}
        for name, session in sessions.items():
            try:
                session.close()
            except Exception:  # noqa: BLE001 - shutdown must never raise
                print(f"[remote-uri] closing the Barrage session to {name!r} failed:")
                traceback.print_exc()

    # -- mechanism 1: subscription -----------------------------------------------

    def subscribe(self, leaf_name: Any, name: str) -> Any:
        """Resolve one of a leaf's exports as a **live** table.

        ``uri`` mode goes through ``deephaven.uri.resolve``; ``barrage`` mode
        subscribes an explicit scope ticket on this resolver's session. Both produce
        the same Barrage subscription -- the second form is the one that also takes
        an auth type/token, which is why it is a supported alternative rather than
        dead code.

        Raises:
            RemoteError: If the export is not (yet) bound on the leaf, or the leaf
                is unreachable.
        """
        leaf = self._leaf(leaf_name)
        if self._settings.resolver == RESOLVER_BARRAGE:
            session = self.session(leaf.name)
            try:
                return session.subscribe(scope_ticket(name))
            except Exception as exc:  # noqa: BLE001 - not-yet-exported is the common case
                raise RemoteError(
                    f"leaf {leaf.name!r} does not expose {name!r} "
                    f"({type(exc).__name__}: {exc})"
                ) from exc
        uri = scope_uri(leaf.uri, name)
        try:
            from deephaven.uri import resolve
        except ImportError as exc:  # pragma: no cover - stock 42.4 ships it
            raise RemoteError(f"deephaven.uri is not available in this server image ({exc})") from exc
        try:
            return resolve(uri)
        except Exception as exc:  # noqa: BLE001 - not-yet-exported is the common case
            # The server's BarrageTableResolver caches ONE session per target and
            # never re-authenticates: after a leaf restart every resolve() to it fails
            # UNAUTHENTICATED for the life of this JVM. A fresh barrage_session for
            # the leaf (created lazily here, dropped by close() on reconnect) yields
            # the same subscription, so fall back to it rather than failing forever.
            # A leaf that has simply not exported yet fails both ways and the caller's
            # retry loop keeps waiting exactly as before (doc 10 section 3).
            first = f"{type(exc).__name__}: {exc}"
            try:
                session = self.session(leaf.name)
                table = session.subscribe(scope_ticket(name))
            except Exception as exc2:  # noqa: BLE001
                raise RemoteError(
                    f"could not resolve {uri} ({first}); barrage fallback also failed "
                    f"({type(exc2).__name__}: {exc2})"
                ) from exc2
            print(
                f"[remote-uri] resolve({uri}) failed ({first}); subscribed {name!r} on "
                f"leaf {leaf.name!r} through a fresh barrage session instead"
            )
            return table

    # -- mechanism 2: snapshot ---------------------------------------------------

    def snapshot(self, leaf_name: Any, name: str) -> Any:
        """Take a **static** copy of one of a leaf's globals.

        Always Barrage, in both resolver modes: ``deephaven.uri`` has no one-shot
        form, and a snapshot is what fleet health and drill-downs want -- a table
        that does not keep a subscription open after the panel is closed.
        """
        leaf = self._leaf(leaf_name)
        session = self.session(leaf.name)
        try:
            return session.snapshot(scope_ticket(name))
        except Exception as exc:  # noqa: BLE001
            raise RemoteError(
                f"could not snapshot {name!r} on leaf {leaf.name!r} "
                f"({type(exc).__name__}: {exc})"
            ) from exc

    # -- mechanism 3: remote query -----------------------------------------------

    def execute(self, leaf_name: Any, script: str, what: str = "script") -> None:
        """Run python on a leaf's console.

        ``executeCode`` reports *failures*, not values: the returned ``Changes``
        carries a java ``Optional<String>`` error message, which is the only way to
        find out that the script did not run.

        Raises:
            RemoteError: If the console is unreachable or the script failed.
        """
        leaf = self._leaf(leaf_name)
        console = self._console(leaf.name)
        try:
            changes = console.executeCode(script)
        except Exception as exc:  # noqa: BLE001 - jpy / Java client surface
            raise RemoteError(
                f"remote {what} on leaf {leaf.name!r} could not be sent "
                f"({type(exc).__name__}: {exc})"
            ) from exc
        message = _error_message(changes)
        if message:
            raise RemoteError(f"remote {what} on leaf {leaf.name!r} failed: {message}")

    def query_snapshot(self, leaf_name: Any, expression: str) -> Any:
        """Run a filter **on the leaf** and bring back a static result.

        Only matching rows cross the wire -- the point of the mechanism. The
        transient global is deleted as soon as the snapshot has been taken, even if
        the snapshot itself failed.

        Args:
            leaf_name: The leaf owning the data.
            expression: A python expression evaluating to a Table on the leaf, e.g.
                ``'oms_executions.where("GlobalKey == `OMS-A|A-1`")'``.

        Returns:
            A static :class:`~deephaven.table.Table`.
        """
        leaf = self._leaf(leaf_name)
        name = next_query_name()
        self.execute(leaf.name, f"{name} = {expression}", what=f"query {name}")
        try:
            return self.snapshot(leaf.name, name)
        finally:
            self._drop(leaf.name, name)

    def query_live(self, leaf_name: Any, expression: str) -> Any:
        """Run a filter on the leaf and subscribe to the **live** result.

        The transient global stays bound on the leaf: deleting it would pull the
        subscription's source out from under it. :meth:`release_live` (called by
        ``reconnect()``) is what eventually drops it.
        """
        leaf = self._leaf(leaf_name)
        name = next_query_name()
        self.execute(leaf.name, f"{name} = {expression}", what=f"live query {name}")
        session = self.session(leaf.name)
        try:
            table = session.subscribe(scope_ticket(name))
        except Exception as exc:  # noqa: BLE001
            self._drop(leaf.name, name)
            raise RemoteError(
                f"could not subscribe to the live query {name!r} on leaf {leaf.name!r} "
                f"({type(exc).__name__}: {exc})"
            ) from exc
        with self._lock:
            self._live.append((leaf.name, name))
        return table

    def release_live(self) -> int:
        """``del`` every live query global this resolver bound (best effort).

        Returns:
            How many were dropped. Failures are logged, not raised: a leaf that is
            down has already forgotten its globals, which is the outcome anyway.
        """
        with self._lock:
            pending, self._live = self._live, []
        dropped = 0
        for leaf_name, name in pending:
            if self._drop(leaf_name, name):
                dropped += 1
        return dropped

    def _drop(self, leaf_name: str, name: str) -> bool:
        """``del <name>`` on a leaf, swallowing every failure."""
        try:
            self.execute(leaf_name, f"del {name}", what=f"cleanup of {name}")
            return True
        except Exception as exc:  # noqa: BLE001 - cleanup is best effort
            print(f"[remote-uri] could not drop {name} on leaf {leaf_name!r}: {exc}")
            return False

    # -- the startup resolve loop -------------------------------------------------

    def resolve_once(self) -> ResolveReport:
        """One pass: try to resolve all four exports of every leaf."""
        resolved: Dict[str, Dict[str, Any]] = {}
        missing: List[str] = []
        for leaf in self._settings.leaves:
            tables: Dict[str, Any] = {}
            failures: List[str] = []
            for name in LEAF_EXPORTS:
                try:
                    tables[name] = self.subscribe(leaf.name, name)
                except Exception as exc:  # noqa: BLE001 - collected, not raised
                    failures.append(f"  {leaf.name} {name}: {exc}")
            if failures:
                missing.extend(failures)
            else:
                resolved[leaf.name] = tables
        return ResolveReport(resolved=resolved, missing=missing)

    def wait_for_all(
        self,
        timeout: Optional[float] = None,
        interval: Optional[float] = None,
    ) -> ResolveReport:
        """Retry :meth:`resolve_once` until every export is there, or time runs out.

        ``resolve()`` is not retried by Deephaven and a leaf's gRPC port answers long
        before Application Mode has bound ``rx_orders``, so this loop -- not the
        compose healthcheck -- is what "the fleet is ready" means.

        Args:
            timeout: Seconds to keep trying; defaults to ``REMOTEURI_CONNECT_TIMEOUT``.
            interval: Seconds between attempts; defaults to
                ``REMOTEURI_CONNECT_INTERVAL``.

        Returns:
            The last :class:`ResolveReport`. **Never raises on timeout**: the caller
            logs one actionable line per missing export and leaves the server up so
            ``reconnect()`` can be run from the console (doc 10 section 6).
        """
        limit = self._settings.connect_timeout if timeout is None else float(timeout)
        step = self._settings.connect_interval if interval is None else float(interval)
        started = time.monotonic()
        attempts = 0
        report = ResolveReport(resolved={}, missing=("no attempt made",))
        while True:
            attempts += 1
            report = self.resolve_once()
            report.attempts = attempts
            report.elapsed = time.monotonic() - started
            if report.complete:
                return report
            if report.elapsed + step > limit:
                return report
            print(
                f"[remote-uri] waiting for the fleet: {len(report.missing)} export(s) "
                f"not resolved yet after {report.elapsed:.0f}s of {limit:.0f}s "
                f"(retrying every {step:.0f}s)"
            )
            time.sleep(step)


def next_query_name() -> str:
    """The next ``rx_q_<n>`` transient global name."""
    return f"{QUERY_GLOBAL_PREFIX}{next(_QUERY_SEQUENCE)}"


def _error_message(changes: Any) -> str:
    """Read ``Changes.errorMessage()``, whatever jpy hands back.

    The Java API returns ``Optional<String>``; jpy usually surfaces it as the Java
    object (``isPresent()``/``get()``), but a build that auto-converts it to
    ``None``/``str`` must behave identically rather than reporting every successful
    script as failed.
    """
    try:
        error = changes.errorMessage()
    except Exception as exc:  # noqa: BLE001 - never fail on the error path
        return f"could not read the error message ({type(exc).__name__}: {exc})"
    if error is None:
        return ""
    is_present = getattr(error, "isPresent", None)
    if callable(is_present):
        try:
            return str(error.get()) if is_present() else ""
        except Exception as exc:  # noqa: BLE001
            return f"could not read the error message ({type(exc).__name__}: {exc})"
    text = str(error)
    # An empty Optional stringifies as "Optional.empty" on the java side.
    if not text or text == "Optional.empty":
        return ""
    return text
