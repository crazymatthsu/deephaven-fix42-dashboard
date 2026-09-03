"""``market_data_latest``: the simulated quote table -- doc 10 section 6.

The Deephaven half of :mod:`remote_uri.marketdata`. Two moving parts:

``function_generated_table(gen, refresh_interval_ms=REMOTEURI_MD_PERIOD_MS)``
    every refresh, ``gen`` steps the python walk once and returns a **fresh**
    one-row-per-symbol ``new_table``. The definition never changes and the table
    never grows: it is O(#symbols), unlike a ``time_table`` of ticks joined onto the
    orders. No stateful python ever runs inside an ``update_view`` formula.

a ``listen()`` on the collector's order flow
    symbols that show up in orders but are not in ``REMOTEURI_MD_SYMBOLS`` are added
    to the walk with their first non-null ``Price`` (else ``100.0``). The callback
    only mutates a dict under a lock -- it builds no tables -- but it still enters
    the execution context captured at construction, because that is the contract for
    anything running on the update-graph thread (doc 04 section 1).

The walk survives a ``reconnect()``: the feed is created once and the collector's
rebuild re-attaches the listener to the *new* ``orders_all`` rather than rebuilding
the quote table, so prices do not jump back to their reference on every reconnect.
"""

from __future__ import annotations

import traceback
from datetime import datetime, timezone
from typing import Any, Optional

from deephaven import new_table
from deephaven.column import double_col, string_col
from deephaven.execution_context import get_exec_ctx
from deephaven.table_factory import function_generated_table
from deephaven.table_listener import listen

try:  # server 42.x exports the Instant column factory as datetime_col
    from deephaven.column import datetime_col as instant_col
except ImportError:  # pragma: no cover - other versions export instant_col directly
    from deephaven.column import instant_col

try:  # doc 04 flags this helper's location as version-drifting
    from deephaven.time import to_j_instant
except ImportError:  # pragma: no cover - older servers exposed it as to_datetime
    from deephaven.time import to_datetime as to_j_instant

from remote_uri.config import CollectorSettings
from remote_uri.marketdata import MarketDataWalk

__all__ = ["MARKET_DATA_COLUMNS", "MarketDataFeed"]

#: ``market_data_latest`` columns, in order (doc 10 section 6).
MARKET_DATA_COLUMNS = ("Symbol", "Bid", "Ask", "Mid", "MdTs")


def _utcnow() -> datetime:
    """Current time as a tz-aware UTC datetime."""
    return datetime.now(timezone.utc)


def _safe_listen(source: Any, callback: Any) -> Optional[Any]:
    """``listen`` without ``do_replay``, for a build that does not accept it."""
    try:
        return listen(source, callback, description="remote-uri-market-data")
    except Exception as exc:  # noqa: BLE001 - the quote table works without it
        print(
            f"[remote-uri] could not listen for new symbols "
            f"({type(exc).__name__}: {exc}); only the configured symbols will be quoted"
        )
        return None


class MarketDataFeed:
    """Owns the walk, the generated table and the symbol listener.

    A plain class rather than a ``dataclass`` (Application Mode breaks
    ``dataclasses``' introspection -- see ``multi_oms.app.Runtime``).
    """

    def __init__(self, settings: CollectorSettings) -> None:
        """Seed the walk from ``REMOTEURI_MD_SYMBOLS`` and build the quote table."""
        self.settings = settings
        self.walk = MarketDataWalk(
            universe=settings.md_universe,
            seed=settings.md_seed,
            spread_bps=settings.md_spread_bps,
        )
        # Captured on the setup thread, entered inside the listener callback.
        self._ctx = get_exec_ctx()
        self._handle: Any = None
        self._source: Any = None
        self.table = self._build_table()

    # -- the table ---------------------------------------------------------------

    def _build_table(self) -> Any:
        """The ``function_generated_table`` behind ``market_data_latest``."""

        def generate() -> Any:
            """Step the walk once and render the whole book (never raises)."""
            try:
                rows = self.walk.step()
            except Exception:  # noqa: BLE001 - a stall would freeze the quote table
                traceback.print_exc()
                rows = self.walk.snapshot_rows()
            stamp = to_j_instant(_utcnow())
            return new_table(
                [
                    string_col("Symbol", [row[0] for row in rows]),
                    double_col("Bid", [float(row[1]) for row in rows]),
                    double_col("Ask", [float(row[2]) for row in rows]),
                    double_col("Mid", [float(row[3]) for row in rows]),
                    instant_col("MdTs", [stamp] * len(rows)),
                ]
            ).sort(["Symbol"])

        return function_generated_table(
            generate, refresh_interval_ms=self.settings.md_period_ms
        )

    # -- the symbol listener -----------------------------------------------------

    def attach(self, orders: Any) -> Any:
        """(Re-)subscribe the symbol listener to a collector order table.

        Args:
            orders: ``orders_all`` (or any table carrying ``Symbol`` and ``Price``).

        Returns:
            The listener handle, also parked on this feed. Called again after a
            ``reconnect()``: the previous handle is stopped first, because the table
            it listened to has been replaced.
        """
        self.detach()
        try:
            source = orders.view(["Symbol", "Price"]).select_distinct(["Symbol", "Price"])
        except Exception as exc:  # noqa: BLE001 - the quote table works without it
            print(
                f"[remote-uri] could not build the market-data symbol source "
                f"({type(exc).__name__}: {exc}); only {len(self.walk)} configured "
                "symbols will be quoted"
            )
            return None
        self._source = source
        # do_replay=True delivers the rows that were already there as one "added"
        # update: the Barrage subscriptions carry their initial snapshot before this
        # listener is attached, so without a replay every symbol of the first batch
        # would go unquoted until a *new* (Symbol, Price) pair turned up.
        try:
            self._handle = listen(
                source,
                self._on_update,
                description="remote-uri-market-data",
                do_replay=True,
            )
        except TypeError:  # pragma: no cover - a build without do_replay
            self._handle = _safe_listen(source, self._on_update)
        except Exception as exc:  # noqa: BLE001
            print(
                f"[remote-uri] could not listen for new symbols "
                f"({type(exc).__name__}: {exc}); only the configured symbols will be "
                "quoted -- unquoted orders mark at their own limit price"
            )
            self._handle = None
        return self._handle

    def detach(self) -> None:
        """Stop the symbol listener (best effort; never raises)."""
        handle, self._handle = self._handle, None
        self._source = None
        if handle is None:
            return
        try:
            handle.stop()
        except Exception:  # noqa: BLE001 - shutdown must never raise
            traceback.print_exc()

    def _on_update(self, update: Any, is_replay: bool = False) -> None:
        """Update-graph thread: add any symbol this cycle introduced."""
        try:
            added = update.added()
            if not added:
                return
            symbols = added.get("Symbol")
            prices = added.get("Price")
            if symbols is None:
                return
            with self._ctx:
                for index, symbol in enumerate(symbols):
                    price = prices[index] if prices is not None and index < len(prices) else None
                    self.walk.ensure(symbol, price)
        except Exception:  # noqa: BLE001 - a listener exception would kill the stream
            traceback.print_exc()

    # -- reporting ---------------------------------------------------------------

    def describe(self) -> str:
        """One-line summary for the startup banner."""
        listening = "listening" if self._handle is not None else "not listening"
        return (
            f"{self.walk.describe()} every {self.settings.md_period_ms}ms "
            f"({listening} for new symbols)"
        )
