"""The mock-venue thread: ``venue.step()`` every ``OMS_SIM_INTERVAL_MS``. Server only.

The thread runs inside the execution context captured when the simulator is created
(the app-mode script's), because the bridge builds one-row tables with ``new_table`` on
every fill and that needs a context on the calling thread. ``InputTable.add`` takes the
update-graph lock itself, so no explicit locking is needed here; the core serialises
the venue against the traders' callbacks with its own lock.
"""

from __future__ import annotations

import threading
import time
import traceback
from typing import Any, Optional

from basket_oms_demo.venue import MockVenue

__all__ = ["Simulator"]


class Simulator:
    def __init__(self, venue: MockVenue, interval_ms: int = 750, exec_ctx: Optional[Any] = None) -> None:
        self.venue = venue
        self.interval = max(0.05, interval_ms / 1000.0)
        self._ctx = exec_ctx
        if self._ctx is None:
            try:
                from deephaven.execution_context import get_exec_ctx

                self._ctx = get_exec_ctx()
            except Exception:  # noqa: BLE001 - outside a server (tests) there is no context
                self._ctx = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.steps = 0
        self.errors = 0
        self.last_error: Optional[str] = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive() and not self._stop.is_set()

    def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="oms-mock-venue", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout)
        self._thread = None

    def step_once(self) -> Any:
        """One synchronous step (the console API's ``oms_sim("step")``)."""
        if self._ctx is not None:
            with self._ctx:
                return self._step()
        return self._step()

    def _step(self) -> Any:
        try:
            report = self.venue.step()
            self.steps += 1
            return report
        except Exception as exc:  # noqa: BLE001 - keep the thread alive, report once per error
            self.errors += 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            print(f"[basket-oms] venue step failed: {self.last_error}", flush=True)
            traceback.print_exc()
            return None

    def _run(self) -> None:
        if self._ctx is not None:
            with self._ctx:
                self._loop()
        else:
            self._loop()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._step()
            self._stop.wait(self.interval)
