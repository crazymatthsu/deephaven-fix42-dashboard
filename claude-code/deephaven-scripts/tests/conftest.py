"""Shared pytest fixtures for the fix42cache suite."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from fix42cache import OrderStateMachine


class FakeClock:
    """Deterministic ingest clock: every call advances by a fixed step."""

    def __init__(
        self,
        start: datetime | None = None,
        step: timedelta = timedelta(milliseconds=1),
    ) -> None:
        self.current = start or datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
        self.step = step

    def __call__(self) -> datetime:
        value = self.current
        self.current = self.current + self.step
        return value


@pytest.fixture
def clock() -> FakeClock:
    """A deterministic UTC clock for ingest timestamps."""
    return FakeClock()


@pytest.fixture
def machine(clock: FakeClock) -> OrderStateMachine:
    """A fresh state machine wired to the deterministic clock."""
    return OrderStateMachine(now_fn=clock)
