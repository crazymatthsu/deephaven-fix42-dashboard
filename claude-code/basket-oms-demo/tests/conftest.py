"""Shared fixtures: a fixed clock and a recording listener around an OmsCore."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace
from typing import List

import pytest

from basket_oms_demo.core import Basket, Event, Execution, OmsCore, Order, Quote


class FixedClock:
    """Advances by one second per call so `Updated` ordering is testable."""

    def __init__(self, start: dt.datetime | None = None) -> None:
        self.now = start or dt.datetime(2026, 9, 16, 13, 30, tzinfo=dt.timezone.utc)

    def __call__(self) -> dt.datetime:
        self.now = self.now + dt.timedelta(seconds=1)
        return self.now


class RecordingListener:
    def __init__(self) -> None:
        self.baskets: List[Basket] = []
        self.orders: List[Order] = []
        self.executions: List[Execution] = []
        self.events: List[Event] = []
        self.quotes: List[Quote] = []

    def on_basket(self, basket: Basket) -> None:
        self.baskets.append(replace(basket))

    def on_order(self, order: Order) -> None:
        self.orders.append(replace(order))

    def on_execution(self, execution: Execution) -> None:
        self.executions.append(replace(execution))

    def on_event(self, event: Event) -> None:
        self.events.append(replace(event))

    def on_quote(self, quote: Quote) -> None:
        self.quotes.append(replace(quote))

    def last_order(self, order_id: str) -> Order:
        for order in reversed(self.orders):
            if order.order_id == order_id:
                return order
        raise KeyError(order_id)


@pytest.fixture
def clock() -> FixedClock:
    return FixedClock()


@pytest.fixture
def listener() -> RecordingListener:
    return RecordingListener()


@pytest.fixture
def core(clock, listener) -> OmsCore:
    return OmsCore(trader="alice", clock=clock, listener=listener)


@pytest.fixture
def basket(core):
    """A three-line basket, all NEW."""
    return core.create_basket(
        "Tech rebalance",
        ["AAPL BUY 1000 LMT 189.50", "MSFT SELL 500", "NVDA BUY 2500 LMT 118.25 GTC"],
        strategy="VWAP",
    )
