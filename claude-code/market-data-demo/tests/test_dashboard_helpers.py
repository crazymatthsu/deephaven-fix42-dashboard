import datetime as dt

import pytest

from market_data_demo.charts import CHART_TYPES, PER_SYMBOL_CHARTS, gap_rangebreaks
from market_data_demo.config import Config
from market_data_demo.dashboard import PRESETS, coerce_range, coerce_selection, initial_symbols, preset_range
from market_data_demo.derived import INTERVALS, interval_nanos, interval_seconds, symbol_filter

DAYS = [dt.date(2026, 8, 3) + dt.timedelta(days=i) for i in range(30) if (dt.date(2026, 8, 3) + dt.timedelta(days=i)).weekday() < 5]


def test_presets_cover_registry():
    assert [label for label, _ in PRESETS] == ["1D", "5D", "1M", "3M", "All"]
    assert preset_range(DAYS, 1) == (DAYS[-1], DAYS[-1])
    assert preset_range(DAYS, 5) == (DAYS[-5], DAYS[-1])
    assert preset_range(DAYS, 63) == (DAYS[0], DAYS[-1])  # more than available -> all
    assert preset_range(DAYS, None) == (DAYS[0], DAYS[-1])
    assert preset_range([], 5) is None
    assert preset_range(list(reversed(DAYS)), 2) == (DAYS[-2], DAYS[-1])


def test_coerce_selection():
    universe = ["AAPL", "MSFT", "NVDA"]
    assert coerce_selection("all", universe) == universe
    assert coerce_selection(["NVDA", "aapl", "ZZZ"], universe) == ["AAPL", "NVDA"]
    assert coerce_selection({"MSFT"}, universe) == ["MSFT"]
    assert coerce_selection("msft,aapl", universe) == ["AAPL", "MSFT"]
    assert coerce_selection(None, universe) == []
    assert coerce_selection([], universe) == []
    assert coerce_selection(["x y"], universe) == []
    assert coerce_selection(["TSLA"], []) == ["TSLA"]  # empty universe: trust the caller


class _Range:
    def __init__(self, start, end):
        self.start, self.end = start, end


def test_coerce_range():
    assert coerce_range({"start": "2026-09-01", "end": "2026-09-04"}) == (dt.date(2026, 9, 1), dt.date(2026, 9, 4))
    assert coerce_range({"start": "2026-09-04", "end": "2026-09-01"}) == (dt.date(2026, 9, 1), dt.date(2026, 9, 4))
    assert coerce_range(_Range("2026-09-01T00:00:00Z", dt.date(2026, 9, 2))) == (dt.date(2026, 9, 1), dt.date(2026, 9, 2))
    assert coerce_range((dt.date(2026, 9, 1), dt.date(2026, 9, 1))) == (dt.date(2026, 9, 1), dt.date(2026, 9, 1))
    assert coerce_range({"start": None, "end": "2026-09-04"}) is None
    assert coerce_range(None) is None
    assert coerce_range("garbage") is None


def test_initial_symbols():
    universe = ["AAPL", "MSFT", "NVDA", "TSLA"]
    assert initial_symbols(Config(default_symbols=["TSLA", "ZZZ"]), universe) == ["TSLA"]
    assert initial_symbols(Config(default_symbols=["ZZZ"]), universe) == ["AAPL", "MSFT", "NVDA"]
    assert initial_symbols(Config(), universe, fallback=2) == ["AAPL", "MSFT"]
    assert initial_symbols(Config(), []) == []


def test_intervals():
    assert list(INTERVALS) == ["1m", "5m", "15m", "30m", "1h", "1D"]
    assert interval_seconds("5m") == 300
    assert interval_nanos("1h") == 3_600_000_000_000
    with pytest.raises(ValueError):
        interval_seconds("2m")


def test_chart_registry():
    assert list(CHART_TYPES) == ["candlestick", "ohlc", "line", "area", "normalized", "volume"]
    assert set(PER_SYMBOL_CHARTS) <= set(CHART_TYPES)


def test_gap_rangebreaks_follow_dst():
    summer = gap_rangebreaks(dt.date(2026, 9, 4))
    assert summer[0] == {"bounds": ["sat", "mon"]}
    assert summer[1] == {"bounds": [20.0, 13.5], "pattern": "hour"}
    winter = gap_rangebreaks(dt.date(2026, 12, 4))
    assert winter[1] == {"bounds": [21.0, 14.5], "pattern": "hour"}
    assert len(gap_rangebreaks(None)) == 2


def test_symbol_filter():
    assert symbol_filter(["aapl", "MSFT"]) == "Symbol in `AAPL`, `MSFT`"
    assert symbol_filter([]) is None
