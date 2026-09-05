import datetime as dt

import pytest

from market_data_demo.layout import (
    ParquetRef,
    clamp_range,
    day_prefix,
    days_in_range,
    normalize_symbol,
    parse_day_prefix,
    parse_relative_path,
    parse_symbols,
    relative_path,
    to_date,
    trading_days,
)


def test_relative_path_and_prefix():
    assert day_prefix(dt.date(2026, 9, 4)) == "2026/09/04"
    assert relative_path(dt.date(2026, 9, 4), "aapl") == "2026/09/04/AAPL.parquet"


@pytest.mark.parametrize("raw", ["AAPL", " msft ", "brk.b", "BRK-B", "es_f", "9988"])
def test_normalize_symbol_accepts_tickers(raw):
    assert normalize_symbol(raw) == raw.strip().upper()


@pytest.mark.parametrize("raw", ["", "  ", "A/B", "A B", "`X`", "x" * 33, None, ".hidden"])
def test_normalize_symbol_rejects_junk(raw):
    with pytest.raises(ValueError):
        normalize_symbol(raw)


def test_parse_symbols_dedupes_and_orders():
    assert parse_symbols("aapl, msft;NVDA  aapl") == ["AAPL", "MSFT", "NVDA"]
    assert parse_symbols(["msft", "", None, "MSFT"]) == ["MSFT"]
    assert parse_symbols(None) == []
    assert parse_symbols("") == []


def test_parse_day_prefix():
    assert parse_day_prefix("2026/09/04") == dt.date(2026, 9, 4)
    assert parse_day_prefix("/2026/09/04/") == dt.date(2026, 9, 4)
    assert parse_day_prefix("2026/13/04") is None
    assert parse_day_prefix("2026/9/4") is None
    assert parse_day_prefix("ohlc/2026/09/04") is None


def test_parse_relative_path_file_shape():
    ref = parse_relative_path("2026/09/04/AAPL.parquet")
    assert ref == ParquetRef(dt.date(2026, 9, 4), "AAPL", "2026/09/04/AAPL.parquet")
    assert parse_relative_path("2026/09/04/aapl.PARQUET").symbol == "AAPL"


def test_parse_relative_path_directory_shape():
    ref = parse_relative_path("2026/09/04/NVDA/part-00000-abc.parquet")
    assert ref.day == dt.date(2026, 9, 4)
    assert ref.symbol == "NVDA"
    assert parse_relative_path("2026/09/04/NVDA/_metadata") is None
    assert parse_relative_path("2026/09/04/NVDA/.part.parquet.crc") is None


@pytest.mark.parametrize(
    "rel",
    [
        "2026/09/04/_SUCCESS",
        "2026/09/04/notes.txt",
        "2026/09/04/.parquet",
        "2026/09/AAPL.parquet",
        "2026/09/04/a/b/c.parquet",
        "2026/09/31/AAPL.parquet",
        "README.md",
        "",
    ],
)
def test_parse_relative_path_rejects(rel):
    assert parse_relative_path(rel) is None


def test_days_and_trading_days():
    start, end = dt.date(2026, 9, 3), dt.date(2026, 9, 8)  # Thu .. Tue
    assert len(days_in_range(start, end)) == 6
    assert trading_days(start, end) == [
        dt.date(2026, 9, 3),
        dt.date(2026, 9, 4),
        dt.date(2026, 9, 7),
        dt.date(2026, 9, 8),
    ]
    assert days_in_range(end, start) == []
    assert trading_days(dt.date(2026, 9, 5), dt.date(2026, 9, 6)) == []


class _JavaLike:
    def __init__(self, text):
        self.text = text

    def __str__(self):
        return self.text


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-09-04", dt.date(2026, 9, 4)),
        (" 2026-09-04 ", dt.date(2026, 9, 4)),
        ("2026-09-04T13:30:00Z", dt.date(2026, 9, 4)),
        ("2026-09-04T09:30-04:00[America/New_York]", dt.date(2026, 9, 4)),
        (dt.date(2026, 9, 4), dt.date(2026, 9, 4)),
        (dt.datetime(2026, 9, 4, 13, 30), dt.date(2026, 9, 4)),
        (_JavaLike("2026-09-04"), dt.date(2026, 9, 4)),
        (None, None),
        ("", None),
        ("yesterday", None),
        ("2026-99-04", None),
    ],
)
def test_to_date(value, expected):
    assert to_date(value) == expected


def test_clamp_range():
    avail = [dt.date(2026, 9, 1), dt.date(2026, 9, 2), dt.date(2026, 9, 3)]
    assert clamp_range(None, None, avail) == (dt.date(2026, 9, 1), dt.date(2026, 9, 3))
    assert clamp_range(dt.date(2026, 9, 2), None, avail) == (dt.date(2026, 9, 2), dt.date(2026, 9, 3))
    assert clamp_range(dt.date(2026, 9, 3), dt.date(2026, 9, 1), avail) == (dt.date(2026, 9, 1), dt.date(2026, 9, 3))
    assert clamp_range(None, None, []) is None
