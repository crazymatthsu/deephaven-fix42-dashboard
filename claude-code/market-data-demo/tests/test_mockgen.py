import datetime as dt

import pytest

from market_data_demo.layout import ParquetRef
from market_data_demo.mockgen import (
    BARS_PER_DAY,
    COLUMNS,
    DEFAULT_SYMBOLS,
    daily_open_close,
    generate,
    generate_day_bars,
    reference_price,
    resolve_universe,
    session_timestamps,
    us_eastern_offset,
    write_day_file,
)

MON = dt.date(2026, 8, 31)


@pytest.mark.parametrize(
    "day, hours",
    [
        (dt.date(2026, 3, 7), -5),  # Saturday before the 2nd Sunday of March
        (dt.date(2026, 3, 8), -4),  # DST starts (2nd Sunday of March 2026)
        (dt.date(2026, 7, 1), -4),
        (dt.date(2026, 10, 31), -4),
        (dt.date(2026, 11, 1), -5),  # DST ends (1st Sunday of November 2026)
        (dt.date(2027, 3, 14), -4),  # 2027: 2nd Sunday is the 14th
        (dt.date(2027, 3, 13), -5),
    ],
)
def test_us_eastern_offset(day, hours):
    assert us_eastern_offset(day) == dt.timedelta(hours=hours)


def test_session_timestamps_summer_and_winter():
    summer = session_timestamps(dt.date(2026, 9, 4))
    assert len(summer) == BARS_PER_DAY == 390
    assert summer[0] == dt.datetime(2026, 9, 4, 13, 30, tzinfo=dt.timezone.utc)
    assert summer[-1] == dt.datetime(2026, 9, 4, 19, 59, tzinfo=dt.timezone.utc)
    winter = session_timestamps(dt.date(2026, 12, 4))
    assert winter[0] == dt.datetime(2026, 12, 4, 14, 30, tzinfo=dt.timezone.utc)
    assert all((b - a) == dt.timedelta(minutes=1) for a, b in zip(summer, summer[1:]))


def test_reference_price_known_and_derived():
    assert reference_price("aapl") == DEFAULT_SYMBOLS["AAPL"]
    derived = reference_price("ZZZZ")
    assert 20.0 <= derived < 500.0
    assert reference_price("ZZZZ") == derived  # stable
    assert reference_price("ZZZZ") != reference_price("YYYY")


def test_resolve_universe():
    assert resolve_universe(None) == DEFAULT_SYMBOLS
    assert resolve_universe("") == DEFAULT_SYMBOLS
    assert list(resolve_universe("msft, zzzz")) == ["MSFT", "ZZZZ"]


def test_daily_walk_is_deterministic_and_continuous():
    o1, c1 = daily_open_close("AAPL", MON, seed=42)
    o2, c2 = daily_open_close("AAPL", MON, seed=42)
    assert (o1, c1) == (o2, c2)
    assert daily_open_close("AAPL", MON, seed=43) != (o1, c1)
    # Tuesday opens an overnight gap away from Monday's close: a few percent at most.
    o_tue, _ = daily_open_close("AAPL", MON + dt.timedelta(days=1), seed=42)
    assert abs(o_tue / c1 - 1.0) < 0.05
    with pytest.raises(ValueError):
        daily_open_close("AAPL", dt.date(2019, 12, 31), seed=42)


def test_generate_day_bars_shape_and_invariants():
    bars = generate_day_bars("MSFT", MON, seed=42)
    assert len(bars) == BARS_PER_DAY
    assert set(bars.columns) == set(COLUMNS)
    assert all(len(bars.columns[name]) == BARS_PER_DAY for name in COLUMNS)
    prev_close = None
    for k in range(BARS_PER_DAY):
        row = bars.row(k)
        assert row["Symbol"] == "MSFT"
        assert row["Low"] <= min(row["Open"], row["Close"])
        assert row["High"] >= max(row["Open"], row["Close"])
        assert row["Low"] <= row["VWAP"] <= row["High"]
        assert row["Volume"] >= 1 and row["TradeCount"] >= 1
        assert round(row["Open"], 2) == row["Open"]
        if prev_close is not None:
            assert row["Open"] == prev_close  # each bar opens at the previous close
        prev_close = row["Close"]
    day_open, day_close = daily_open_close("MSFT", MON, seed=42)
    assert bars.row(0)["Open"] == round(day_open, 2)
    assert bars.row(BARS_PER_DAY - 1)["Close"] == round(day_close, 2)


def test_generate_day_bars_is_deterministic_and_seed_sensitive():
    a = generate_day_bars("NVDA", MON, seed=1)
    b = generate_day_bars("NVDA", MON, seed=1)
    c = generate_day_bars("NVDA", MON, seed=2)
    assert a.columns == b.columns
    assert a.columns["Close"] != c.columns["Close"]


def test_generate_day_bars_rejects_weekend():
    with pytest.raises(ValueError):
        generate_day_bars("AAPL", dt.date(2026, 9, 5), seed=42)


def test_volume_profile_is_u_shaped():
    bars = generate_day_bars("AAPL", MON, seed=42)
    vol = bars.columns["Volume"]
    edges = sum(vol[:30]) + sum(vol[-30:])
    lunch = sum(vol[180:240])
    assert edges > lunch


def test_write_day_file_idempotent_and_layout(tmp_path):
    pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    ref = write_day_file(tmp_path, "aapl", MON, seed=42)
    target = tmp_path / "2026" / "08" / "31" / "AAPL.parquet"
    assert ref == ParquetRef(MON, "AAPL", str(target.resolve()))
    assert target.is_file()
    first = target.read_bytes()
    write_day_file(tmp_path, "AAPL", MON, seed=42)  # overwrite with identical content
    assert target.read_bytes() == first
    table = pq.read_table(str(target))
    assert table.num_rows == BARS_PER_DAY
    assert table.schema.names == list(COLUMNS)
    assert str(table.schema.field("Timestamp").type) == "timestamp[us, tz=UTC]"
    assert not list(tmp_path.glob("**/*.tmp"))


def test_generate_skips_weekends_and_existing(tmp_path):
    pytest.importorskip("pyarrow")
    start, end = dt.date(2026, 9, 3), dt.date(2026, 9, 8)  # Thu..Tue -> 4 trading days
    seen = []
    report = generate(tmp_path, symbols="AAPL,MSFT", start=start, end=end, seed=42, progress=lambda r, w: seen.append(w))
    assert len(report.days) == 4
    assert report.symbols == ["AAPL", "MSFT"]
    assert len(report.written) == 8 and not report.skipped
    assert all(seen)
    assert not (tmp_path / "2026" / "09" / "05").exists()
    again = generate(tmp_path, symbols="AAPL,MSFT", start=start, end=end, seed=42)
    assert not again.written and len(again.skipped) == 8
    forced = generate(tmp_path, symbols="AAPL", start=start, end=start, seed=42, force=True)
    assert len(forced.written) == 1
    with pytest.raises(ValueError):
        generate(tmp_path, start=end, end=start)


def test_generate_accepts_price_mapping(tmp_path):
    pytest.importorskip("pyarrow")
    report = generate(tmp_path, symbols={"xyz": 10.0}, start=MON, end=MON, seed=1)
    assert report.symbols == ["XYZ"]
    bars = generate_day_bars("XYZ", MON, seed=1, reference=10.0)
    # ~6.5 years of a 1.2%-daily walk from a $10 anchor: same order of magnitude, never
    # the $20-500 a derived reference would give an unknown ticker.
    assert 2.0 < bars.row(0)["Open"] < 60.0
    assert bars.row(0)["Open"] != generate_day_bars("XYZ", MON, seed=1).row(0)["Open"]
