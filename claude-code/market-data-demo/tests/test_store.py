import datetime as dt
from pathlib import Path

import pytest

from market_data_demo.layout import ParquetRef
from market_data_demo.store import LocalStore, S3Store, summarize_inventory


def test_local_store_lists_both_shapes_and_ignores_junk(local_tree, days):
    store = LocalStore(local_tree)
    refs = store.list_files(days[0], days[4])
    assert [(r.day, r.symbol) for r in refs] == [
        (days[0], "AAPL"),
        (days[0], "MSFT"),
        (days[1], "AAPL"),
        (days[1], "MSFT"),
        (days[2], "AAPL"),
        (days[2], "MSFT"),
        (days[2], "NVDA"),
    ]
    assert all(Path(r.path).is_absolute() and Path(r.path).is_file() for r in refs)
    assert refs[-1].path.endswith("NVDA/part-0000.parquet")


def test_local_store_filters_symbols_and_days(local_tree, days):
    store = LocalStore(local_tree)
    only = store.list_files(days[1], days[2], symbols=["msft", "nvda"])
    assert [(r.day, r.symbol) for r in only] == [(days[1], "MSFT"), (days[2], "MSFT"), (days[2], "NVDA")]
    assert store.list_files(days[3], days[4]) == []
    assert store.list_files(days[2], days[0]) == []
    assert store.available_days() == days[:3]
    assert store.available_symbols() == ["AAPL", "MSFT", "NVDA"]
    assert store.available_symbols(days[0], days[1]) == ["AAPL", "MSFT"]
    assert "local:" in store.describe()


def test_local_store_missing_root(tmp_path):
    store = LocalStore(tmp_path / "nowhere")
    assert store.available_days() == []
    assert store.available_symbols() == []
    assert store.list_files(dt.date(2026, 1, 1), dt.date(2026, 1, 5)) == []


def _seed(fake, prefix):
    keys = [
        f"{prefix}2026/09/01/AAPL.parquet",
        f"{prefix}2026/09/01/MSFT.parquet",
        f"{prefix}2026/09/01/_SUCCESS",
        f"{prefix}2026/09/02/AAPL.parquet",
        f"{prefix}2026/09/02/NVDA/part-0.parquet",
        f"{prefix}2026/09/02/NVDA/part-1.parquet",
        f"{prefix}2026/09/02/NVDA/_metadata",
        f"{prefix}2026/10/01/AAPL.parquet",
        f"{prefix}README.md",
    ]
    for key in keys:
        fake.objects[key] = b"x"


@pytest.mark.parametrize("prefix", ["", "ohlc/"])
def test_s3_store_lists_per_day_prefix_with_pagination(fake_s3, prefix):
    _seed(fake_s3, prefix)
    store = S3Store("market-data", prefix.strip("/"), client=fake_s3)
    refs = store.list_files(dt.date(2026, 9, 1), dt.date(2026, 9, 2))
    assert [(r.day, r.symbol) for r in refs] == [
        (dt.date(2026, 9, 1), "AAPL"),
        (dt.date(2026, 9, 1), "MSFT"),
        (dt.date(2026, 9, 2), "AAPL"),
        (dt.date(2026, 9, 2), "NVDA"),
        (dt.date(2026, 9, 2), "NVDA"),
    ]
    assert refs[0].path == f"s3://market-data/{prefix}2026/09/01/AAPL.parquet"
    assert refs[-1].path.endswith("NVDA/part-1.parquet")
    # one Prefix per day, never a whole-bucket scan
    prefixes = {call["Prefix"] for call in fake_s3.calls}
    assert prefixes == {f"{prefix}2026/09/01/", f"{prefix}2026/09/02/"}
    assert any("ContinuationToken" in call for call in fake_s3.calls)


def test_s3_store_symbol_filter_and_days(fake_s3):
    _seed(fake_s3, "ohlc/")
    store = S3Store("market-data", "/ohlc/", client=fake_s3)
    assert store.prefix == "ohlc"
    assert store.root_uri == "s3://market-data/ohlc"
    only = store.list_files(dt.date(2026, 9, 1), dt.date(2026, 9, 2), symbols=["nvda"])
    assert {r.symbol for r in only} == {"NVDA"} and len(only) == 2
    assert store.available_days() == [dt.date(2026, 9, 1), dt.date(2026, 9, 2), dt.date(2026, 10, 1)]
    assert store.available_symbols() == ["AAPL", "MSFT", "NVDA"]
    assert store.available_symbols(dt.date(2026, 10, 1), dt.date(2026, 10, 1)) == ["AAPL"]
    assert store.key_for("2026/09/01/AAPL.parquet") == "ohlc/2026/09/01/AAPL.parquet"


def test_s3_store_rejects_bad_bucket():
    with pytest.raises(ValueError):
        S3Store("bucket/with/slash")
    with pytest.raises(ValueError):
        S3Store("")


def test_s3_upload_tree_mirrors_layout(local_tree, fake_s3, days):
    store = S3Store("market-data", "ohlc", client=fake_s3)
    assert store.ensure_bucket() is False
    uploaded = store.upload_tree(LocalStore(local_tree), progress=lambda ref, key: None)
    assert len(uploaded) == 7
    assert "ohlc/2026/08/31/AAPL.parquet" in fake_s3.objects
    assert "ohlc/2026/09/02/NVDA/part-0000.parquet" in fake_s3.objects
    assert uploaded[0].path == "s3://market-data/ohlc/2026/08/31/AAPL.parquet"
    # the uploaded bucket lists back identically to the local tree
    listed = store.list_files(days[0], days[4])
    assert [(r.day, r.symbol) for r in listed] == [(r.day, r.symbol) for r in LocalStore(local_tree).list_files(days[0], days[4])]
    other = S3Store("brand-new", client=fake_s3)
    assert other.ensure_bucket() is True
    subset = other.upload_tree(LocalStore(local_tree), start=days[1], end=days[1], symbols=["AAPL"])
    assert [k for k in fake_s3.objects if k.startswith("2026/")] == ["2026/09/01/AAPL.parquet"]
    assert len(subset) == 1


def test_s3_client_is_lazy():
    calls = []

    def factory():
        calls.append(1)
        return object()

    store = S3Store("b", client_factory=factory)
    assert not calls
    store.client
    store.client
    assert calls == [1]


def test_summarize_inventory():
    d1, d2 = dt.date(2026, 9, 1), dt.date(2026, 9, 2)
    inv = summarize_inventory(
        [
            ParquetRef(d1, "AAPL", "a"),
            ParquetRef(d2, "AAPL", "b"),
            ParquetRef(d2, "NVDA", "c1"),
            ParquetRef(d2, "NVDA", "c2"),
        ]
    )
    assert inv.symbols == ["AAPL", "NVDA"]
    assert inv.days == [d1, d2]
    assert inv.first_day == d1 and inv.last_day == d2
    assert inv.symbol_rows[1] == {"Symbol": "NVDA", "FirstDay": d2, "LastDay": d2, "Days": 1, "Files": 2}
    assert inv.day_rows[1] == {"Day": d2, "Symbols": 2, "SymbolList": "AAPL,NVDA"}
    empty = summarize_inventory([])
    assert empty.symbols == [] and empty.first_day is None
