"""End-to-end over the real Deephaven engine, using the embedded ``deephaven_server``.

Skipped unless BOTH hold: ``deephaven_server`` is importable (``pip install
deephaven-server==42.4`` -- ~600 MB with its JVM jars, needs JDK 17+) and ``MD_DH_TEST=1``
is set, so the default unit run stays light. What it proves, on a generated local tree:

* the parquet files the generator writes are what ``deephaven.parquet.read`` expects
  (UTC ``Instant`` timestamps, the canonical columns);
* the reader merges the right files for a period and symbol list and caches them;
* resampling, the daily summary and the normalized view are valid Deephaven query
  expressions and produce the expected row counts;
* every chart type builds a ``deephaven.plot.express`` figure;
* the ``deephaven.ui`` dashboard builds (``build_dashboard`` returns an element).
"""

from __future__ import annotations

import datetime as dt
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("MD_DH_TEST") != "1", reason="set MD_DH_TEST=1 to run the embedded Deephaven test"
)

deephaven_server = pytest.importorskip("deephaven_server")

DAYS = [dt.date(2026, 8, 31) + dt.timedelta(days=i) for i in range(5)]  # Mon..Fri


@pytest.fixture(scope="module")
def server():
    """Start the embedded server.

    ``Server.start()`` opens the script session's ExecutionContext on the calling thread, so
    everything this module does on the pytest main thread has a QueryScope and an
    UpdateGraph. Threads the *reader* spawns do not -- a table constructed on such a thread
    captures a poisoned update graph and the later ``merge`` fails -- which is what
    ``BarReader`` guards against by re-applying its captured context on its workers.
    """
    port = int(os.environ.get("MD_DH_TEST_PORT", "10095"))
    srv = deephaven_server.Server(port=port, jvm_args=["-Xmx1g"])
    srv.start()
    return srv


@pytest.fixture(scope="module")
def tree(tmp_path_factory):
    from market_data_demo.mockgen import generate

    root = tmp_path_factory.mktemp("md") / "data"
    generate(root, symbols="AAPL,MSFT,NVDA", start=DAYS[0], end=DAYS[4], seed=42)
    return root


@pytest.fixture(scope="module")
def runtime(server, tree):
    from market_data_demo.config import load_config, make_store
    from market_data_demo.reader import BarReader
    from market_data_demo.store import summarize_inventory

    cfg = load_config({"MD_LOCAL_ROOT": str(tree), "MD_DEFAULT_SYMBOLS": "AAPL,MSFT", "MD_DEFAULT_DAYS": "2"})
    store = make_store(cfg)
    reader = BarReader(store, cfg)
    days = store.available_days()
    inventory = summarize_inventory(store.list_files(days[0], days[-1]))
    return cfg, store, reader, inventory


def test_reader_merges_and_caches(runtime):
    cfg, store, reader, inventory = runtime
    result = reader.read(DAYS[0], DAYS[2], ["AAPL", "MSFT", "ZZZ"])
    assert result.ok, result.errors
    assert len(result.files) == 6
    assert result.missing_symbols == ["ZZZ"]
    assert result.table.size == 6 * 390
    assert list(result.table.column_names) == [
        "Timestamp", "Symbol", "Open", "High", "Low", "Close", "Volume", "VWAP", "TradeCount",
    ]
    assert result.table.columns[0].data_type.j_name == "java.time.Instant"
    again = reader.read(DAYS[0], DAYS[2], ["AAPL", "MSFT"])
    assert again.cached_files == 6
    everything = reader.read(DAYS[0], DAYS[4], None)
    assert everything.symbols == ["AAPL", "MSFT", "NVDA"]
    assert everything.table.size == 15 * 390
    none = reader.read(DAYS[3], DAYS[4], ["TSLA"])
    assert none.ok and none.table.size == 0 and none.missing_symbols == ["TSLA"]


def test_first_bar_is_1330z_and_sorted(runtime):
    cfg, store, reader, inventory = runtime
    table = reader.read(DAYS[0], DAYS[0], ["AAPL"]).table
    import pandas as pd  # via deephaven's dependency

    from deephaven.pandas import to_pandas

    frame = to_pandas(table.head(2))
    first = pd.Timestamp(frame["Timestamp"].iloc[0])
    assert (first.hour, first.minute) == (13, 30)
    assert frame["Timestamp"].iloc[1] > frame["Timestamp"].iloc[0]


def test_resample_daily_and_normalized(runtime):
    from market_data_demo.derived import daily_summary, normalized, resample

    cfg, store, reader, inventory = runtime
    bars = reader.read(DAYS[0], DAYS[1], ["AAPL", "MSFT"]).table
    assert resample(bars, "1m") is bars
    five = resample(bars, "5m")
    assert five.size == 2 * 2 * 78
    assert "Bars" in five.column_names
    hourly = resample(bars, "1h")
    assert hourly.size == 2 * 2 * 7  # 09:30-10:00 partial + 6 full hours
    daily = resample(bars, "1D")
    assert daily.size == 4
    summary = daily_summary(bars)
    assert summary.size == 4
    assert {"TradeDate", "ReturnPct", "RangePct", "Bars"} <= set(summary.column_names)
    from deephaven.pandas import to_pandas

    frame = to_pandas(summary)
    assert (frame["Bars"] == 390).all()
    assert (frame["High"] >= frame["Low"]).all()
    norm = normalized(bars)
    nf = to_pandas(norm.sort(["Symbol", "Timestamp"]).first_by("Symbol"))
    assert (nf["PctChange"].abs() < 1e-9).all()


def test_every_chart_type_builds(runtime):
    from market_data_demo.charts import CHART_TYPES, build_charts
    from market_data_demo.derived import normalized, resample

    cfg, store, reader, inventory = runtime
    bars = resample(reader.read(DAYS[0], DAYS[1], ["AAPL", "MSFT"]).table, "5m")
    norm = normalized(bars)
    for kind in CHART_TYPES:
        for hide in (True, False):
            charts = build_charts(kind, bars, ["AAPL", "MSFT"], interval="5m", hide_gaps=hide, first_day=DAYS[0], normalized_table=norm)
            assert not charts.notes, (kind, hide, charts.notes)
            expected = 2 if kind in ("candlestick", "ohlc") else 1
            assert len(charts.figures) == expected, (kind, hide)


def test_dashboard_and_app_export(runtime, tree, monkeypatch):
    from market_data_demo.dashboard import build_dashboard

    cfg, store, reader, inventory = runtime
    element = build_dashboard(reader, inventory, cfg)
    assert element is not None

    # The app entrypoint end to end: exec like Application Mode does, into a fresh namespace.
    import market_data_demo
    from market_data_demo import app as app_module

    monkeypatch.setenv("MD_LOCAL_ROOT", str(tree))
    monkeypatch.setenv("MD_DEFAULT_SYMBOLS", "NVDA")
    monkeypatch.setenv("MD_DEFAULT_DAYS", "2")
    monkeypatch.setattr(market_data_demo, "_MARKET_DATA_RUNTIME", None, raising=False)
    namespace = {"__name__": "__main__", "__file__": app_module.__file__}
    with open(app_module.__file__, encoding="utf-8") as handle:
        exec(compile(handle.read(), app_module.__file__, "exec"), namespace)
    for name in ("md_bars", "md_daily_summary", "md_inventory_symbols", "md_inventory_days", "md_load", "md_chart", "market_data_dashboard"):
        assert name in namespace, name
    assert namespace["md_inventory_symbols"].size == 3
    assert namespace["md_bars"].size == 2 * 390  # NVDA, default 2 days
    loaded = namespace["md_load"]("AAPL", str(DAYS[0]), str(DAYS[1]), interval="15m")
    assert loaded.size == 2 * 26
    fig = namespace["md_chart"]("line", "AAPL,MSFT", DAYS[0], DAYS[4], interval="1h")
    assert fig is not None
    assert "source=local" in namespace["md_status"]()
    # A second exec reuses the runtime instead of re-scanning.
    second = {"__name__": "__main__", "__file__": app_module.__file__}
    with open(app_module.__file__, encoding="utf-8") as handle:
        exec(compile(handle.read(), app_module.__file__, "exec"), second)
    assert second["market_data_runtime"] is namespace["market_data_runtime"]
