# Market Data Demo (contract)

Design for the `market-data-demo` submodule: load **historical per-minute OHLC bars** for
one or more symbols over a UI-chosen period from **parquet files** stored either on
**local disk** or on **S3**, laid out as `YYYY/MM/DD/<SYMBOL>.parquet`, and show them in a
Deephaven dashboard as **candlestick** or other **time-series** charts. A **mock data
generator** produces the parquet tree locally for testing; the stack runs under **podman**.

This doc is **binding** for the submodule the way docs 09/10 are for theirs: the layout,
the parquet schema, the table / column names, the `MD_*` variables and the CLI flags below
are frozen; a deviation must update this doc in the same change. The user-facing guide is
[`market-data-demo/README.md`](../market-data-demo/README.md).

---

## 1. The problem, restated

| Requirement | Where it lands |
|---|---|
| OHLC per minute, by one or more symbols | §2 layout, §6 `BarReader`, §8 symbol multi-select |
| choose a time period from the UI | §8 date-range picker + presets, §9 `md_load(start, end)` |
| candlestick or other time-series charts | §7 chart registry: candlestick, ohlc, line, area, normalized, volume |
| parquet on local disk **or** S3, selectable | §4 `LocalStore` / `S3Store`, §5 `MD_SOURCE` |
| `YYYY/MM/DD/<symbol>` parquet layout | §2 |
| generate mock parquet files locally | §3 `python -m market_data_demo generate` |
| run Deephaven with podman | §11 `docker/docker-compose.market-data.yml` (+ MinIO for S3) |

The historical data is **static**: nothing here ticks. That is the design's main
simplification relative to the other apps in this repo — every table is a static
Deephaven table built on demand from immutable files, so there is no listener, no
publisher, no replay concern; a UI change just builds a new small DAG and lets the old one
be collected.

---

## 2. Layout and schema

```
<root>/YYYY/MM/DD/<SYMBOL>.parquet              canonical (what the generator writes)
<root>/YYYY/MM/DD/<SYMBOL>/<file>.parquet       accepted (directory per symbol, e.g. Spark part files)
```

`<root>` is a local directory (`MD_LOCAL_ROOT`) or `s3://<MD_S3_BUCKET>/<MD_S3_PREFIX>`.
The date comes **first** so that a period query lists **one small prefix per day** instead
of scanning the store — on disk and on S3 alike (§4). Anything under a day prefix that does
not parse as one of the two shapes (`_SUCCESS`, `_metadata`, `.crc`, `README`) is ignored.

Parquet schema, one file = one symbol × one regular session (390 rows):

| Column | Parquet type | Deephaven type | Contract |
|---|---|---|---|
| `Timestamp` | INT64 TIMESTAMP(MICROS, adjustedToUTC) | `Instant` | bar **start**; 09:30–15:59 New York, one per minute |
| `Symbol` | UTF8 | `String` | upper-case ticker; the path-derived symbol is used only when the column is absent |
| `Open`, `High`, `Low`, `Close` | DOUBLE | `double` | `Low ≤ min(Open, Close) ≤ max(Open, Close) ≤ High` |
| `Volume` | INT64 | `long` | ≥ 1 |
| `VWAP` | DOUBLE | `double` | in `[Low, High]`; optional on read (null-filled) |
| `TradeCount` | INT64 | `long` | ≥ 1; optional on read (null-filled) |

Column order in every table the module produces: `Timestamp, Symbol, Open, High, Low,
Close, Volume, VWAP, TradeCount` (`BAR_COLUMNS`). Symbols match `[A-Za-z0-9][A-Za-z0-9._-]{0,31}`
and are upper-cased everywhere (`normalize_symbol`).

---

## 3. Mock data generator (`market_data_demo.mockgen`, CLI `generate`)

```
python -m market_data_demo generate [--root R] [--symbols A,B,...] [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                                    [--seed N] [--force] [--quiet]
```

Defaults: root `market-data-demo/data` (or `MD_LOCAL_ROOT`), the 8-symbol universe
`AAPL MSFT NVDA AMZN GOOGL META TSLA JPM`, end = yesterday, start = end − 30 days, seed 42.

### 3.1 Properties (each pinned by a unit test)

1. **Deterministic per `(seed, symbol, day)`.** Every random stream is a `random.Random`
   seeded from `sha256("seed|symbol|day|stream")` — never python's salted `hash`. A day's
   file does not depend on which other days exist, so regenerating one file yields the
   identical bytes and a tree can be topped up incrementally (existing files are skipped
   unless `--force`).
2. **Continuous across days without replaying history.** A *daily* walk from the anchor
   `2020-01-01` (weekday steps: `close = open·e^r`, `r ~ N(0.0003, 0.012)`; `next open =
   close·e^g`, `g ~ N(0, 0.004)`) fixes each day's open and close in ~260 iterations per
   year. The *intraday* path is a **Brownian bridge** in log space between those two
   endpoints with per-minute noise `N(0, 0.0006)`, so consecutive days chain and a
   multi-day chart reads as one instrument.
3. **Well-formed bars.** `Open[k] = Close[k−1]`; wicks are `max(Open, Close)·(1+|N(0,4bp)|)`
   / `min(...)·(1−|N(0,4bp)|)`, rounded to cents and clamped so the body stays inside;
   volume is `base · U(k) · e^{N(0,0.35)}` with a U-shaped profile (heavy open/close, light
   lunch) and `base = 4e8 / reference price / 390`; `VWAP = (O+H+L+C)/4` clamped to
   `[Low, High]`.
4. **Session and calendar.** 390 bars from 09:30 New York; the UTC offset is computed from
   the US DST rule (second Sunday of March → first Sunday of November) so the host needs no
   tz database. Weekends are skipped; **exchange holidays are not modelled** (a Labor Day
   file exists — acceptable for mock data, and the reader does not care).
5. **Reference prices.** Known symbols use the table above; any other ticker gets a stable
   pseudo-random price in `[20, 500)` derived from its name, so arbitrary `--symbols` work.
6. **Atomic writes.** Each file is written to `<name>.tmp` and `os.replace`d into place.

### 3.2 Other CLI commands

- `upload --root R --bucket B [--prefix P] [--endpoint URL] [--region] [--access-key/--secret-key | --anonymous] [--start/--end/--symbols]`
  — copy a local tree into S3 key for key (`create_bucket` if `head_bucket` fails).
- `list [--root R | --bucket B ...] [--start/--end/--symbols] [--files]` — print the inventory.

---

## 4. Stores (`market_data_demo.store`)

Both stores implement one interface and hand back `ParquetRef(day, symbol, path)` where
`path` is what `deephaven.parquet.read` accepts — an absolute local path or an `s3://` URI
— so the reader never knows which store it is talking to.

| Method | `LocalStore` | `S3Store` |
|---|---|---|
| `list_files(start, end, symbols=None)` | `os.scandir(<root>/YYYY/MM/DD)` per day | `list_objects_v2(Prefix=<prefix>/YYYY/MM/DD/)` per day, paginated |
| `available_days()` | walk `YYYY/MM/DD` directories | three-level `Delimiter="/"` common-prefix listing |
| `available_symbols(start, end)` | union of `list_files` | union of `list_files` |
| `describe()` | `local: <root>` | `s3: s3://bucket/prefix` |

`S3Store` additionally has `ensure_bucket()` and `upload_tree(local, ...)` for the CLI.

### 4.1 Clients

`S3Store` takes a **boto3-shaped client** (`list_objects_v2`, `put_object`, `head_bucket`,
`create_bucket`) that is injected or built lazily by `boto3_client_factory(...)`. The unit
suite runs it against an in-memory fake with forced pagination; boto3 is an optional
extra (`pip install -e ".[s3]"`) and is **never imported for `MD_SOURCE=local`**.

### 4.2 Two S3 clients, deliberately

- **Listing / uploading: boto3**, path-style addressing when an endpoint override is set.
- **Reading: Deephaven's own S3 channel provider** (`deephaven.experimental.s3.S3Instructions`
  → the AWS Java SDK), because it streams parquet column chunks straight into the engine.

The Java SDK addresses a bucket behind a custom endpoint **virtual-host style**: probed on
42.4, `endpoint_override="http://minio-probe:9556"` produced a request to
`Host: market-data.minio-probe:9556`, path `/ohlc/2026/09/04/AAPL.parquet`; an **IP**
endpoint (`http://127.0.0.1:9555`) produced path style. `S3Instructions` exposes no
force-path-style switch. Consequences, all in the compose file (§11): the MinIO service
carries the network alias `<bucket>.minio` and `MINIO_DOMAIN=minio`; outside compose, use
an IP endpoint.

---

## 5. Configuration (`market_data_demo.config`)

Read once at startup; a malformed value raises `ValueError` (startup error, never a silent
fallback). Every variable has a default that makes the **local** demo run with none set.

| Variable | Default | Validation |
|---|---|---|
| `MD_SOURCE` | `local` | `local` \| `s3` |
| `MD_LOCAL_ROOT` | `/market-data` | non-blank |
| `MD_S3_BUCKET` | — | required for `s3`; no `/` |
| `MD_S3_PREFIX` | `` | slashes stripped |
| `MD_S3_REGION` | `us-east-1` | |
| `MD_S3_ENDPOINT` | `` | `http(s)://` when set |
| `MD_S3_ACCESS_KEY_ID`, `MD_S3_SECRET_ACCESS_KEY` | `` | set together or not at all; blank → default credential chain (both clients) |
| `MD_S3_ANONYMOUS` | `false` | not with explicit keys |
| `MD_S3_PATH_STYLE` | `true` iff endpoint set | boto3 only |
| `MD_DEFAULT_SYMBOLS` | `` | valid symbols; blank → first 3 of the inventory |
| `MD_DEFAULT_DAYS` | `5` | ≥ 1 (most-recent available days) |
| `MD_DEFAULT_INTERVAL` | `1m` | key of `INTERVALS` |
| `MD_DEFAULT_CHART` | `candlestick` | key of `CHART_TYPES` |
| `MD_HIDE_GAPS` | `true` | bool |
| `MD_CACHE_FILES` | `512` | ≥ 0 |
| `MD_MAX_FILES` | `2000` | ≥ 1 |
| `MD_READ_THREADS` | `4` | ≥ 1 |

`Config.describe()` never prints the secret key.

---

## 6. Reading and derived tables (`reader`, `derived`)

**`BarReader.read(start, end, symbols) -> LoadResult`** — `store.list_files` → one
`parquet.read(path, special_instructions=S3Instructions|None)` per file (thread pool of
`MD_READ_THREADS`) → `_conform` (add `Symbol` from the path / null `VWAP`,`TradeCount` if
absent; fail the file if any other column is missing) → `merge` → `sort(Symbol, Timestamp)`.
Per-file tables are cached by path in an LRU of `MD_CACHE_FILES` (files are immutable). The
method **never raises**: listing failures, over-`MD_MAX_FILES` requests and per-file read
errors land in `LoadResult.errors` (first five, then a count) and `status()` renders them
for the UI. `missing_symbols` names requested symbols with no file in the period. A load
with no files returns `empty_bars()` — a zero-row table with the canonical schema.

**`resample(bars, interval)`** — `1m` returns the input; otherwise
`update_view(Bin = lowerBin(Timestamp, <nanos>L))` then `agg_by` per `(Symbol, Bin)`:
`first(Timestamp, Open)`, `max(High)`, `min(Low)`, `last(Close)`, `sum(Volume, TradeCount)`,
`weighted_avg(Volume, VWAP)`, `count(Bars)`. The bar's `Timestamp` is the **first minute in
the bin**, so a `1D` bar is stamped 09:30 New York rather than midnight UTC. Output columns:
`BAR_COLUMNS + Bars`.

| `INTERVALS` | `1m` | `5m` | `15m` | `30m` | `1h` | `1D` |
|---|---|---|---|---|---|---|
| seconds | 60 | 300 | 900 | 1800 | 3600 | 86400 |

**`daily_summary(bars)`** — `TradeDate = toLocalDate(Timestamp, timeZone("America/New_York"))`,
then per `(Symbol, TradeDate)`: `Open` first, `High` max, `Low` min, `Close` last,
`Volume`/`TradeCount` sums, `Bars` count, `ReturnPct = (Close/Open − 1)·100`,
`RangePct = (High − Low)/Open·100`.

**`normalized(bars)`** — `natural_join` each row to its symbol's first close in the table
(`BaseClose`), `PctChange = (Close/BaseClose − 1)·100` — the "normalized % change" chart.

---

## 7. Charts (`market_data_demo.charts`)

| `CHART_TYPES` key | Figure | Symbols |
|---|---|---|
| `candlestick` | `dx.candlestick(x=Timestamp, open/high/low/close)` | **one figure per symbol** (no `by` on financial plots in plotly-express 0.20) |
| `ohlc` | `dx.ohlc(...)` | one figure per symbol |
| `line` | `dx.line(y=Close, by=Symbol)` | one figure |
| `area` | `dx.area(y=Close, by=Symbol)` | one figure |
| `normalized` | `dx.line(normalized, y=PctChange, by=Symbol)` | one figure |
| `volume` | `dx.bar(y=Volume, by=Symbol)` | one figure |

`build_charts(kind, bars, symbols, interval, hide_gaps, first_day, normalized_table) ->
ChartSet(kind, figures=[(title, figure)], notes)`. `hide_gaps` applies plotly
`rangebreaks` through `unsafe_update_figure`: `["sat", "mon"]` and the overnight hours
`[session_close, session_open]` in UTC, using the first day's DST offset (`gap_rangebreaks`).
Every figure is attempted with its extras and retried plain if the plugin rejects them;
what was dropped is recorded in `notes` and shown in the status line.

---

## 8. Dashboard (`market_data_demo.dashboard`, global `market_data_dashboard`)

```
+------------------------------------------------------+---------------------------+
| Symbols [multi-select] [+add]  Period [range] 1D 5D 1M 3M All | Available symbols     |
| Interval v  Chart v  [x] hide gaps  Reload  Clear   | Available days              |
| status: N file(s) for A .. B; symbols: ...           |                           |
+------------------------------------------------------+---------------------------+
| Chart: candlestick/OHLC -> one tab per symbol; line/area/normalized/volume -> one |
+----------------------------------------------+---------------------------------+
| Bars (resampled)                             | Daily summary (click -> that day)|
+----------------------------------------------+---------------------------------+
```

State is four scalars — the symbol tuple, `(start, end)`, `interval`, `chart` — plus the
`hide gaps` flag and a reload counter. Data flow, all `ui.use_memo`:

| memo | deps | what |
|---|---|---|
| `result` | `(symbols, start, end, reload)` | `reader.read(...)` — the only thing that touches the store |
| `resampled` | `+ interval` | `resample(result.table, interval)` |
| `summary` | load key | `daily_summary(result.table)` |
| `norm_table` | `+ interval, chart` | `normalized(resampled)` only when the chart is `normalized` |
| `charts` | `+ interval, chart, hide_gaps` | `build_charts(...)` |

So changing the chart type or interval never re-reads files; changing symbols or the period
re-reads only uncached ones. Initial state: `MD_DEFAULT_SYMBOLS` ∩ inventory (else the first
three symbols), the last `MD_DEFAULT_DAYS` available days, `MD_DEFAULT_INTERVAL`,
`MD_DEFAULT_CHART`. Presets are in *available* days (`1D`=1, `5D`=5, `1M`=21, `3M`=63,
`All`). Clicking a daily-summary row sets the period to that `TradeDate`.

Version tolerance as in docs 09/10: `deephaven.ui` imported lazily, `build_dashboard`
returns `None` without it (the app then exports only tables and functions); every control
is built inside `_first`/`_safe` fallback chains (`list_view` → `checkbox_group`;
`date_range_picker` → two `date_picker`s; `picker` → `radio_group`; `tabs` → stacked
figures); `on_row_press` accepts every known payload shape; `to_date` coerces the Java
`LocalDate`/`Instant` values the pickers deliver. UI callbacks never raise.

---

## 9. Exported globals

| Global | Kind | Contents |
|---|---|---|
| `market_data_dashboard` | `ui.dashboard` | §8 |
| `md_bars` | table | the initial selection's bars (so the no-`deephaven.ui` fallback has data) |
| `md_daily_summary` | table | `daily_summary(md_bars)` |
| `md_inventory_symbols` | table | `Symbol, FirstDay, LastDay, Days, Files` |
| `md_inventory_days` | table | `Day, Symbols, SymbolList` |
| `market_data_runtime` | object | the wired `Runtime` (store, reader, inventory, tables, api) |
| `md_load(symbols=None, start=None, end=None, interval="1m")` | fn → table | bars (all symbols when `None`; the inventory span when a bound is `None`) |
| `md_daily(symbols, start, end)` | fn → table | daily summary |
| `md_normalized(symbols, start, end, interval)` | fn → table | with `PctChange` |
| `md_chart(kind, symbols, start, end, interval, hide_gaps)` | fn → figure | first figure of `build_charts` |
| `md_files(symbols, start, end)` | fn → table | `Day, Symbol, Path` of the files a query reads |
| `md_symbols()`, `md_days()` | fn → table | the inventory tables |
| `md_refresh()` | fn → table | re-scan the store, drop the file cache, reload defaults |
| `md_status()` | fn → str | source + inventory summary |

`app.py` follows the repo's app-mode pattern: `sys.path` bootstrap, a plain `Runtime`
class (no dataclass — app mode may exec with an unregistered `__name__`), `main()`
memoized on the package object so a re-exec reuses the runtime, `export(globals(), rt)`,
and a startup banner (`Market Data Demo -- ready`).

---

## 10. Testing

**Unit (host python, `run_tests.sh`, wired into `./gradlew build`)** — `tests/test_layout.py`
(paths, both shapes, junk rejection, `to_date` on Java-like values), `test_mockgen.py`
(§3.1 properties, DST table, parquet schema and idempotent bytes), `test_store.py` (local
tree with both shapes and junk; S3 fake with pagination, prefixes, upload round-trip;
inventory roll-up), `test_config.py` (defaults, s3, every startup error, secret masking),
`test_dashboard_helpers.py` (presets, selection/range coercion, intervals, chart registry,
rangebreaks), `test_cli.py` (generate → list round trip).

**Embedded engine (`MD_DH_TEST=1`, `deephaven-server==42.4` installed)** —
`tests/test_deephaven_embedded.py` starts the server in-process and asserts: reader merge
(6 files → 2340 rows), `Instant` timestamps at 13:30Z, cache hits, resample row counts
(`5m`→78/day, `1h`→7/day, `1D`→1/day), daily summary and normalized invariants, every
chart type × hide-gaps builds, the dashboard element builds, and the app entrypoint exports
every §9 global and is idempotent on a second exec. Verified on this contract's
implementation against 42.4 with deephaven.ui 0.42.0 / plotly-express 0.20.0.

---

## 11. Deployment (`docker/docker-compose.market-data.yml`)

A separate compose project (`name: market-data-demo`) with its own containers and
network; it shares host port 10000 with the fix42-dashboard stack (override with
`DH_PORT`).

| Service | Image | Role |
|---|---|---|
| `deephaven` (`md-deephaven`) | `localhost/fix42-deephaven-market-data:42.4` built from `docker/deephaven-market-data.Dockerfile` = `ghcr.io/deephaven/server:42.4` + `boto3` | the app; mounts `market-data-demo/src` at `/md-scripts`, the data dir (`MD_DATA_DIR`, default `market-data-demo/data`) at `/market-data`, `apps/_lib` and `apps/market-data-demo` |
| `minio` (`md-minio`, profile `s3`) | `minio/minio:RELEASE.2025-04-22T22-12-26Z` | S3 API on 9000, console on 9001; `MINIO_DOMAIN=minio`, alias `<bucket>.minio`; healthcheck `mc ready local`; named volume |

Why a derived image: boto3 for listing (§4.2); the stock image has the reader but not
boto3, and an `exec pip install` does not survive `down`. deephaven.ui and plotly-express
are already in the base image.

`market-data-demo/scripts/run_demo.sh [down]` sequences: generate if empty → `up -d --build`
(`--profile s3` for `MD_SOURCE=s3`) → (s3) wait for MinIO, `upload`, restart Deephaven so it
re-scans → wait for the banner → print URLs. `scripts/generate_mock_data.sh` is the
generator through the module venv (also `./gradlew :market-data-demo:generateMockData`).

---

## 12. Module layout

```
market-data-demo/
  build.gradle.kts              base plugin; `pytest` task wired into check; generateMockData task
  pyproject.toml                market-data-demo 0.1.0; deps pyarrow; extras s3=[boto3], test=[pytest]
  run_tests.sh                  venv + pytest (same pattern as the other python modules)
  scripts/generate_mock_data.sh, scripts/run_demo.sh
  src/market_data_demo/
    layout.py    store.py    config.py    mockgen.py    cli.py    __main__.py       (host + server)
    reader.py    derived.py  charts.py    dashboard.py  query_api.py  app.py         (server)
  tests/                        §10
  data/                         generated, git-ignored
docker/deephaven-market-data.Dockerfile
docker/docker-compose.market-data.yml
docker/apps/market-data-demo/{market-data-demo.app, main.py}
```

Sibling docs: [`09-multi-oms-blotter.md`](09-multi-oms-blotter.md) and
[`10-deephaven-remote-uri.md`](10-deephaven-remote-uri.md) for the app-mode / compose /
version-tolerance conventions this module reuses unchanged.
