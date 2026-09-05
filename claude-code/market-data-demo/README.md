# Market Data Demo — historical OHLC bars from parquet, charted in Deephaven

A Deephaven app that loads **per-minute OHLC bars** (open / high / low / close / volume) for
**one or more symbols** over a **UI-chosen period** from parquet files laid out as
`YYYY/MM/DD/<SYMBOL>.parquet` on **local disk or S3**, and renders them as **candlestick**,
**OHLC**, **line**, **area**, **normalized % change** or **volume** charts at a selectable bar
interval (1m … 1D). A deterministic **mock data generator** produces the parquet tree for
testing, and the whole thing runs under **podman** from one compose file — with an optional
MinIO service so the S3 path can be exercised without an AWS account.

```
 python -m market_data_demo generate ──► market-data-demo/data/YYYY/MM/DD/<SYMBOL>.parquet
                                                 │                       │
                                       bind mount /market-data     `upload` (boto3) ──► MinIO / S3
                                                 │                                        │
                                   MD_SOURCE=local                          MD_SOURCE=s3  │
                                                 ▼                                        ▼
                     Deephaven (md-deephaven :10000) ── LocalStore / S3Store list the day prefixes
                       BarReader: parquet.read(file[, S3Instructions]) per file → merge → sort
                       derived: resample(interval) · daily_summary · normalized
                       charts:  dx.candlestick / ohlc / line / area / bar
                       deephaven.ui dashboard: market_data_dashboard   +  md_* console API
```

The binding contract is [`docs/11-market-data-demo.md`](../docs/11-market-data-demo.md):
the layout, the parquet schema, the table and column names, the `MD_*` variables and the
CLI flags there are frozen the way docs 09/10 are for their modules.

---

## Quickstart (local parquet files)

```bash
cd claude-code

# 1. generate the mock universe (8 symbols, the last 30 calendar days, 1-minute bars)
#    -> market-data-demo/data/YYYY/MM/DD/<SYMBOL>.parquet   (~200 files, ~15 MB)
bash market-data-demo/scripts/generate_mock_data.sh
#    or a specific universe / period / seed:
bash market-data-demo/scripts/generate_mock_data.sh --symbols AAPL,MSFT,NVDA --start 2026-08-03 --end 2026-09-04 --seed 42

# 2. bring Deephaven up on it (builds the derived image once: server 42.4 + boto3)
podman compose -f docker/docker-compose.market-data.yml up -d --build

# 3. open the dashboard
open http://localhost:10000/ide          # Panels ▸ market_data_dashboard
```

Or all of that in one go: `bash market-data-demo/scripts/run_demo.sh` (it generates data only
if the data directory is empty, starts the stack, waits for the banner and prints the URLs;
`bash market-data-demo/scripts/run_demo.sh down` tears it down).

**What you see.** One dashboard:

| Panel | Contents |
|---|---|
| Market data — controls | a multi-select **Symbols** list (plus a text box to add tickers), a **Period** date-range picker with `1D / 5D / 1M / 3M / All` presets, the **Bar interval** (`1m 5m 15m 30m 1h 1D`), the **Chart** type, a **hide gaps** toggle (weekends and overnight are cut out of the x axis), **Reload**, and a status line naming the files that were read |
| Available symbols / days | the store inventory: per symbol its first/last day and file count; per day how many symbols |
| Chart | candlestick or OHLC: **one tab per selected symbol**; line / area / normalized / volume: **every symbol on one figure**, colored by `Symbol` |
| Bars | the loaded table at the chosen interval — sort, filter and export it like any Deephaven table |
| Daily summary | one row per symbol per trading day: OHLC, volume, `ReturnPct`, `RangePct` — **click a row to zoom the period to that day** |

Changing symbols or the period re-reads only the files that changed (per-file tables are
cached); changing the interval or the chart type re-plots without touching the store.

**Dashboard-only view (no IDE chrome)** — the same iframe endpoint the other apps use:

- <http://localhost:10000/iframe/widget/?name=market_data_dashboard> — the whole dashboard
- `http://localhost:10000/iframe/table/?name=md_bars` — the pre-loaded default bars

Every table is also a plain global, so the app degrades to individual panels if
`deephaven.ui` is ever missing. From the IDE console:

```python
md_load("AAPL, MSFT", "2026-08-03", "2026-08-07")             # 1-minute bars, one table
md_load(["NVDA"], "2026-08-03", "2026-09-04", interval="1h")  # rolled up to hourly bars
md_daily("AAPL", "2026-08-01", "2026-09-04")                  # per-day OHLC + return %
md_normalized("AAPL,MSFT,NVDA", "2026-08-03", "2026-09-04")   # PctChange since first close
md_chart("candlestick", "AAPL", "2026-09-01", "2026-09-04")   # a figure; also ohlc/line/area/normalized/volume
md_files("AAPL", "2026-09-01", "2026-09-04")                  # which files a query reads
md_symbols(); md_days(); md_status(); md_refresh()            # inventory; re-scan after new files land
```

---

## S3 (MinIO locally, or real S3)

The same files, served from an S3 bucket, with Deephaven reading them through its own S3
channel provider (`deephaven.experimental.s3.S3Instructions`):

```bash
cd claude-code
MD_SOURCE=s3 bash market-data-demo/scripts/run_demo.sh
```

which is short for

```bash
MD_SOURCE=s3 podman compose -f docker/docker-compose.market-data.yml --profile s3 up -d --build
market-data-demo/.venv/bin/python -m market_data_demo upload \
    --root market-data-demo/data --bucket market-data --prefix ohlc \
    --endpoint http://localhost:9000 --access-key minioadmin --secret-key minioadmin
podman restart md-deephaven          # re-scan the bucket (it was empty at first start)
```

The MinIO console is on <http://localhost:9001> (`minioadmin` / `minioadmin`); the bucket is
created by `upload` when missing. To point the app at **real S3**, set `MD_S3_BUCKET`,
`MD_S3_PREFIX`, `MD_S3_REGION`, an empty `MD_S3_ENDPOINT`, and either explicit
`MD_S3_ACCESS_KEY_ID` / `MD_S3_SECRET_ACCESS_KEY` or nothing (the default AWS credential
chain is used both by Deephaven and by boto3); `MD_S3_ANONYMOUS=true` for a public bucket.
`python -m market_data_demo upload` takes the same settings as flags to seed the bucket.

> **Why MinIO needs `MINIO_DOMAIN` and a `<bucket>.minio` alias.** Deephaven's reader is the
> AWS Java SDK, which addresses a bucket behind a custom endpoint *virtual-host* style: a
> read of `s3://market-data/ohlc/…` against `http://minio:9000` is sent to
> `http://market-data.minio:9000/ohlc/…` (verified against 42.4). The compose file gives
> the MinIO service that network alias and sets `MINIO_DOMAIN=minio` so it resolves the
> bucket from the `Host` header. boto3 (listing and upload) uses path style and is
> unaffected. Running Deephaven *outside* compose against a host-side MinIO? Use an IP
> endpoint (`MD_S3_ENDPOINT=http://127.0.0.1:9000`): the SDK falls back to path style for
> IP hosts.

---

## The data

### Layout

```
<root>/YYYY/MM/DD/<SYMBOL>.parquet             what the generator writes
<root>/YYYY/MM/DD/<SYMBOL>/<anything>.parquet   also read: a directory per symbol (Spark-style part files)
```

`<root>` is `MD_LOCAL_ROOT` (the `/market-data` bind mount in compose) or
`s3://MD_S3_BUCKET/MD_S3_PREFIX`. Listing is always per day prefix, so a three-month query
touches ~65 small directories and never scans the store. `_SUCCESS`, `_metadata`, `.crc`
and other non-matching names are ignored.

### Schema (one file = one symbol, one regular session, 390 rows)

| Column | Type | Meaning |
|---|---|---|
| `Timestamp` | timestamp[µs, UTC] → Deephaven `Instant` | bar start; 09:30 … 15:59 New York |
| `Symbol` | string | ticker (also in the path; the file wins if they differ) |
| `Open` `High` `Low` `Close` | double | `Low ≤ min(Open, Close)`, `High ≥ max(Open, Close)` |
| `Volume` | int64 | shares, U-shaped intraday profile |
| `VWAP` | double | within `[Low, High]` |
| `TradeCount` | int64 | trades in the bar |

Files without `Symbol`, `VWAP` or `TradeCount` are accepted (filled from the path / with
nulls); a file missing any of the other columns is reported in the status line and skipped.

### The mock generator

`python -m market_data_demo generate [--root R] [--symbols A,B] [--start D] [--end D] [--seed N] [--force]`

- **Deterministic per `(seed, symbol, day)`** — regenerating one day's file gives the
  identical bytes whatever else exists, so a tree can be topped up or a file repaired in
  isolation. Randomness is seeded from a SHA-256 of `seed|symbol|day`.
- **Continuous across days without replaying history** — a cheap daily walk from a fixed
  anchor (2020-01-01) fixes each day's open and close; the intraday path is a Brownian
  bridge between them, so tomorrow opens an overnight gap away from today's close.
- **Well-formed** — each bar opens at the previous close, wicks extend beyond the body,
  volume follows the open/close U-shape, sessions are exactly 390 one-minute bars,
  weekends are skipped (exchange holidays are not modelled).
- Default universe: `AAPL MSFT NVDA AMZN GOOGL META TSLA JPM`. Any other ticker gets a
  stable pseudo-random reference price, so `--symbols FOO,BAR` just works.
- Files are written to a temp name and renamed, so a reader never sees a partial file.
  Existing files are skipped unless `--force`.

`python -m market_data_demo list [--root R | --bucket B …] [--files]` prints the inventory
of either store.

---

## Configuration

All read once at app start, on the `deephaven` service in
[`docker/docker-compose.market-data.yml`](../docker/docker-compose.market-data.yml). A
malformed value is a **startup error**, never a silent fallback.

| Variable | Default | Meaning |
|---|---|---|
| `MD_SOURCE` | `local` | `local` or `s3` |
| `MD_LOCAL_ROOT` | `/market-data` | root of the local layout |
| `MD_S3_BUCKET` | — (required for s3) | bucket |
| `MD_S3_PREFIX` | `` | key prefix above `YYYY/MM/DD` (compose default `ohlc`) |
| `MD_S3_REGION` | `us-east-1` | region |
| `MD_S3_ENDPOINT` | `` | endpoint override (compose default `http://minio:9000`) |
| `MD_S3_ACCESS_KEY_ID` / `MD_S3_SECRET_ACCESS_KEY` | `` | explicit keys, set together; blank → default credential chain |
| `MD_S3_ANONYMOUS` | `false` | unsigned requests (public bucket) |
| `MD_S3_PATH_STYLE` | endpoint set | boto3 addressing style (listing / upload only) |
| `MD_DEFAULT_SYMBOLS` | `` | initial selection (compose default `AAPL,MSFT,NVDA`; blank → first 3 in the inventory) |
| `MD_DEFAULT_DAYS` | `5` | initial period, in most-recent available days |
| `MD_DEFAULT_INTERVAL` | `1m` | initial bar interval |
| `MD_DEFAULT_CHART` | `candlestick` | initial chart type |
| `MD_HIDE_GAPS` | `true` | cut weekends / overnight out of the x axis |
| `MD_CACHE_FILES` | `512` | per-file table cache (files are immutable) |
| `MD_MAX_FILES` | `2000` | refuse a single load larger than this |
| `MD_READ_THREADS` | `4` | parallel file reads |

The startup banner in `podman logs md-deephaven` prints the resolved source, the inventory
(symbols, days, files, span), what was pre-loaded, every exported global and whether the
dashboard came up.

---

## Tests

**Unit** — pure python, no Deephaven, no containers: the layout parser, the generator's
bar invariants (determinism, bridge endpoints, `Low ≤ body ≤ High`, U-shaped volume,
DST-correct session times, weekend skipping, idempotent files), both stores (the S3 one
against an in-memory fake client with forced pagination), the configuration validator,
the dashboard's pure helpers and the CLI.

```bash
bash run_tests.sh                          # standalone (creates .venv, installs pyarrow + pytest)
./gradlew :market-data-demo:pytest         # or through gradle (wired into `check` / `build`)
```

**Embedded engine** — `tests/test_deephaven_embedded.py` runs the real thing without
containers: it starts `deephaven_server` in-process, generates a tree, and asserts the
reader's merge and cache, resampling / daily-summary / normalized row counts, that every
chart type builds a figure, that the dashboard element builds, and that the app entrypoint
exports every global and is idempotent. It needs a JDK 17+ and the (large) server wheel:

```bash
.venv/bin/pip install "deephaven-server==42.4" deephaven-plugin-ui deephaven-plugin-plotly-express
MD_DH_TEST=1 bash run_tests.sh tests/test_deephaven_embedded.py
```

It is skipped (never fails) without `MD_DH_TEST=1` or without the wheel.

---

## Troubleshooting

**Empty inventory, banner says `0 symbols, 0 days`.** The data directory is empty or not
mounted: `ls market-data-demo/data/2026` on the host, and check the compose was run with
`-f docker/docker-compose.market-data.yml` from `claude-code/` so the relative bind mount
resolves. Generate with `bash market-data-demo/scripts/generate_mock_data.sh`, then
`md_refresh()` in the console (or restart the container).

**S3: banner shows the inventory but every load errors with `NoSuchKey` / connection
refused.** Listing (boto3, path style) works but reading (Deephaven, virtual-host style)
does not — see the MinIO note above. Check `podman exec md-deephaven getent hosts
market-data.minio` resolves, and that `MINIO_DOMAIN=minio` is set on the MinIO container.
Against real S3, check `MD_S3_ENDPOINT` is empty.

**S3: `ModuleNotFoundError: boto3`.** The stack was started against the stock image. Use
`up -d --build` (or `podman compose -f docker/docker-compose.market-data.yml build`) so the
derived image from `docker/deephaven-market-data.Dockerfile` is used.

**The chart shows big flat gaps between days.** `hide gaps` is off, or the period spans a
DST change (the gap bounds are computed from the first day of the period; the sliver at one
end is cosmetic). Toggle the checkbox, or shorten the period.

**`No tables or dashboard in the IDE`.** The app-mode script did not load:
`podman logs md-deephaven | grep -E '\[market-data-demo\]|\[market-data\]'`. A python
traceback means the entrypoint raised (a misconfigured `MD_*` variable prints its reason);
the server stays up, so fix it and `podman restart md-deephaven`.

**Port 10000 is taken.** The fix42-dashboard stack uses it too — run one or the other, or
`DH_PORT=10001 podman compose -f docker/docker-compose.market-data.yml up -d`.
