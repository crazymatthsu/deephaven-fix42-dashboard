# Market data around orders: as-of and window joins in SQL and in Deephaven (recipes)

A learning note, not a contract. It records what was **verified on 2026-09-06** against the
running market-data-demo stack (`ghcr.io/deephaven/server:42.4`, `deephaven-sql 42.4` on
Apache Calcite 1.42, deephaven.ui 0.40.2): how to answer *"what was the market doing around
the time of this order / execution?"* by joining the parquet OHLC bars (doc 11) to FIX order
or execution rows, first in SQL through `deephaven.experimental.sql.evaluate`, then with the
engine's native as-of join. Sibling: [`11-market-data-demo.md`](11-market-data-demo.md).

---

## 1. The question and the data

| Side | Table | Time column | Key |
|---|---|---|---|
| Orders / executions | `orders`, `executions` (fix42 app, doc 01 columns) or any table with these two columns | `TransactTime` — FIX tag 60, a UTC `Instant` | `Symbol` (tag 55) |
| Market data | `md_bars` / `md_load(...)` (doc 11) — one row per symbol per minute | `Timestamp` — bar **start**, UTC `Instant` | `Symbol` |

Both time columns are UTC instants, so no time-zone conversion is involved. "The bar at
the order time" is the bar whose start is the latest one **not after** `TransactTime`
(an order at 14:03:17Z belongs to the 14:03 bar). "Around the order" is a window of bars
either side of it.

---

## 2. Running SQL in the IDE

The IDE console is Python; SQL goes through the experimental adapter, which resolves table
names **from the console's variables**:

```python
from deephaven.experimental.sql import evaluate
result = evaluate("""SELECT ... FROM orders o JOIN bars b ON ...""")
```

### What the adapter accepts (verified)

| Works | Rejected (`UnsupportedSqlOperation` / `UnsupportedOperationException`) |
|---|---|
| `SELECT` / `WHERE` / `ORDER BY` | `ASOF JOIN ... MATCH_CONDITION ...` (Calcite syntax) |
| `GROUP BY` with `MIN/MAX/SUM/COUNT` | `INTERVAL` arithmetic (`TransactTime - INTERVAL '5' MINUTE`) |
| `JOIN ... ON` with an equality **and** comparison predicates (`AND b."Timestamp" <= o.TransactTime`) | window functions (`LAG(...) OVER (PARTITION BY ...)`) |
| `BETWEEN` on instants | `TIMESTAMP '2026-09-04 19:55:00'` literals |
| `WITH` common table expressions, sub-queries | |

Two consequences: **time bounds are computed in Python** (an `update_view` on the orders
table), and the as-of join is written as "latest bar not after the order" with a
`GROUP BY` + re-join, because neither `ASOF JOIN` nor `LAG/OVER` is available.

`Timestamp`, `Open`, `High`, `Low`, `Close`, `Volume` are SQL keywords for this parser and
must be **double-quoted**; `Symbol`, `VWAP`, `TransactTime`, `ClOrdID` need not be.

---

## 3. Setup (run once in the console)

Loads the bars for the order dates and builds a sample orders table. Replace the sample
with the real `orders` / `executions` table when it lives in the same server (§6); only
`Symbol` and `TransactTime` are required.

```python
from deephaven import new_table
from deephaven.column import string_col, datetime_col, double_col, long_col
from deephaven.time import to_j_instant
from deephaven.experimental.sql import evaluate

bars = md_load("AAPL,MSFT", "2026-09-04", "2026-09-04")        # 1-minute bars for the order dates

orders = new_table([
    string_col("ClOrdID", ["A1", "A2", "B1"]),
    string_col("Symbol", ["AAPL", "MSFT", "AAPL"]),
    string_col("Side", ["1", "2", "1"]),
    long_col("OrderQty", [100, 200, 300]),
    double_col("Price", [418.5, 1250.0, 419.0]),
    datetime_col("TransactTime", [to_j_instant("2026-09-04T14:03:17Z"),
                                  to_j_instant("2026-09-04T15:30:05Z"),
                                  to_j_instant("2026-09-04T19:59:40Z")]),
]).update_view(["WinStart = TransactTime - 5 * MINUTE", "WinEnd = TransactTime + 5 * MINUTE"])
```

---

## 4. The SQL recipes

### 4.1 The bar at each order's time (as-of join)

```python
at_order = evaluate("""
WITH last_bar AS (
  SELECT o.ClOrdID, MAX(b."Timestamp") AS BarTime
  FROM orders o JOIN bars b ON o.Symbol = b.Symbol AND b."Timestamp" <= o.TransactTime
  GROUP BY o.ClOrdID
)
SELECT o.ClOrdID, o.Symbol, o.Side, o.OrderQty, o.Price, o.TransactTime,
       b."Timestamp" AS BarTime, b."Open", b."High", b."Low", b."Close", b."VWAP", b."Volume"
FROM orders o
JOIN last_bar l ON l.ClOrdID = o.ClOrdID
JOIN bars b ON b.Symbol = o.Symbol AND b."Timestamp" = l.BarTime
ORDER BY o.TransactTime
""")
```

Result on the sample (3 rows):

```
ClOrdID Symbol Side OrderQty   Price   TransactTime           BarTime               Open     High     Low     Close    VWAP       Volume
A1      AAPL   1    100        418.5   2026-09-04 14:03:17Z   2026-09-04 14:03:00Z  415.77   416.04   415.06  415.07   415.4850   5634
A2      MSFT   2    200        1250.0  2026-09-04 15:30:05Z   2026-09-04 15:30:00Z  1246.06  1246.50  1246.04 1246.11  1246.1775  1862
B1      AAPL   1    300        419.0   2026-09-04 19:59:40Z   2026-09-04 19:59:00Z  419.02   419.07   418.68  419.04   418.9525   8822
```

The sub-query form (no `WITH`) works identically if you prefer it:

```sql
SELECT ..., b."Close"
FROM orders o
JOIN bars b ON o.Symbol = b.Symbol AND b."Timestamp" <= o.TransactTime
JOIN (SELECT o2.ClOrdID, MAX(b2."Timestamp") AS LastBar
      FROM orders o2 JOIN bars b2 ON o2.Symbol = b2.Symbol AND b2."Timestamp" <= o2.TransactTime
      GROUP BY o2.ClOrdID) m
  ON m.ClOrdID = o.ClOrdID AND m.LastBar = b."Timestamp"
```

### 4.2 Every bar within ±5 minutes of each order

```python
around_order = evaluate("""
SELECT o.ClOrdID, o.Symbol, o.TransactTime, b."Timestamp" AS BarTime,
       b."Open", b."High", b."Low", b."Close", b."VWAP", b."Volume"
FROM orders o
JOIN bars b ON o.Symbol = b.Symbol AND b."Timestamp" BETWEEN o.WinStart AND o.WinEnd
ORDER BY o.ClOrdID, b."Timestamp"
""")
```

25 rows on the sample: 10 bars for A1 and A2 (13:59 .. 14:08 around a 14:03:17 order), 5
for B1 because its window runs past the 16:00 close. `>= o.WinStart AND <= o.WinEnd` is
an equivalent spelling.

### 4.3 One summary row per order over the window

```python
window_summary = evaluate("""
SELECT o.ClOrdID, o.Symbol, o.Price,
       MIN(b."Low") AS WinLow, MAX(b."High") AS WinHigh, SUM(b."Volume") AS WinVolume, COUNT(*) AS Bars
FROM orders o
JOIN bars b ON o.Symbol = b.Symbol AND b."Timestamp" >= o.WinStart AND b."Timestamp" <= o.WinEnd
GROUP BY o.ClOrdID, o.Symbol, o.Price
""")
```

```
ClOrdID Symbol Price   WinLow   WinHigh  WinVolume Bars
A1      AAPL   418.5   414.65   416.08   65252     10
A2      MSFT   1250.0  1245.48  1248.34  21156     10
B1      AAPL   419.0   418.50   419.23   39494     5
```

---

## 5. The native equivalents (what to use beyond a few thousand orders)

Every SQL join above is a cross join on `Symbol` plus a filter under the hood, so cost
grows with `orders × bars-per-symbol`. The engine's purpose-built joins are O(n log n),
work on a **ticking** left table (a live `executions` table), and read more clearly:

```python
# 4.1 -- the bar at or before the order: as-of join (`aj`); `raj` gives the bar at or after
at_order = orders.aj(bars, on=["Symbol", "TransactTime >= Timestamp"],
                     joins=["BarTime = Timestamp", "Open", "High", "Low", "Close", "VWAP", "Volume"])

# 4.2 -- the bars in a window: join on Symbol, then filter on the precomputed bounds
around_order = orders.join(bars, on=["Symbol"], joins=["BarTime = Timestamp", "Open", "High", "Low", "Close", "Volume"]) \
                     .where(["BarTime >= WinStart", "BarTime <= WinEnd"]) \
                     .sort(["ClOrdID", "BarTime"])

# 4.3 -- the summary: aggregate the window rows
from deephaven import agg
window_summary = around_order.agg_by(
    [agg.min_("WinLow = Low"), agg.max_("WinHigh = High"), agg.sum_("WinVolume = Volume"), agg.count_("Bars")],
    by=["ClOrdID", "Symbol", "Price"])
```

`aj` on the sample returned exactly the three rows of §4.1. For "the bar the order is in"
at a coarser interval, resample first (`md_load(..., interval="5m")`) and join to that.

---

## 6. Getting orders and executions next to the bars

The FIX tables live in the fix42-dashboard server (globals `orders`, `executions`,
`executions_blink`), the bars in the market-data server: two compose stacks, two JVMs.
Either

* pull the FIX tables into the market-data server with the remote-table mechanism of
  doc 10 — `deephaven.uri.resolve("dh+plain://fix42-deephaven:10000/scope/executions")`
  — which needs the two stacks on one podman network and heaps that fit the 6 GB machine
  together (`DH_XMX=1g` for the market-data side); or
* run the market-data app inside the fix42 stack: the application directory
  (`-Ddeephaven.application.dir`) may hold several `.app` descriptors, so mounting
  `docker/apps/market-data-demo` beside `docker/apps/fix42-dashboard` and adding the
  `MD_*` variables puts `md_load` and `executions` in one console.

The joins are the same either way; with a live `executions` table, `aj` keeps the joined
columns updating as executions arrive.

---

## 7. Ideas for the dashboard (not built)

A "Query" panel in `market_data_dashboard` (deephaven.ui `text_area` + Run button +
`ui.table` result + error line) backed by `evaluate` covers §4; a structured
"around the order" form (pick the orders table, the time and symbol columns, a window and
an interval) would run §5 in Python and zoom the existing chart to the selected order's
window with a marker at `TransactTime`. Doc 11 §8 describes the row-click mechanism that
would be reused.
