# Basket OMS demo — Deephaven as an equities OMS front end

Can Deephaven carry the **front end of an order-management system**? This module answers
with a working demo: a **basket ticket** (build lines or paste them), a **basket list**
(one basket per row, live roll-ups), an **order blotter** for the selected basket with a
**right-click context menu** — *Modify… / Route ▸ venue / Split… / Cancel* — plus
execution and audit tapes, a ticking quote board, an order tree for split parents and a
fill-progress chart. A **mock venue** thread acks, fills, rejects and cancels so every
panel ticks while you work. Everything is mock data; nothing leaves the container.

```
 New basket ──▶ ticket (modal) ──Create──▶ OmsCore (pure python) ──▶ keyed input tables
 click basket ─▶ orders panel filters        │  validated transitions   oms_baskets / oms_orders
 right-click ──▶ Modify / Route / Split /     │  audit + executions      oms_events / oms_executions
                 Cancel  ──▶ dialog ─────────▶┘  roll-ups               oms_quotes ◀── mock venue
 select order ─▶ executions / events filter                             ▼
                                                        oms_baskets_view · oms_orders_view · oms_orders_tree
                                                        deephaven.ui dashboard: basket_oms_dashboard
```

Design: [`ANALYSIS.md`](ANALYSIS.md) (Python vs Java, popup vs inline, what `deephaven.ui`
can and cannot do, the data model). Plan and verification log: [`PLAN.md`](PLAN.md).
Binding contract (table / column / global / `OMS_*` names):
[`docs/13-basket-oms-demo.md`](../docs/13-basket-oms-demo.md).

---

## Quickstart

```bash
cd claude-code
bash basket-oms-demo/scripts/run_demo.sh          # podman compose up, wait for healthy, print URLs
open http://localhost:10000/iframe/widget/?name=basket_oms_dashboard
bash basket-oms-demo/scripts/run_demo.sh down     # tear down
```

The IDE is at <http://localhost:10000/ide> (Panels ▸ `basket_oms_dashboard`, or any of the
`oms_*` tables). One stock `ghcr.io/deephaven/server:42.4` container, `-Xmx2g`; set
`DH_XMX=1g` when another Deephaven stack is on the podman machine, `DH_PORT` to move the port.

**What you see** (1440×900):

| Panel | Contents |
|---|---|
| **Baskets** (top) | toolbar — *New basket*, *Add orders*, *Route basket ▾*, *Cancel basket*, the *Mock venue* switch, the *Trader* name — over `oms_baskets_view`: one basket per row with `Status`, `Orders`, `Live`, `Done`, `Qty`, `Filled`, `Filled %` databar, `Notional`. Click a row to select it; right-click for *Add orders…*, *Route N unrouted order(s) to ▸*, *Cancel basket*. |
| **Orders — \<basket\>** (bottom left) | the selected basket's rows of `oms_orders_view`: side and status colours, `Filled %` databar, limit / average / last prices, live `Bid` / `Ask` / `Last` from the quote board. **Right-click a row** for the order actions; select a row to filter the tapes to it. |
| **Executions / Events / Quotes / Order tree / Fill progress** (bottom right, tabs) | the execution tape and the audit tape for the selected order (else the basket), the ticking quote board, the split hierarchy (`tree` on `OrderId` / `ParentId`), a `Filled` vs `Leaves` bar chart per symbol. |

**The context menu.** Items are built per row from the order's state (doc 13 §4): a `NEW`
order offers all four actions, a working order *Modify…* and *Cancel*, a filled or
cancelled one nothing. *Route* is a submenu of the venues; *Modify…*, *Split…* and
*Cancel* open modal dialogs; every commit raises a toast and lands in `oms_events`.
Select the row first, then right-click: a right-click does select the cell, but the
selection reaches the server just after the menu is built, so a menu opened on an
unselected row shows the generic *Modify / Route / Split / Cancel selected order* items,
which resolve the order when clicked (verified on 42.4 / deephaven.ui 0.42).

**The ticket.** *New basket* opens a modal with a **line builder** (Symbol, Side, Qty,
Type, Limit, TIF, *Add line*) and a **paste box** (`SYMBOL SIDE QTY [MKT|LMT price]
[DAY|IOC|GTC]`, one per line, `#` comments) with a live parse preview and per-line
errors; *Create basket* is disabled until every line parses. *Route on create* routes the
whole basket straight away. The same ticket serves *Add orders…* for an existing basket.
`OMS_TICKET_PANEL=true` renders it inline in the right-hand stack instead of as a dialog.

**The mock venue** (switch in the toolbar, `oms_sim(...)` from the console): every
`OMS_SIM_INTERVAL_MS` it acks `ROUTED` orders (a few `DARK` ones are rejected), fills
working orders in round lots at the quote — a `LMT` order only when the market is at or
through its limit — gives `IOC` orders one shot and cancels the rest, and random-walks
the quotes around their reference prices.

---

## Console API (doc 13 §3)

```python
oms_status()                                                   # counts, venue state, dashboard
oms_new_basket("Demo", ["AAPL BUY 1000", "MSFT SELL 500 LMT 415.20 GTC"], strategy="VWAP", route="NYSE")
oms_add_orders("B-0006", "JPM BUY 300 IOC")
oms_route("O-000001", "ARCA");        oms_route_basket("B-0001", "ALGO-VWAP")
oms_modify("O-000001", qty=800, limit_px=189.75, tif="GTC")
oms_split("O-000003", slices=4);      oms_split("O-000004", qtys=[700, 500])
oms_cancel("O-000002");               oms_cancel_basket("B-0002")
oms_sim(False); oms_sim("step"); oms_sim(True); oms_sim()   # stop / one step / start / state
```

Illegal transitions raise `OmsError` with the same message the dialogs show. The raw
tables (`oms_baskets`, `oms_orders`, …) are the keyed / append-only input tables; the
`_view` tables and the tree are read-only projections (the `InputTable` attribute is
stripped so the grid never edits them behind the core's back).

---

## Configuration (`OMS_*`, doc 13 §6)

| Variable | Default | Meaning |
|---|---|---|
| `OMS_SEED` | `42` | seed for the mock baskets and the venue |
| `OMS_MOCK_BASKETS` | `5` | seeded baskets: the five story baskets first (staged, pairs, index-arb, done, split), random *Program N* baskets after; `0` starts empty |
| `OMS_SIM` | `true` | start the mock venue thread |
| `OMS_SIM_INTERVAL_MS` | `750` | venue step interval |
| `OMS_TRADER` | `trader1` | default `Trader` / `User` (editable in the toolbar per browser session) |
| `OMS_VENUES` | `NYSE,NASDAQ,ARCA,BATS,DARK,ALGO-VWAP,ALGO-TWAP` | routing targets |
| `OMS_TICKET_PANEL` | `false` | inline ticket instead of the modal |
| `DH_PORT` / `DH_XMX` / `DH_CONTAINER` | `10000` / `2g` / `oms-deephaven` | compose knobs |

---

## Tests

```bash
bash basket-oms-demo/run_tests.sh          # venv + pytest: core, parser, venue, mock data, dashboard helpers
./gradlew :basket-oms-demo:check           # the same, from Gradle
```

The suite is Deephaven-free and runs in well under a second. The embedded-server test
(`tests/test_deephaven_embedded.py`) drives the table bridge and the venue inside an
in-process Deephaven server and only runs with `OMS_DH_TEST=1` and `deephaven-server`
installed in the venv (on this arm64 host it needs the arm64 JDK 21 as `JAVA_HOME`, see
the market-data README).

---

## Layout

```
basket-oms-demo/
  ANALYSIS.md  PLAN.md  README.md
  build.gradle.kts  pyproject.toml  run_tests.sh
  scripts/run_demo.sh                         up | down | status | logs
  src/basket_oms_demo/
    core.py       baskets, orders, transitions, roll-ups, audit   (pure python)
    mockdata.py   symbol universe + the seeded story baskets      (pure python)
    venue.py      the mock venue: acks / fills / rejects / quotes (pure python)
    config.py     OMS_* variables                                 (pure python)
    tables.py     input tables + derived views                    (server)
    simulator.py  the venue thread                                (server)
    dashboard.py  the deephaven.ui dashboard                      (server)
    app.py        entrypoint + console API                        (server)
  tests/
docker/docker-compose.basket-oms.yml
docker/apps/basket-oms-demo/{basket-oms-demo.app, main.py}
```
