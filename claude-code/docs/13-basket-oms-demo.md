# Basket OMS demo (contract)

Design contract for the `basket-oms-demo` submodule: a Deephaven front end for an
**equities basket order-management system** — basket entry ticket, basket list, order
blotter with a **right-click context menu** (modify / cancel / route / split), execution
and audit tapes, a ticking quote board and a mock venue that fills orders — all on
**mock data**, to test whether Deephaven can carry an OMS UI.

This doc is **binding** the way docs 09/10/11 are for their modules: the table, column,
global and `OMS_*` names below are frozen; a change must update this doc in the same
commit. The reasoning (Python vs Java, popup vs inline, the API facts this relies on) is
[`basket-oms-demo/ANALYSIS.md`](../basket-oms-demo/ANALYSIS.md); the step list is
[`basket-oms-demo/PLAN.md`](../basket-oms-demo/PLAN.md); the user guide is
[`basket-oms-demo/README.md`](../basket-oms-demo/README.md).

---

## 1. Decisions

| Question | Answer | Where argued |
|---|---|---|
| Python or Java | **Python**: `deephaven.ui` is the only Deephaven-native interactive component layer; the order core is pure Python with no Deephaven imports | analysis §1 |
| Truth | The in-process `OmsCore` (validated transitions, ids, roll-ups); the input tables are its live projection | analysis §4 |
| Tables | keyed input tables for baskets / orders / quotes, append-only input tables for events / executions | analysis §4 |
| New basket | modal dialog from a toolbar button (option A); `OMS_TICKET_PANEL=true` renders the same ticket inline | analysis §3 |
| Order actions | context menu on the blotter; Modify / Split / Cancel open modal dialogs, Route is a venue submenu | analysis §2, §5 |

---

## 2. Tables and columns (frozen)

All globals are prefixed `oms_`. Times are `Instant` (UTC); quantities `long`; prices
`double`, `NULL_DOUBLE` when not applicable; ids and enums `String`.

### `oms_baskets` — keyed input table, key `BasketId`

| Column | Values |
|---|---|
| `BasketId` | `B-0001` … |
| `Name` | free text |
| `Strategy` | `DMA` `VWAP` `TWAP` `POV` |
| `Trader` | who created it |
| `Status` | `STAGED` `WORKING` `DONE` `CANCELLED` (roll-up, §4) |
| `Created` `Updated` | Instant |
| `Note` | free text |

### `oms_orders` — keyed input table, key `OrderId`

Column *order* in `oms_orders_view` is a display choice (trading columns first,
bookkeeping last); the names and types are what is frozen.

| Column | Values |
|---|---|
| `OrderId` | `O-000001` … |
| `BasketId` | owning basket |
| `ParentId` | the split parent's `OrderId`, else null |
| `Symbol` | ticker |
| `Side` | `BUY` `SELL` `SHORT` |
| `Qty` | order quantity (after modifies) |
| `OrdType` | `MKT` `LMT` |
| `LimitPx` | limit price; `NULL_DOUBLE` for `MKT` |
| `TIF` | `DAY` `IOC` `GTC` |
| `Venue` | null while unrouted, else one of `OMS_VENUES` |
| `Status` | `NEW` `ROUTED` `WORKING` `PARTIAL` `FILLED` `CANCELLED` `REJECTED` `SPLIT` |
| `Filled` `Leaves` | `Filled + Leaves == Qty` for live orders; `Leaves == 0` when terminal or `SPLIT` |
| `AvgPx` | volume-weighted, `NULL_DOUBLE` until the first fill |
| `LastPx` `LastQty` | last execution |
| `Created` `Updated` | Instant |
| `Trader` | who owns it |
| `Note` | last action's detail (reject reason, modify summary) |

### `oms_events` — append-only input table (the audit tape)

`Seq` (long), `Time`, `Type` (`CREATE` `ROUTE` `ACK` `MODIFY` `CANCEL` `SPLIT` `FILL`
`REJECT` `BASKET`), `OrderId` (null for basket-level events), `BasketId`, `Detail`,
`User`.

### `oms_executions` — append-only input table

`ExecId` (`X-000001` …), `Time`, `OrderId`, `BasketId`, `Symbol`, `Side`, `LastQty`,
`LastPx`, `Venue`.

### `oms_quotes` — keyed input table, key `Symbol`

`Symbol`, `Bid`, `Ask`, `Last`, `RefPx`, `ChgPct`, `Updated`.

### Derived (built once, all ticking)

| Global | Definition |
|---|---|
| `oms_baskets_view` | `oms_baskets` natural-joined to the per-basket aggregate of `oms_orders`: `Orders`, `Live` (NEW/ROUTED/WORKING/PARTIAL), `Done` (FILLED/CANCELLED/REJECTED), `Qty`, `Filled`, `Leaves`, `PctFilled` (0–100), `Notional` (Σ Qty × RefPx), `FilledNotional` (Σ Filled × AvgPx) |
| `oms_orders_view` | `oms_orders` natural-joined to `oms_quotes` on `Symbol` (`Bid`, `Ask`, `Last`, `RefPx`) plus `PctFilled`, `Notional` |
| `oms_orders_tree` | `tree(oms_orders, id_col="OrderId", parent_col="ParentId")` |
| `basket_oms_dashboard` | the `deephaven.ui` dashboard (§5) |

---

## 3. Console API (frozen signatures)

```python
oms_new_basket(name, lines, strategy="DMA", trader=None, route=None)  # lines: str or list; -> BasketId
oms_add_orders(basket_id, lines)                                      # -> [OrderId]
oms_route(order_id, venue)            oms_route_basket(basket_id, venue)
oms_modify(order_id, qty=None, limit_px=None, tif=None)
oms_cancel(order_id)                  oms_cancel_basket(basket_id)
oms_split(order_id, slices=None, qtys=None)                           # exactly one of the two -> [OrderId]
oms_sim(enabled=None)                 # True/False starts/stops the mock venue; None returns the state
oms_status()                          # prints basket / order / execution counts, sim state
```

Line syntax (ticket paste box and `lines` above): `SYMBOL SIDE QTY [MKT|LMT price] [DAY|IOC|GTC]`,
case-insensitive, `#` comments, blank lines ignored; e.g. `AAPL BUY 1000 LMT 189.50 DAY`,
`msft sell 500`.

---

## 4. Transitions (the core enforces these; the menu shows only what is allowed)

| Action | Allowed when | Result |
|---|---|---|
| route | `NEW` | `ROUTED`, `Venue` set; the venue acks to `WORKING` |
| modify | `NEW` `ROUTED` `WORKING` `PARTIAL`; new `Qty >= Filled`; `LimitPx` only for `LMT` | in place; `Leaves` recomputed; `Note` = summary |
| cancel | not `FILLED` `CANCELLED` `REJECTED` `SPLIT` | `CANCELLED`, `Leaves = 0` |
| split | `NEW` (unrouted) with `Qty >= slices >= 2` | parent `SPLIT` (`Leaves = 0`), children `NEW` with `ParentId`, sizes differ by ≤ 1 |
| ack / fill / reject | venue only | `WORKING` / `PARTIAL` → `FILLED` / `REJECTED` |

Basket roll-up over the basket's leaf orders (`SPLIT` parents excluded): `STAGED`
(every leaf `NEW`, or no orders), `DONE` (every leaf terminal, ≥ 1 fill), `CANCELLED`
(every leaf terminal, no fill), `WORKING` otherwise — something is in flight, or some
orders are still to be routed while others have finished.

---

## 5. Dashboard (`basket_oms_dashboard`)

```
+------------------------------------------------------------------------------------+
| Baskets   [New basket] [Route basket ▾] [Cancel basket]   sim ● / trader           |
|  BasketId Name Strategy Status Orders Live Filled% Qty Filled Notional Trader …    |
+-------------------------------------------------+----------------------------------+
| Orders — <basket>  (right-click a row)          | Executions | Events | Quotes |   |
|  OrderId Symbol Side Qty Filled Leaves Type Px  | Order tree | Fill progress       |
|  TIF Venue Status AvgPx Bid Ask Last …          |                                  |
+-------------------------------------------------+----------------------------------+
```

- Basket row press → the orders panel filters to that basket; order selection → the
  executions / events tabs filter to that order (else to the basket).
- Orders context menu: `Modify…`, `Route ▸ <venues>`, `Split…`, `Cancel`, built per
  row from §4. Baskets context menu: `Route all unrouted ▸ <venues>`, `Cancel basket`,
  `Add orders…`.
- Dialogs are modal `ui.form`s (the submit event carries the typed values); every
  commit raises a toast; every action lands in `oms_events`. The `_view` tables and the
  tree are read-only projections: the `InputTable` attribute a derived table inherits
  is stripped, so the grid never edits `oms_orders` behind the core's back.
- A menu opened on a row that was not selected beforehand shows generic *Modify /
  Route / Split / Cancel selected order* items that resolve the order when clicked (the
  right-click selects the cell, but that selection reaches the server after the menu is
  built); a selected row gets the status-specific items.

---

## 6. Configuration (`OMS_*`, read once at startup)

| Variable | Default | Meaning |
|---|---|---|
| `OMS_SEED` | `42` | seed for the mock baskets and the venue |
| `OMS_MOCK_BASKETS` | `5` | number of seeded baskets (0 = start empty) |
| `OMS_SIM` | `true` | start the mock venue thread |
| `OMS_SIM_INTERVAL_MS` | `750` | venue step interval |
| `OMS_TRADER` | `trader1` | default `Trader` / `User` |
| `OMS_VENUES` | `NYSE,NASDAQ,ARCA,BATS,DARK,ALGO-VWAP,ALGO-TWAP` | routing targets |
| `OMS_TICKET_PANEL` | `false` | render the new-basket ticket inline instead of as a dialog |
| `DH_PORT` / `DH_XMX` / `DH_CONTAINER` | `10000` / `2g` / `oms-deephaven` | compose knobs |

---

## 7. Module layout

```
basket-oms-demo/
  ANALYSIS.md  PLAN.md  README.md
  build.gradle.kts  pyproject.toml  run_tests.sh  .gitignore
  scripts/run_demo.sh
  src/basket_oms_demo/
    config.py  core.py  mockdata.py  venue.py            (host + server, no deephaven imports)
    tables.py  simulator.py  dashboard.py  app.py         (server)
  tests/  test_core.py  test_mockdata.py  test_venue.py  test_dashboard_helpers.py
          test_deephaven_embedded.py (OMS_DH_TEST=1 only)
docker/docker-compose.basket-oms.yml
docker/apps/basket-oms-demo/{basket-oms-demo.app, main.py}
```
