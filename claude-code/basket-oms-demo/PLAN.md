# Basket OMS demo — implementation plan

Step-by-step plan for the `basket-oms-demo` submodule, derived from
[`ANALYSIS.md`](ANALYSIS.md). Names (tables, columns, globals, env variables) are frozen
in [`docs/13-basket-oms-demo.md`](../docs/13-basket-oms-demo.md); a step that has to
deviate updates the contract in the same commit. Each step ends with a check that is
run before the next step starts. Status marks: `[ ]` open, `[x]` done, `[~]` done with
a noted change.

Working rules for this plan: one branch (`claude/basket-oms-demo`), one PR at the end;
the core is implemented and unit-tested **before** any Deephaven code; the dashboard is
verified in a real browser against the podman stack, not only through `pydeephaven`;
no more than one helper agent at a time (token budget).

---

## Step 0 — Skeleton and packaging

- [x] `basket-oms-demo/` with `pyproject.toml` (`basket-oms-demo` 0.1.0, no runtime
      deps; extra `test=[pytest]`), `build.gradle.kts` (base plugin, `pytest` task on
      `check`, same as `:market-data-demo`), `run_tests.sh`, `.gitignore`;
      `settings.gradle.kts` gains `include(":basket-oms-demo")`.
- [x] Package `src/basket_oms_demo/` with `__init__.py`, `config.py`.
- Check: `bash basket-oms-demo/run_tests.sh` creates the venv and runs an empty suite.

## Step 1 — Core model (`core.py`), pure Python

- [x] Dataclasses `Basket`, `Order`, `Execution`, `Event`; enums as string constants
      (`Side`, `OrdType`, `TIF`, `OrderStatus`, `BasketStatus`, `EventType`).
- [x] `OmsCore` with a `threading.Lock`, monotonic id counters (`B-0001`, `O-000001`,
      `E-000001`) and the mutations `create_basket`, `add_orders`, `route`, `modify`,
      `cancel`, `split`, `ack`, `reject`, `fill`, `route_basket`, `cancel_basket`;
      each returns the touched orders/baskets and appends events; each raises
      `OmsError` on an illegal transition with a message the dialog can show.
- [x] Pure helpers: `allowed_actions(order) -> set`, `split_quantities(qty, n)` (sizes
      differ by at most 1, sum exactly), `parse_basket_lines(text)` (`SYMBOL SIDE QTY
      [MKT|LMT px] [DAY|IOC|GTC]`, per-line errors), `basket_rollup(orders)` (status +
      counts + pct), `weighted_avg_px`.
- [x] A `Listener` protocol: the core calls `on_basket(b)`, `on_order(o)`,
      `on_execution(x)`, `on_event(e)` after every mutation — the Deephaven bridge
      (step 3) and the unit tests (a recording listener) plug in here.
- Check: `tests/test_core.py` — every transition, every rejection, split arithmetic,
      parser corner cases, roll-up status; runs on the host in < 1 s.

## Step 2 — Mock data and mock venue (`mockdata.py`, `venue.py`), pure Python

- [x] Symbol universe with reference prices (about 24 large caps), seeded `random`.
- [x] `seed_demo(core, n_baskets, seed)`: named baskets that tell a story — a
      *Tech rebalance* (STAGED, all NEW), a *Pairs* basket (one BUY / one SELL, WORKING),
      an *Index arb* basket partially filled, a *Closed* basket (all FILLED / CANCELLED),
      plus one with a split parent so the tree panel has something to show on first open.
- [x] `MockVenue(core, rng)` with a pure `step()`: for every ROUTED order → ack (WORKING);
      for every WORKING / PARTIAL order → with strategy-dependent probability a fill of
      a random slice at a price around the quote (LMT never through the limit; IOC
      cancels the remainder after one shot); a small reject probability for the `DARK`
      venue; quotes random-walk around the reference price. `step()` returns what it
      did so the tests can assert on it; the thread wrapper (step 3) just loops.
- Check: `tests/test_mockdata.py`, `tests/test_venue.py` — determinism for a seed,
      LMT fills never cross the limit, `Filled + Leaves == Qty` always, IOC semantics.

## Step 3 — Deephaven bridge (`tables.py`, `simulator.py`)

- [x] `TableBridge(core)` creates the five input tables (`oms_baskets`, `oms_orders`,
      `oms_events`, `oms_executions`, `oms_quotes`) with the contract's column
      definitions, implements the `Listener` protocol as upserts / appends, and builds
      the derived tables `oms_baskets_view`, `oms_orders_view`, `oms_orders_tree`.
- [x] Column types: ids and enums `string`, quantities `long`, prices `double`
      (`NULL_DOUBLE` for a market order's limit), times `Instant`.
- [x] `simulator.py`: a daemon thread calling `venue.step()` every
      `OMS_SIM_INTERVAL_MS` inside the captured execution context; `start()` / `stop()`;
      `oms_sim(True|False)` from the console.
- Check: `tests/test_deephaven_embedded.py` (runs only with `OMS_DH_TEST=1` and
      `deephaven_server` importable, as the market-data module does): create a basket,
      route, fill through the bridge, assert the view tables' rows.

## Step 4 — App wiring (`app.py`, `docker/apps/basket-oms-demo`, compose)

- [x] `app.py`: config → core → bridge → seed → simulator → console API
      (`oms_new_basket`, `oms_route`, `oms_modify`, `oms_cancel`, `oms_split`,
      `oms_route_basket`, `oms_cancel_basket`, `oms_sim`, `oms_status`) → dashboard;
      memoised on the package object so re-running the script in the console is safe.
- [x] `docker/apps/basket-oms-demo/{basket-oms-demo.app, main.py}` through the shared
      loader; `docker/docker-compose.basket-oms.yml` (project `basket-oms-demo`,
      container `oms-deephaven`, stock `ghcr.io/deephaven/server:42.4`, `DH_PORT`,
      `DH_XMX` default 2g, the `OMS_*` variables); `scripts/run_demo.sh` (up / down /
      status, waits for healthy, prints the URLs).
- Check: `bash basket-oms-demo/scripts/run_demo.sh` → the five tables and the views
      appear in the IDE; `oms_status()` prints counts.

## Step 5 — Dashboard, part 1: layout and master/detail (`dashboard.py`)

- [x] `build_dashboard(runtime)` returning `ui.dashboard(...)`, `None` without
      `deephaven.ui`. Layout: top row = **Baskets** panel (toolbar + `ui.table` of
      `oms_baskets_view`, single-row selection, `on_row_press` → `selected_basket`);
      bottom row = **Orders** panel (left, ~62 %) = `oms_orders_view.where(BasketId ==
      selected)` and a right-hand `ui.stack` with **Executions**, **Events**, **Quotes**,
      **Order tree**, **Fill progress** (a `dx.bar` of Filled vs Leaves by Symbol).
- [x] Formatting: Side colours, Status colours, `PctFilled` databar, `LimitPx` blank
      for MKT; column display names; hidden internal columns.
- [x] Executions / Events filter to the selected order when one is selected, else to
      the selected basket.
- Check (browser, 1440×900): clicking a basket switches the orders panel; selecting an
      order filters the executions; the mock venue's fills are visible ticking.

## Step 6 — Dashboard, part 2: context menu and dialogs

- [x] Selection tracking on the orders table (`on_selection_change`,
      `always_fetch_columns=True`) → `selected_orders` state.
- [~] **Live check first**: does a right-click on an unselected row select it? **Yes**,
      the grid selects the cell — but the selection event reaches the server *after*
      the menu items are resolved (see the log). Kept the `OrderId`-cell fallback and
      added generic *… selected order* items that resolve the order when clicked.
- [x] `context_menu` resolver: items built from `allowed_actions(order)`:
      *Modify…*, *Route ▸ (venues)*, *Split…*, *Cancel*, plus *Copy* -style info items
      (`description` shows the order summary). Actions set
      `pending = ("modify"|"split"|"cancel", order)` or call the core directly (route).
- [~] Dialogs as controlled `ui.dialog_trigger(type="modal", is_open=True)` mounted
      only while `pending` is set (a hidden trigger button): **Modify** (Qty, LimitPx,
      TIF; validation message from the core), **Split** (slice count *or* quantity
      list; preview of the children), **Cancel** (confirm with the order summary).
      Every commit → `ui.toast`. **Changed:** Modify and Split are `ui.form`s whose
      submit carries the field values (see the log on the input race).
- [x] Baskets context menu: *Route all unrouted ▸ venue*, *Cancel basket*, *Add
      orders…*.
- Check (browser): each action from the menu, including the failure paths (modify below
      filled, route a filled order is hidden, cancel a cancelled order is hidden).

## Step 7 — Dashboard, part 3: New basket ticket

- [~] **New basket** button in the Baskets toolbar → modal dialog: Name, Strategy
      picker, *Route on create* picker; a **line builder** form (Symbol, Side, Qty,
      Type, Limit, TIF, **Add line**) and the **paste box** with a live parse preview
      and per-line errors; **Create basket** (disabled until every line parses),
      **Cancel**. **Changed:** no tabs and no `ui.table` preview — two sibling
      `ui.form`s (forms cannot nest) and `ui.text` rows for the staged lines, which
      is lighter inside a dialog that re-renders per keystroke; the trader name is a
      toolbar field, not a ticket field.
- [x] The same component serves *Add orders…* from a basket's context menu (basket
      preselected, name read-only).
- [x] `OMS_TICKET_PANEL=true` renders the ticket inline in the right-hand stack instead
      (analysis §3 option B) — one flag.
- Check (browser): create a 5-line basket from the paste box, watch it appear on top,
      select it, route it from its context menu, watch fills arrive.

## Step 8 — Tests, docs, README, PR

- [x] `tests/test_dashboard_helpers.py` for the pure helpers in `dashboard.py`
      (row-payload extraction, menu item construction from a status, form coercion).
- [x] `basket-oms-demo/README.md` (quickstart, what you see, the console API, the
      `OMS_*` table), a section + tree entry in the top-level `README.md`,
      `docs/13-basket-oms-demo.md` cross-checked against the code.
- [x] `./gradlew :basket-oms-demo:check` green; the podman stack up → every flow
      verified in the browser at 1440×900 (log below); the PR description carries the
      findings.
- [x] Update this file's checkboxes, note every `[~]`; commit; push; open the PR.

---

## Verification log

Stack: `ghcr.io/deephaven/server:42.4`, `deephaven-plugin-ui` 0.42, podman 5.8 on the
6 GB machine, `DH_XMX=2g`; browser at 1440×900 on the `/iframe/widget/` view.
2026-09-16/17.

| Step | Result |
|---|---|
| 0–2 | 48 host tests green on the first run (core, parser, split, roll-up, seed story, venue rules). |
| 3 | First `up`: all five input tables, the three views and the tree built; the venue thread fills from second one, no errors in the log. `test_deephaven_embedded.py` (2 tests, `OMS_DH_TEST=1`, arm64 JDK 21) green. |
| 4 | `run_demo.sh up` waits on the health check and prints the URLs; `oms_status()` reports 5 baskets / 24 orders. |
| 5 | **Fixed:** `ui.panel` has no `width`; wrap in `ui.column(width=…)`. Basket row press switches the orders panel, the chart and the tapes; formats (side / status colours, `Filled %` databar, `HH:mm:ss` times) render. **Fixed:** the views inherited the input table's `InputTable` attribute, so the grids offered *Delete Selected Rows / Paste* and were editable — stripped with `without_attributes("InputTable")`. Blotter column order set with `move_columns_down` / `move_columns`. `ui.stack(active_item_index=0)` picks the Executions tab. |
| 6 | Right-click **does** select the cell, but the menu is resolved before the selection round trip lands: an unselected row gets the generic *… selected order* items (resolved on click), an already-selected row gets `Modify O-… / Route O-… to ▸ / Split O-… / Cancel O-…` built from `allowed_actions`. Custom items sort by the `order` key. Modify: **fixed** `number_field` snapping (`min_value + k·step`) and the commit race (`number_field` commits on blur, `text_field` changes are debounced ~250 ms on the client, so Save could read a stale value): dialogs are `ui.form`s, submit carries the values. Modify / Route (submenu) / Split (4 children, parent `SPLIT`) / Cancel (confirm) each verified: row updates in place, basket roll-up ticks, audit rows appear, toast shows. |
| 7 | Ticket: builder form adds a line even when *Add line* is clicked within the debounce window (the form submit carries the typed symbol); paste box parses live with per-line errors and disables *Create*; created basket appears on top, then routed from its context menu (*Route 3 unrouted order(s) to ▸ ARCA*) and filled by the venue. Order tree expands the split parent to its three children. |
| 8 | 63 host tests + 2 embedded; README, contract and top-level README updated. |

Open (not blocking, noted in the analysis): toasts were verified in the widget view;
`OMS_TICKET_PANEL=true` (inline ticket) is implemented but was not exercised in the
browser; the toolbar's "N unrouted" text is recomputed after the trader's own actions
only (the venue never changes that count).
