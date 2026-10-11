# Order Tree Reconciliation — Raptor → algo → order router

The order tree for a **moniker / symbol / side**, built in Deephaven from three systems'
order tables. A Raptor client order fans out to an algo parent and its slices, each
slice to an order-router parent and its venue orders. Every level is reconciled against
the level below, so you can see how many shares were **bought** and how many are **open**
in each system, and which hop a difference comes from.

```
RAPTOR client order ──► ALGO parent ──► ALGO child ×n ──► OR parent ──► OR child ×n (venue)
     Moniker              LinkId =         LinkId =         LinkId =       LinkId =
     OrderId / ClOrdID    Raptor OrderId   algo parent id   algo child id  OR parent id
                                                            (or Raptor ClOrdID for DMA)
```

The design, every formula and the reasoning behind each choice are in
[`docs/14-order-tree-reconciliation.md`](../docs/14-order-tree-reconciliation.md). This
file is the runbook.

---

## Quickstart

```bash
cd claude-code
bash order-tree-recon/scripts/run_demo.sh            # podman compose up -> wait for healthy -> URLs
open http://localhost:10000/iframe/widget/?name=order_tree_dashboard
bash order-tree-recon/scripts/run_demo.sh down
```

No broker: the app builds three mock tables (`raptor_orders`, `algo_orders`,
`router_orders`) in their own native shapes, maps them onto one node table with one
adapter per level, and builds the DAG. The DAG is incremental, so the same code runs over
ticking tables. The embedded test proves it with a live fill.

The same book as order-state messages (`state-samples/`: CSV and JSON Lines), read through
the field mapping:

```bash
OTR_SOURCE=state-files bash order-tree-recon/scripts/run_demo.sh
```

**Try:** type `du` in *Moniker*, press the `DUNE / MSFT / BUY` exposure row, and expand
the tree. The client order is canceled, but one venue order still has 100 shares open,
and the router parent above it is flagged red `CHILD_STILL_OPEN`. Or type `CEDAR`, pin
`JPM / BUY`, and read the ladder: the street executed 900 while every level above it
says 800.

| Panel | Contents |
|---|---|
| Find orders | Moniker / Symbol type-in boxes (prefix match), Side, *any order id from any system*, include unlinked, breaks only |
| Exposure by moniker / symbol / side | client vs street `CumQty` / `LeavesQty`, `NetUnbookedQty`, held, over-routed — press a row to pin the bucket |
| Level ladder | each level's totals, how they compare to the client order, and the per-edge gaps attributed to that level |
| Order tree | the native expand/collapse tree; `BreakKind`, quantities, `FillGap` and `RouteGap` on every node |
| Breaks in view | the red nodes of the trees in view |

From the IDE console:

```python
r = order_tree("DUNE", "MSFT", "BUY")   # {"tree", "flat", "ladder", "exposure", "breaks"}
r["ladder"]
find_tree("OR-00013.2")                 # a venue order id -> its whole tree, from the client order down
exposure(symbol="NVDA")                 # includes the unattributed orphan bucket
```

Every table is also a global: `otr_recon`, `otr_tree`, `otr_tree_flat`,
`otr_level_ladder`, `otr_exposure`, `otr_breaks`, `otr_break_summary`, `otr_nodes`,
`otr_id_index`, `otr_levels`.

---

## Using your own tables

Only [`src/order_tree_recon/sources.py`](src/order_tree_recon/sources.py) changes:

1. Replace `native_tables()` with your Raptor, algo and router tables. Anything that is
   a Deephaven table works: Kafka, JDBC (doc 07), parquet, or a remote URI (doc 10).
2. Edit the five `view`s in `adapters()` so each level produces
   `config.CANONICAL_COLUMNS` (`OrderId, AltId, LinkId, Moniker, Symbol, Side, OrderQty,
   CumQty, LeavesQty, AvgPx, Status, Destination`). The rules (role split, which id each
   system stores, one side coding, moniker at the root only) are in doc 14 section 3.
3. If your flow differs, edit `config.DEFAULT_LEVELS` (e.g. add a level for an internal
   crossing book). `parse_topology` validates it at startup.

---

## Using your own order-state messages

When each system sends its orders' states (one message per change, each the order's
complete state), map each system's field names in one JSON file. No code changes are
needed. Doc 14 section 12 has the details.

1. Put each system's messages in one folder, outside the repository: `.csv` with a
   header row, `.jsonl` with one JSON object per line, or `.json` as an array.
2. Copy [`state-samples/state_config.json`](state-samples/state_config.json) next to them
   and edit it. For each system you set:
   - its files;
   - which field is the order id (`order_id`) and, optionally, a second id (`alt_id`);
   - which of your fields fill `Symbol`, `Side`, `OrderQty`, `CumQty`, `LeavesQty`,
     `AvgPx`, `Status`, ... (`fields`);
   - how to tell parent from child (`when`) and which field holds the parent's id
     (`link`);
   - `sequence`, if messages can arrive out of order.
3. Dry-run the mapping on the host. No Deephaven or container is needed:

   ```bash
   PYTHONPATH=src python -m order_tree_recon.states_check /path/to/files
   ```

   It shows the columns it found, any mapped field that no file has, orders per level,
   how they linked, the breaks, and why the first unlinked orders did not link.
4. Run the dashboard on them:

   ```bash
   OTR_SOURCE=state-files OTR_STATE_HOST_DIR=/path/to/files bash order-tree-recon/scripts/run_demo.sh
   ```

For a live feed (Kafka JSON, or any append-only table with one row per message), call
`states_dag.build_from_states(...)` yourself. Doc 14 section 12.4 has a complete snippet.

---

## Copying the module into another project

The package needs only `deephaven` and `deephaven.ui`. Export it together with its
compose file, app folder, loader, `repos.env` / `scripts/repos.sh` and docs:

```bash
bash order-tree-recon/scripts/export_standalone.sh ~/work/order-tree
cd ~/work/order-tree && bash order-tree-recon/run_tests.sh && bash order-tree-recon/scripts/run_demo.sh
```

To use it in an existing Deephaven server instead, put `src/` on its `PYTHONPATH` and run
`from order_tree_recon import app; app.main(globals())`. Doc 14 section 13 has the
details.

---

## How to read the numbers

Per edge (a node against its **direct** children):

- `FillGap = CumQty − ΣchildCumQty`: negative means **unbooked fills** (the children
  executed more than this order shows), positive means **phantom fills**.
- `RouteGap = LeavesQty − ΣchildLeavesQty`: positive means **held** (open here, not routed
  down), negative means **over-routed** (more open below than above), or
  `CHILD_STILL_OPEN` when this order is already done.

Per bucket (`otr_exposure`), for every fully linked bucket:

```
ClientLeavesQty = HeldQty − OverRoutedQty + StreetLeavesQty
StreetCumQty − ClientCumQty (= NetUnbookedQty) = UnbookedQty − PhantomQty
```

The full break taxonomy (`ORPHAN`, `NO_LINK`, `TOO_DEEP`, `MISMATCH`, `OVERFILL`,
`UNBOOKED_FILL`, `PHANTOM_FILL`, `CHILD_STILL_OPEN`, `OVER_ROUTED`, `HELD`) is in doc 14
section 5.2.

---

## Configuration

Set on the `deephaven` service in
[`docker/docker-compose.order-tree.yml`](../docker/docker-compose.order-tree.yml). A
malformed value is a startup error.

| Variable | Default | Meaning |
|---|---|---|
| `OTR_QTY_TOL` | `1e-6` | absolute tolerance for quantity comparisons |
| `OTR_SEED` | `42` | mock data seed |
| `OTR_MOCK_FAMILIES` | `24` | random healthy families on top of the 11 catalog scenarios |
| `OTR_SOURCE` | `mock` | `mock` or `state-files` (order-state message files) |
| `OTR_STATE_HOST_DIR` | `order-tree-recon/state-samples` | the host folder with the files and `state_config.json`, mounted at `/otr-states` (`OTR_STATE_DIR`) |
| `OTR_STATE_CONFIG` | `/otr-states/state_config.json` | another mapping file (relative to `/otr-states`); the built-in default if neither exists |

`DH_PORT` (10000), `DH_XMX` (2g) and `DH_CONTAINER` (`otr-deephaven`) move the port, the
heap and the container name.

---

## Tests

```bash
bash run_tests.sh                                   # host suite: pure python, no Deephaven
./gradlew :order-tree-recon:pytest                  # the same, through gradle (wired into `check`)

pip install deephaven-server==42.4                  # ~600 MB, needs JDK 17+
OTR_DH_TEST=1 PYTHONPATH=src python -m pytest tests # + the embedded-engine suite
```

The host suite (110 tests) checks the python reference: every scenario's breaks, the edge
taxonomy, signed gaps, DMA linking via `AltId`, nested algo depth, `TOO_DEEP` and
cycles, depth-first `TreePath`, both identities on every complete bucket, the filter
builders and the mock book. It also checks the order-state layer: config validation, the
CSV / JSON readers, the fold (latest state, sequence order, id history), `states_check`,
the committed `state-samples/`, and the mock book as state messages reconciling exactly
like the table path, even when delivered out of order. The embedded suite asserts the Deephaven DAG matches the
reference **row for row** (every `otr_recon` column, every ladder and exposure row). It
also builds the tree, the API, the app and the dashboard, and turns the router table into
a keyed input table to show that one new venue fill re-derives `UNBOOKED_FILL` and the
bucket's `NetUnbookedQty` incrementally. For the order-state path, it asserts the
engine's fold equals the python fold row for row (shuffled feeds, typed columns) and the
tree equals the reference. It also shows that an appended state message ticks through to
a break, and builds the app with `OTR_SOURCE=state-files`.

---

## Troubleshooting

**Everything is `ORPHAN` at one level.** The adapter's `LinkId` is not the id the
upstream system indexes under. Check which id the downstream system stores (order id
vs `ClOrdID`), and put the other one in the upstream level's `AltId`.

**Order states: a whole level is `ORPHAN` or `NO_LINK`.** Run `states_check` on the folder.
`NO_LINK` means the level's `link` field is empty in the orders' latest state (often a
misspelled field name; `states_check` flags mapped fields that no file has). `ORPHAN`
means the field has a value that is not an id of the parent system (its `order_id`,
`alt_id` or `id_fields`). The first unlinked orders are printed with the value they
carried.

**Order states: a system shows 0 messages.** Its `files` globs matched nothing. The
startup banner and `states_check` both list what each glob matched. Globs are relative to
the folder.

**Every child is `MISMATCH`.** The systems code `Side` differently (FIX `1`/`2` vs
`BUY`/`SELL`). Decode in the adapter, as `RAPTOR_SIDE` does.

**A moniker search misses router orders.** It can't, as long as they are linked: the
search matches the root's moniker. Unlinked ones appear when *include unlinked* is on
(the default) and they match the symbol and side.

**No `otr_tree` panel.** The banner (`podman logs otr-deephaven | grep order-tree`)
says `tree panel: UNAVAILABLE` if the server rejected `Table.tree`. `otr_tree_flat`
shows the same trees as an indented table.
