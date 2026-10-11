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

The same book as FIX order states, one file per AMPS SOW topic (`fix-samples/`), read through
the SOW mirror and the FIX mapping:

```bash
OTR_SOURCE=fix-files bash order-tree-recon/scripts/run_demo.sh
OTR_SOURCE=amps OTR_AMPS_BROKER=local bash order-tree-recon/scripts/run_demo.sh   # a local AMPS (AMPS_IMAGE), loaded with them
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

## Using your AMPS SOW topics (FIX order states)

When each system publishes its orders' latest state as FIX messages into AMPS SOW topics,
list the topics and tags in one JSON file. No code changes are needed. Doc 14 section 12
has the details: deletes (`oof`), reconnects, the config reference.

1. Copy [`fix-samples/fix_config.json`](fix-samples/fix_config.json) to a folder of your own
   and edit it. Per system:
   - `topics`: one or more SOW topics;
   - the `when` rule that tells parent from child;
   - the `link_tag` holding the parent's id: usually `11` across systems, a custom tag
     inside one.

   Standard FIX tags (37, 11, 55, 54, 38, 14, 151, 6, 39, ...) are the defaults.
2. Dry-run the mapping against your AMPS. It runs a one-off `sow` query: no subscription,
   no Deephaven, only `pip install amps-python-client`:

   ```bash
   PYTHONPATH=src python -m order_tree_recon.fixstate_check --amps tcp://amps-host:9007/amps/fix --config /path/to/fix_config.json
   ```

   It shows each topic's record count, any mapped tag that no message carries, orders per
   level, how they linked, the breaks, and why the first unlinked orders did not link.
3. Run the dashboard on the live topics:

   ```bash
   OTR_SOURCE=amps OTR_AMPS_URI=tcp://amps-host:9007/amps/fix OTR_FIX_HOST_DIR=/path/to/folder \
     bash order-tree-recon/scripts/run_demo.sh
   ```

   This builds the server image with the AMPS client (`docker/deephaven-amps.Dockerfile`,
   pip index from `repos.env`) and runs one `sow_and_subscribe` per topic. The server comes
   up even while AMPS is unreachable. In the console, `amps_source.status()` shows each
   topic's snapshot epoch and record counts, and `fix_<system>_sow` is the live SOW mirror.

Without AMPS, the same messages from files (SOW dumps, one FIX message per line) go through
the same mirror and fold: `OTR_SOURCE=fix-files OTR_FIX_HOST_DIR=/path/to/files`, and
`fixstate_check --dir`.

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
| `OTR_SOURCE` | `mock` | `mock`, `amps` (FIX order states from AMPS SOW topics) or `fix-files` |
| `OTR_FIX_HOST_DIR` | `order-tree-recon/fix-samples` | the host folder with `fix_config.json` (and, for `fix-files`, the message files), mounted at `/otr-fix` |
| `OTR_FIX_CONFIG` | `/otr-fix/fix_config.json` | another mapping file (relative to `/otr-fix`); the built-in default if neither exists |
| `OTR_AMPS_URI` | — | `amps`: the AMPS URI(s), comma separated for an HA pair |
| `OTR_AMPS_CLIENT_NAME` | `dh-order-tree` | unique per server (a duplicate logon displaces the other) |
| `OTR_AMPS_BROKER` | — | `local`: also start a local AMPS from `AMPS_IMAGE` and publish the samples into it |

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

The host suite (132 tests) checks the python reference: every scenario's breaks, the edge
taxonomy, signed gaps, DMA linking via `AltId`, nested algo depth, `TOO_DEEP` and
cycles, depth-first `TreePath`, both identities on every complete bucket, the filter
builders and the mock book. It also checks the FIX / AMPS layer: the parser, the tag
mapping, the SOW mirror (`oof`, reconnect snapshots), the fold, the id history,
`fixstate_check` (files and a stubbed AMPS `sow` query), the committed `fix-samples/`, the
AMPS source's subscribe calls and message classification, and the mock book as FIX order
states reconciling exactly like the table path. The embedded suite asserts the Deephaven DAG matches the
reference **row for row** (every `otr_recon` column, every ladder and exposure row). It
also builds the tree, the API, the app and the dashboard, and turns the router table into
a keyed input table to show that one new venue fill re-derives `UNBOOKED_FILL` and the
bucket's `NetUnbookedQty` incrementally. For the FIX / AMPS path, it asserts:
- the engine's SOW mirror and fold equal the python twins;
- the tree equals the reference;
- real `AMPS.Message` objects fed through a stub client tick the tree live (a fill, an
  `oof`, a reconnect snapshot);
- the app builds with `OTR_SOURCE=fix-files` and with `OTR_SOURCE=amps` while AMPS is
  unreachable.

No real AMPS broker was available to test against.

---

## Troubleshooting

**Everything is `ORPHAN` at one level.** The adapter's `LinkId` is not the id the
upstream system indexes under. Check which id the downstream system stores (order id
vs `ClOrdID`), and put the other one in the upstream level's `AltId`.

**AMPS: a whole level is `ORPHAN` or `NO_LINK`.** Run `fixstate_check --amps URI`.
- `NO_LINK` means the level's `link_tag` is not in the orders' latest state. The check
  flags any mapped tag that no message carries.
- `ORPHAN` means the tag has a value that is not an id the parent system reports (its
  `id_tags`, 37 / 11 / 41).

The first unlinked orders are printed with the value they carried.

**AMPS: the tables stay empty.** Check the log with `run_demo.sh logs`:
- `AMPS connect failed (...)` repeats every 5 s until AMPS answers;
- each topic logs `snapshot N complete, M records` once its `sow_and_subscribe` snapshot is
  in;
- `amps_source.status()` shows per-topic counts.

A topic not in `fix_config.json` is never subscribed.

**AMPS: orders that were deleted are still shown.** They are not, once the topic has sent
`oof` (the subscription asks for it) or a reconnect snapshot has completed. A record that
is only *filtered out* by your `filter` arrives as `oof` too.

**Every child is `MISMATCH`.** The systems code `Side` differently (FIX `1`/`2` vs
`BUY`/`SELL`). Decode in the adapter, as `RAPTOR_SIDE` does.

**A moniker search misses router orders.** It can't, as long as they are linked: the
search matches the root's moniker. Unlinked ones appear when *include unlinked* is on
(the default) and they match the symbol and side.

**No `otr_tree` panel.** The banner (`podman logs otr-deephaven | grep order-tree`)
says `tree panel: UNAVAILABLE` if the server rejected `Table.tree`. `otr_tree_flat`
shows the same trees as an indented table.
