# Multi-server Deephaven — remote-URI leaves and collector

The third app pair in this repo, and the first stack with **more than one Deephaven server**. The
doc 09 multi-OMS order flow is spread over **leaf** servers (`DH1..DHn`) that fold the OMS hub
tapes from **AMPS**, plus a **collector** server that holds only a subset of the leaves' data —
acquired through Deephaven's remote-table mechanisms — re-links the cross-hub families, marks open
quantity against a market-data table, and answers: *given a source OMS, a client account and a
symbol, show every hop upstream → downstream with the latest CumQty, LeavesQty and notional
exposure.*

```
 generator --multi-oms --amps-uri ──► AMPS (rx-amps :29007)   journalled fix topics fix42.oms-a … fix42.oms-c
                                          │ bookmark_subscribe(EPOCH)                │
                                   DH1 (rx-dh1 :10011)                        DH2 (rx-dh2 :10012)
                                   hubs: OMS-A                                hubs: OMS-B-parent, OMS-B-child, OMS-C
                                   fix42cache fold per hub (multi_oms.pipeline, unchanged)
                                   exports: rx_orders  rx_id_index  rx_exposure  rx_leaf_stats
                                          └──── dh+plain://dhN:10000/scope/rx_*  (Barrage subscriptions) ────┘
                                                        collector (rx-collector :10010)
                                     orders_all = merge(rx_orders_*) → doc 09 linking + per-edge recon
                                     → market_data_latest → orders_marked → exposure_by_level / exposure_by_source
                                     remote query back to the owning leaf: remote_executions("OMS-A|A-0001")
```

Every leaf runs the unchanged `fix42cache` state machine (one per hub, through `multi_oms.pipeline`)
and keeps its own caches; the collector never folds a FIX message. What crosses the wire is a
projection (17 of 37 columns per order), the id index, per-hub aggregates and a one-row health
table per leaf — executions, events and raw messages stay on the leaves and are fetched on demand.

The binding contract is [`docs/10-deephaven-remote-uri.md`](../docs/10-deephaven-remote-uri.md):
table names, column names, env vars and CLI flags there are frozen, and it carries the
orchestration analysis — how to shard 400 million FIX messages across such a fleet, what it costs
in fold throughput and memory, and what the collector can afford to hold.

---

## Quickstart

```bash
cd claude-code

# 1. build the derived image (server + amps-python-client) and bring the stack up:
#    rx-amps (:29007), rx-dh1 (:10011), rx-dh2 (:10012), rx-collector (:10010)
podman compose -f docker/docker-compose.remote-uri.yml up -d --build

# 2. publish the four correlated drop-copy tapes to AMPS (12 families, fan-out up to 3)
./gradlew :fix-mock-generator:run \
  --args="--multi-oms --amps-uri tcp://localhost:29007/amps/fix --seed 42 --orders 12 --children 3 --rate 200"

# 3. open the collector
open http://localhost:10010/ide     # Panels ▸ remote_uri_dashboard
```

`--amps-uri` makes the generator publish to AMPS instead of Kafka: same topics
(`fix42.oms-a`, `fix42.oms-b-parent`, `fix42.oms-b-child`, `fix42.oms-c`), same scenarios, same
`--emit-expected` oracle. It cannot be combined with an explicit `--bootstrap-servers`.

**What you see.** Pick a source OMS, an account and a symbol (blank = any) and press *Find*:

| Panel | Contents |
|---|---|
| Totals (root level) | the headline: a one-row `exposure_for(...)` table — orders, CumQty, LeavesQty, ExecNotional, OpenNotional, TotalNotional, SignedExposure |
| Families upstream → downstream | every hop of every matching family (`orders_marked`, sorted `RootKey, Depth, Oms, OrderKey`), with each hop's own quantities, `LinkState`, `BreakKind` and the `Leaf` that holds it |
| Totals by level | `exposure_by_level` — the same sums per hub level, so you see where the flow went |
| Executions of selected hop | a **remote query**: click a hop and its executions are pulled from the leaf that owns it (the panel title names the leaf) |
| Market data (latest) | `market_data_latest` — the simulated quotes the open quantity is marked against |
| Fleet | one row per leaf: orders held, messages folded, buffer pending, heap used |
| Per-hub totals by leaf | `exposure_by_leaf` — the aggregate each leaf exports over *all* its orders, no order row on the wire |

Each leaf is a normal Deephaven server too: `http://localhost:10011/ide` shows DH1's own
`oms_orders_latest`, `oms_executions`, `rx_orders` (OMS-A rows only) and `rx_leaf_stats`.

From the collector console:

```python
find_exposure("OMS-A", "ACC-1", "META")      # every hop of every family rooted at OMS-A / ACC-1 / META
exposure_for("OMS-A", "ACC-1", "META")       # the root-level totals (one row per source/account/symbol)
family_totals("OMS-A", "", "")               # per-level sums for everything rooted at OMS-A
remote_executions("OMS-A|A-0001")            # static table pulled from DH1 by a remote query
remote_live_executions("OMS-B-child|BC-0001-1")   # the same, as a live subscription
snapshot_leaf("DH2")                          # one-shot copy of DH2's rx_leaf_stats
reconnect()                                   # re-resolve every leaf and rebuild the DAG
```

---

## Remote mechanisms (the point of the demo)

All three run inside the collector's python; the leaves use the stack's anonymous auth.

| Mechanism | API | Where it is used |
|---|---|---|
| **Remote subscription** (push, live) | `deephaven.uri.resolve("dh+plain://dh1:10000/scope/rx_orders")`, or `barrage_session(host, port).subscribe(b"s/rx_orders")` with `REMOTEURI_RESOLVER=barrage` | the `rx_*_<leaf>` tables everything is built on |
| **Remote snapshot** (pull, one-shot) | `barrage_session(host, port).snapshot(b"s/rx_leaf_stats")` | `snapshot_leaf(name)` |
| **Remote query** (pull, parameterised) | the collector runs `rx_q_<n> = oms_executions.where(...)` on the owning leaf's console through the same Java client, then snapshots (or subscribes to) `s/rx_q_<n>` | `remote_executions`, `remote_live_executions` |

Try them by hand in the collector console:

```python
from deephaven.uri import resolve
resolve("dh+plain://dh1:10000/scope/rx_orders").size()

from deephaven.barrage import barrage_session
s = barrage_session(host="dh2", port=10000)
s.snapshot(b"s/rx_leaf_stats")
```

A remote table lives under the collector's own update graph, so `merge`, joins and aggregations
treat it like any local table — and when the remote side goes away, every dependent fails with it.
`reconnect()` re-resolves all leaves and rebuilds the DAG (the documented v1 failure model; doc 10
§2.7 describes the `TablePublisher` bridge that would make the collector immune).

---

## Exposure semantics

On `orders_marked` (doc 10 §7):

| Column | Definition |
|---|---|
| `ExecNotional` | `AvgPx × CumQty` — doc 09's `Notional`, null-safe |
| `MarkPx` | market `Mid` for the symbol, else the order's limit `Price` |
| `OpenNotional` | `LeavesQty × MarkPx` — what can still execute |
| `TotalNotional` | `ExecNotional + OpenNotional` — the order's notional exposure |
| `SignedExposure` | `+TotalNotional` for `BUY`, `−TotalNotional` otherwise |

`exposure_by_level` sums these per `(RootOms, RootAccount, RootSymbol, Oms, HubDepth)`;
`exposure_by_source` keeps only the root level (`Depth == 0`) — **those are the totals**. Summing
across hubs would count the same economic flow once per hop (doc 09's rule); the per-level table
is there to show the flow, not to be added up. `exposure_by_leaf` is the per-hub aggregate each
leaf exports over *all* its orders, joined with the leaf name — totals that never needed an order
row to travel.

The market data is simulated inside the collector: a seeded random walk over the generator's
symbols (`REMOTEURI_MD_SYMBOLS`), refreshed every second as a bounded snapshot table; symbols seen
in orders but not in the universe are added with their first limit price.

---

## Configuration

Read once at app start; a violation is a **startup error**, never a silent fallback.

Both roles:

| Variable | Default | Meaning |
|---|---|---|
| `REMOTEURI_ROLE` | *(required)* | `leaf` or `collector` |
| `REMOTEURI_HUBS` | doc 09's four hubs | the full topology JSON, validated like `MULTIOMS_HUBS` |
| `REMOTEURI_QTY_TOL` / `REMOTEURI_NOTIONAL_TOL` | `1e-6` / `0.01` | doc 09's reconciliation tolerances |

Leaf:

| Variable | Default | Meaning |
|---|---|---|
| `REMOTEURI_LEAF_NAME` | *(required)* | `DH1`, … |
| `REMOTEURI_LEAF_HUBS` | *(required)* | comma-separated hubs this leaf folds (⊆ topology) |
| `REMOTEURI_AMPS_URI` | `tcp://amps:9007/amps/fix` | AMPS URI(s); comma-separated for an HA pair |
| `REMOTEURI_AMPS_BOOKMARK` | `epoch` | `epoch` \| `now` \| `most_recent` \| literal |
| `REMOTEURI_AMPS_FILTER` | `""` | optional server-side content filter |
| `REMOTEURI_AMPS_MAX_PENDING` | `250000` | AMPS→update-graph buffer bound per hub |
| `REMOTEURI_EXEC_RING` | `0` | ring capacity for `oms_executions` / `oms_events`; `0` = append-only |
| `REMOTEURI_STATS_PERIOD_MS` | `5000` | `rx_leaf_stats` refresh |

Collector:

| Variable | Default | Meaning |
|---|---|---|
| `REMOTEURI_LEAVES` | the two-leaf demo | JSON `[{"name","uri","hubs"}]`; every hub on exactly one leaf |
| `REMOTEURI_RESOLVER` | `uri` | `uri` (`deephaven.uri.resolve`) or `barrage` (`barrage_session().subscribe`) |
| `REMOTEURI_CONNECT_TIMEOUT` / `_INTERVAL` | `300` / `5` s | keep retrying until every leaf exposes all four exports |
| `REMOTEURI_MD_SYMBOLS` | the generator's 8 symbols | `SYMBOL:reference price,…` |
| `REMOTEURI_MD_PERIOD_MS` / `_SPREAD_BPS` / `_SEED` | `1000` / `5` / `42` | quote refresh, half-spread, walk seed |

Adding a leaf `dh3`: copy the `dh2` block in `docker/docker-compose.remote-uri.yml` (name,
container, port, `REMOTEURI_LEAF_NAME`, `REMOTEURI_LEAF_HUBS`), add one object to
`REMOTEURI_LEAVES`, keep every hub assigned exactly once, and add it to the collector's `depends_on`.
Move a hub between leaves by editing both lists — the new leaf replays the hub's tape from `EPOCH`.

---

## Tests

**Unit** — pure python, no Deephaven, no containers: configuration and the leaf/hub partition
rules, URI/ticket helpers, the exposure formulas (with a python reference implementation), the
market-data walk, the search clauses.

```bash
bash run_tests.sh                          # standalone
./gradlew :deephaven-remote-uri:pytest     # or through gradle (wired into `check`)
```

**End-to-end** — `down -v` → build → up → wait for the three banners → generator
`--multi-oms --amps-uri … --emit-expected` → `pytest` through `pydeephaven` against the leaves
and the collector → teardown.

```bash
bash e2e/run_e2e.sh

KEEP_STACK=1 bash e2e/run_e2e.sh          # leave the stack up to poke at afterwards
SEED=7 ORDERS=30 CHILDREN=2 bash e2e/run_e2e.sh
PYTEST_ARGS='-k "not restart"' bash e2e/run_e2e.sh   # skip the slow recovery assertion
```

It asserts, against the generator's `--emit-expected` oracle (doc 10 §12): every contract global
exists on its server — and `oms_fix_messages` does *not* exist on a leaf; `rx_orders` on DH1 holds
only OMS-A rows and DH2 only the other three hubs (each leaf's hub set read from its own
`leaf_config`); on the collector every hub-order's `OrdStatus`/`CumQty`/`LeavesQty`/`AvgPx`/
`ExtOrdID`/`LinkState`/`RootKey`/`BreakKind` equal the oracle (cross-server linking is
byte-for-byte doc 09's); `find_exposure` returns whole families in `RootKey, Depth, Oms, OrderKey`
order and both `exposure_for` and every `orders_marked` row match `remote_uri.exposure`'s
pure-python reference — the *shipped* one, so a formula edit that is not mirrored in it fails
here; `remote_executions` returns the same `ExecID`s as the owning leaf's own table, for one order
on each leaf, and `remote_live_executions` returns the same rows as a refreshing table; `fleet`
counts match (and the suite prints heap-per-order per leaf — the first measured figure behind doc
10 §2.4); and a `DH1` restart makes `orders_recon` *fail* (doc 10 §2.7) after which `reconnect()`
reproduces the identical collector state. The suite skips (never fails) without a stack.

The e2e imports `remote_uri.exposure` and `remote_uri.uris` client-side (both stdlib-only), so it
needs no second copy of the frozen formulas or of the `rx_orders_<leaf>` naming rule.

---

## Troubleshooting

**`rx-dh1` exits with code 137 / the collector never resolves.** Out of memory: the podman machine
on this repo's development box has 6 GB, and the heaps default to `-Xmx1g` per leaf and `1536m`
for the collector for that reason. Do not run this stack beside the 4 GB `fix42-dashboard` stack;
`podman machine set --memory 12288` (a machine restart — it stops every container) is the
alternative for bigger heaps (`DH_XMX_LEAF`, `DH_XMX_COLLECTOR`).

**Collector banner says a leaf export is missing.** "Healthy" is not "exported": the gRPC probe
passes before app mode has finished wiring a leaf. The collector retries for
`REMOTEURI_CONNECT_TIMEOUT` seconds; if the leaf really failed, `podman logs rx-dh1` shows the
`[remote-uri] FAILED` line, and `reconnect()` from the collector console picks it up once fixed.

**Tables on the collector are all failed / "table has been failed".** A leaf went away; the
subscribed tables and everything built on them fail together. Restart the leaf (it replays from
`EPOCH`) and run `reconnect()`.

**No `amps` image.** AMPS is commercial software with no public image; the compose file expects
the locally built `localhost/amps-demo:5.3.5.135` (an amd64 image, emulated on Apple silicon —
fine at demo rates). Set `AMPS_IMAGE`, or run without the `amps` service and point every
`REMOTEURI_AMPS_URI` at your broker (note the URI selects the message type: `/amps/fix`).

**Stale families / counts that do not match.** The AMPS journal is a journal: a previous run with
another seed is still in it. `down -v` (the e2e does it) removes the `rx-amps-data` volume.

**Duplicate hub across leaves.** Refused at startup with the offending hub named — a hub folded
twice would make the merged cache non-unique and the linking join would fail later instead.
