# Multi-OMS Drop-Copy Blotter

A second Deephaven app in this repo: it ingests FIX 4.2 **drop-copy** tapes from up to four
OMS hubs at once, links each order to its upstream order through a configurable
external-order-id tag, and reconciles every parent against the rollup of its **direct**
children — so a `CumQty` / notional discrepancy is attributed to the system that owns it
rather than to "somewhere in the chain".

The default topology is a single flow through four hubs, each with its own `ClOrdID` /
`OrderID` / `ExecID` space and its own lifecycle messages:

```
OMS-A  ──►  OMS-B-parent  ──►  OMS-B-child (1..n per parent)  ──►  OMS-C (1 per child)
             16666=A.ClOrdID     16667=B-parent.ClOrdID           16668=B-child.ClOrdID
```

Each hub tape is folded by its **own** `OrderStateMachine` from `fix42cache`, which is
reused **unchanged** from the single-hub dashboard — the module adds no FIX logic, only a
per-hub sticky link map (`{OrderKey: ExtOrdID}`, first non-empty value wins). Everything
after the fold is declarative: an `(Oms, Id) → GlobalKey` index over every id ever seen,
`K−1` iterated joins that resolve each order's `RootKey` (the chain id) and `Depth`, a
per-parent child rollup, and `orders_recon` — the blotter table carrying `DeltaCumQty` /
`DeltaLeavesQty` / `DeltaNotional`, a `BreakKind` and `OnBrokenEdge`. Because linking is a
join and not a fold, an order whose parent tape has not arrived yet is simply `DANGLING`
and **heals** into `LINKED` when the parent shows up, with no replay.

The binding contract is [`docs/09-multi-oms-blotter.md`](../docs/09-multi-oms-blotter.md):
table names, column names, env vars and CLI flags there are frozen the way docs 01/03/05
are for the FIX 4.2 dashboard, and a deviation has to update that doc in the same change.
The assignment it answers is [`TODO.md`](TODO.md).

---

## Quickstart

```bash
# 1. bring the stack up on THIS app (the four hub topics are pre-created by the
#    kafka healthcheck -- see Troubleshooting)
cd claude-code
DH_APP=multi-oms-blotter podman compose -f docker/docker-compose.yml up -d

# 2. publish four correlated drop-copy tapes (12 families, fan-out up to 3)
./gradlew :fix-mock-generator:run \
  --args="--multi-oms --seed 42 --orders 12 --children 3 --rate 200"

# 3. open the dashboard
open http://localhost:10000/ide     # Panels ▸ multi_oms_blotter
```

`--multi-oms` publishes to `fix42.oms-a`, `fix42.oms-b-parent`, `fix42.oms-b-child` and
`fix42.oms-c` — one topic per hub, keyed by that hub's chain key. `--topic` is **rejected**
in this mode (the topology fixes the topics; silently ignoring the flag would hide a
mistake). `--list-scenarios` prints the family catalog, `--dry-run` prints
`<topic>\t<message>` instead of producing.

**What you see.** One dashboard, four rows of panels:

| Panel | Contents |
|---|---|
| Multi-OMS Blotter — filters | Account / Symbol / Side pickers, one checkbox per configured hub, a **breaks only** toggle, free-text search over `ClOrdID` / `OrderID` / `ExtOrdID` / `GlobalKey`, and Prev/Next paging (`MULTIOMS_PAGE_SIZE`, default 200) |
| Breaks by system | `break_summary` — which **system** has breaks, by `BreakKind` |
| Blotter (click a row) | the flat blotter: one row per order per hub, `Oms`, ids, `LinkState`, quantities, the three Δ columns and `BreakKind`, red for a real break and amber for `UNROUTED` |
| Chain of selected row | the whole family of the clicked row (`RootKey ==` the selection), sorted `Depth, Oms` — **clicking any hop lights the whole chain, upstream and downstream** |
| Executions / Order events (selected hop) | that one hop's own tape; quantities are never summed across hubs here |

**breaks only** narrows the blotter to `OnBrokenEdge || BreakKind != NONE` — both ends of
a broken edge, not just the hop that reports the discrepancy.

**Dashboard-only view (no IDE chrome)** — the same iframe endpoint the single-hub app uses:

- <http://localhost:10000/iframe/widget/?name=multi_oms_blotter> — the whole dashboard
- `http://localhost:10000/iframe/table/?name=orders_recon` — just the blotter table
- `http://localhost:10000/iframe/table/?name=orders_tree` — the hierarchical companion
  panel (expand/collapse upstream → downstream, recon columns on every node)

Every table is also a plain global, so the app degrades to individual panels if
`deephaven.ui` is ever missing. From the IDE console:

```python
find_chain("C-0001-2")          # the whole ≥4-hop family, from either end of the chain
get_order("OMS-B-parent", "BP-0001")
breaks_only()                   # anything not clean (wider than the oms_breaks table)
hub_orders("OMS-C"); find_by_account("ACC-1"); find_by_symbol("NVDA")
order_detail("OMS-A|A-0001")    # {"state", "executions", "events"} for one hop
```

---

## Reconciliation semantics

Reconciliation is strictly **per edge**: a parent's own `CumQty` / `LeavesQty` /
`AvgPx·CumQty` against the **sum over its direct children** — never against its whole
subtree (that double-counts mid hops), and never by overwriting a hop's own authoritative
values. `BreakKind` is the taxonomy the UI colors by (doc 09 §5.4):

| `BreakKind` | Meaning | Severity |
|---|---|---|
| `QTY_BREAK` | parent `CumQty` ≠ Σ children `CumQty` — fills disagree; a tape missed or duplicated an execution | red |
| `NOTIONAL_BREAK` | quantities agree but `AvgPx·CumQty` doesn't — a bust/correct propagated wrong | red |
| `DANGLING` | the link value doesn't resolve on the upstream hub (drop-copy gap or bad reference) | red |
| `NO_LINK` | the hub requires a link tag and the order never carried one | red |
| `UNROUTED` | quantities agree; the `LeavesQty` gap is qty open upstream that is not open downstream (unrouted / in-flight) | amber |
| `NONE` | the edge reconciles, or it is a leaf order with a healthy parent edge | quiet |

`DeltaLeavesQty ≠ 0` on its own is deliberately **not** red: with clean fills it is exactly
the unrouted remainder. `oms_breaks` is the red-only table; `breaks_only()` is the wider
"anything not clean" view that also includes the amber `UNROUTED` rows.

`LinkState` is separate from the break kind: a **root-hub** order is `ROOT` (never
`LINKED`), a resolved downstream order is `LINKED`, an unresolvable one is `DANGLING` and
is its own `RootKey` until its parent appears.

---

## Configuration

All read once at app start, on the `deephaven` service in
[`docker/docker-compose.yml`](../docker/docker-compose.yml). A misconfigured topology is a
**startup error**, never a silent fallback.

| Variable | Default | Meaning |
|---|---|---|
| `MULTIOMS_HUBS` | the four hubs above | topology: JSON array of `{name, topic, upstream?, link_tag?}` |
| `MULTIOMS_KAFKA_BOOTSTRAP` | `kafka:9092` | broker for every hub topic |
| `MULTIOMS_QTY_TOL` | `1e-6` | absolute tolerance for qty deltas |
| `MULTIOMS_NOTIONAL_TOL` | `0.01` | absolute tolerance for notional deltas |
| `MULTIOMS_PAGE_SIZE` | `200` | blotter page size (UI default) |

Validation: unique names, unique topics, `upstream` must name another hub, `link_tag`
present **iff** `upstream` is and a positive int, no cycles, at least one root. Adding a
hub also means adding its topic to the kafka healthcheck in the compose file — the
consumer resolves its partitions once, at startup, so a topic that only springs into
existence when the generator first produces leaves that hub's table empty forever.

The startup banner in `podman logs fix42-deephaven` prints the resolved topology,
tolerances, every exported global and whether `orders_tree` and the dashboard came up.

---

## Tests

**Unit** — pure python, no Deephaven, no containers: topology validation, the sticky
`LinkTracker`, key/name sanitizing, row augmentation, ingest configuration and the paging
math.

```bash
bash run_tests.sh                                   # standalone
./gradlew :deephaven-app-multi-oms-blotter:pytest   # or through gradle (wired into `check`)
```

**End-to-end** — `down -v` → stack up on `DH_APP=multi-oms-blotter` → generator
`--multi-oms --emit-expected` → `pytest` over the live tables through `pydeephaven` →
teardown. Self-contained: it never touches `integration-test/`.

```bash
bash e2e/run_e2e.sh

KEEP_STACK=1 bash e2e/run_e2e.sh          # leave the stack up to poke at afterwards
SEED=7 ORDERS=30 CHILDREN=2 bash e2e/run_e2e.sh
PYTEST_ARGS="-k restart" bash e2e/run_e2e.sh
```

It asserts, against the generator's independently computed `--emit-expected` oracle:

1. every doc 09 §5 global exists — plain tables via `open_table`, `orders_tree` and
   `multi_oms_blotter` via `run_script` (a hierarchical table is not a plain-table ticket,
   and the dashboard is not a table at all);
2. per hub-order `OrdStatus` / `CumQty` / `LeavesQty` / `AvgPx` / `ExtOrdID` / `GlobalKey` /
   `LinkState` / `BreakKind` / `RootKey`, keyed by (`Oms`, `ClOrdID`);
3. the family taxonomy: `clean_fill` / `working_fanout` / `late_parent` absent from
   `oms_breaks` (`late_parent` proves out-of-order arrival healed), `missed_fill`
   `QTY_BREAK` on exactly its OMS-A and OMS-B-parent rows with B-parent short and
   `PARTIALLY_FILLED`, `dangling_child` `DANGLING` and its own root, `partial_route`
   `UNROUTED` at OMS-B-parent and **not** in `oms_breaks` (but in `breaks_only()`);
4. the per-edge math on a clean family — parents `HasChildren` with all three deltas zero,
   OMS-C leaves with no children;
5. `find_chain` returns the identical family from the OMS-C leaf and from the OMS-A root;
6. `break_summary` counts equal what the expected export implies;
7. **restart resilience** — restart the Deephaven container and re-assert (2) identically;
   four topics are four journals, so the whole recon is a pure function of what was
   published.

The suite **skips** (never fails) when the stack is unreachable or the expected file is
missing, so it is safe in CI without containers.

---

## Troubleshooting

**Empty tables, no errors.** The four hub topics must exist *before* Deephaven starts:
`kc.consume` resolves a topic's partitions once, at startup, and a topic that does not
exist yet yields zero partitions and a consumer that shuts down immediately. The kafka
healthcheck creates all four (plus `fix42.messages`) and `depends_on: service_healthy`
gates Deephaven on it — so this only bites if you start the server against a broker that
was not brought up by this compose file.

**`DANGLING` rows while the generator is still running.** Expected, and the point of the
design: hub tapes are independent topics with no cross-topic ordering, so a child can be
ingested before its parent. The order is its own root until the parent arrives, then the
family heals in place. The `late_parent` scenario forces this on purpose. Only a `DANGLING`
that *persists* after the flow has settled is a real break.

**Stale families / break counts that don't match.** Always `down -v` before a run. The
topics are journals: a previous run with a different `--seed` leaves its families behind,
and while the per-key assertions survive that, the roll-up counts legitimately won't.
`e2e/run_e2e.sh` does the `down -v` for you.

**Wrong app.** Every table here is missing if `DH_APP` was left pointing somewhere else —
only the selected folder under `docker/apps/` is mounted at `/app.d`. Check the banner:
`podman logs fix42-deephaven | grep -A 12 "Multi-OMS Drop-Copy Blotter"`.

**No `orders_tree` panel.** The banner says `orders_tree UNAVAILABLE` when the server
rejects `Table.tree`. It is a companion panel only — the flat blotter and every other table
are unaffected.
