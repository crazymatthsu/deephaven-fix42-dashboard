# Deephaven Table Types — Analysis & Selection

The TODO asks: *which types of Deephaven tables should be used to build a FIX 4.2
latest-order-state cache?* This doc surveys the table types Deephaven Community Core
offers, their semantics and memory profiles, and records which one each node of our
DAG uses and why.

## 1. Survey of table types

### 1.1 Blink tables (`TableType.blink()` — Kafka default; also `TablePublisher` output)

- Hold **only the rows added in the current update-graph cycle**; previous cycle's rows
  vanish each cycle. Memory: O(rows-per-cycle), effectively constant.
- **Special aggregation semantics:** stateful aggregations over a blink table
  (`last_by`, `sum_by`, `count_by`, `agg_by`, …) accumulate over the **entire stream
  history** while the blink parent only retains the current cycle. This is the
  documented pattern for unbounded streams: keep the stream blink, let the aggregation
  hold the (bounded, per-key) state.
- Downstream row-level ops (`where`, `update`, joins as the right side) see only
  current-cycle rows; listeners see every added row exactly once.
- `deephaven.stream.blink_to_append_only(t)` materializes the full history when needed.

### 1.2 Append-only tables (`TableType.append()` / `blink_to_append_only`)

- Rows are only ever added at the bottom; full history retained. Memory: O(total rows).
- Ideal for **audit and history panels** (executions, order events, raw messages) where
  the user scrolls back. Column-oriented storage keeps this cheap for demo volumes
  (millions of rows is fine); a production deployment would bound it (ring) or persist.

### 1.3 Ring tables (`TableType.ring(N)` / `ring_table(t, N)`)

- Retain the **last N rows**; bounded memory sliding window. Semantics of downstream
  ops are like append within the window. The production-hardening option for the audit
  tables if the feed is unbounded; not needed for the demo.

### 1.4 Derived (refreshing) tables

- Every table operation (`update`, `view`, `where`, `natural_join`, `aj`, `last_by`,
  `agg_by`, `count_by`, `select_distinct`, `sort`, …) yields a new live node in the
  update graph, recomputed **incrementally** per cycle. These are not a distinct
  storage type but are how the whole downstream DAG is expressed.

### 1.5 Partitioned tables (`t.partition_by(cols)`)

- A table of constituent sub-tables, one per key. Useful for per-symbol/per-account
  fan-out, parallelism, and `transform` operations. **Not required** here: our
  dashboards filter with `where` on the (small) latest-state table; noted as the
  scale-out path for very high symbol counts.

### 1.6 Input tables (`keyed_input_table` / `input_table`)

- User/programmatically editable live tables. Considered for manual state overrides
  (ops tooling); **out of scope** for this project.

### 1.7 Static tables (`new_table`, `empty_table`, snapshots)

- Non-refreshing. We use tiny static tables as the vehicle for `TablePublisher.add()`
  batches (build with `new_table`), and `snapshot()`/`snapshot_when` if a frozen view
  is ever needed (integration tests just read the live table via the client instead).

### 1.8 `DynamicTableWriter` vs `TablePublisher`

- Both let imperative code inject rows. `DynamicTableWriter` (row-at-a-time, produces an
  append table) is the legacy path; **`TablePublisher`** (batch `add(table)`, produces a
  blink table) is the modern, faster one and composes with blink-aggregation semantics.
  We use `TablePublisher`.

## 2. Selection per role

| Role in this project | Table type | Why |
|---|---|---|
| Ingest `fix_raw` (Kafka `kc.consume`, or AMPS via `TablePublisher`) | **blink** | Unbounded stream; listener consumes each row once; no retention needed upstream of the state machine. Blink either way, so the source switch (doc 03 §2.1) changes nothing about memory. |
| State-machine outputs (`order_state_blink`, `executions_blink`, `order_events_blink`, `fix_messages_blink`) | **blink** via `TablePublisher` | Imperative injection point; bounded memory; feeds both aggregations and append materializations. |
| **Latest-state cache `order_state_latest`** | **`last_by("OrderKey")` over blink** | The core trick: per-key latest row retained by the aggregation (blink semantics), memory O(#orders), updates in-place per cycle — exactly "latest order state cache". |
| Executions history `executions` | **append-only** (`blink_to_append_only`) | Panel scrolls full history; bust/correct/DK re-emissions preserved as an audit trail. |
| Latest per-exec view `executions_latest` | `last_by("ExecID")` over the executions **blink** | Current disposition per execution (post bust/correct/DK). |
| Order lifecycle history `order_events` | **append-only** | The new/amend/cancel history panel. |
| Raw message audit `fix_messages` | **append-only** | Debugging/audit panel; demo volumes are small. Ring is the documented production fallback. |
| Query indexes (`clordid_index`, `execid_index`) | `last_by` aggregations | Bounded by #ids; resolve any ClOrdID/ExecID → OrderKey. |
| Dashboard summaries (`status_summary`, `symbol_summary`, `account_list`) | `count_by`/`agg_by`/`select_distinct` over `order_state_latest` | Small derived views. |
| Panel filters (click-through) | `where` on derived tables | Driven by `deephaven.ui` state. |

## 3. Memory model summary

For O(M) messages over O(K) orders with O(E) executions:

- blink nodes: O(batch) — constant.
- `order_state_latest`: O(K) rows (the cache — this is the product's bound).
- `executions`/`order_events`/`fix_messages` append tables: O(E)/O(M) — acceptable for
  the demo; switch to `ring` or drop `fix_messages` for production hardening.
- Python state machine dicts: O(K + #ClOrdIDs + E) — same order as the Java reference
  cache. A production concern (terminal-order eviction) is documented, not implemented.

## 4. Alternatives considered and rejected

1. **Append-only ingest + `last_by` directly on parsed FIX columns** (no state machine):
   cannot resolve amend chains (`ORD1001→ORD1002`), late OrderID, per-request reject
   reverts, or ExecID dedupe — a `last_by(ClOrdID)` would show one order as several.
   Rejected: correctness requires the stateful fold.
2. **`update_by` (rolling/cumulative ops) for state**: `update_by` expressions cannot
   express cross-row identifier aliasing or conditional reverts. Rejected.
3. **Ring tables for the cache**: a ring of the state stream bounds memory but loses
   per-key latest guarantees (an idle order could age out of the ring). `last_by` is
   both smaller and correct. Rejected.
4. **Partitioned-table-per-order**: K constituent tables is heavy and unnecessary for a
   cache keyed lookup; `last_by` + `where` is the idiomatic shape. Rejected (kept as a
   scale-out note for per-symbol analytics).
