# Deephaven Table Types and DAG

This document answers the TODO questions: which Deephaven table types
to use, what the DAG looks like, and which engine features carry the
dashboard.

## Why not `last_by` alone?

`last_by("ClOrdID")` on a Kafka blink table would keep the last message
per client id. That is wrong for this product:

- `ClOrdID` changes on every `F`/`G`. Latest state would fragment.
- Identity resolution is a graph (`OrderID` wins, then `ClOrdID`, then
  `OrigClOrdID`, then `ExecID`).
- Apply is a merge: blank fields must not wipe populated ones.
- Stale `TransactTime`, duplicate `ExecID`, pending flags, and rekey
  from `C1` → `O9` are not last-row-wins.

`last_by` **is** the right operator **after** the Java engine has
assigned a stable `OrderKey` and emitted a fully-merged `OrderState`
row. The DAG uses `last_by` as a projection, not as the state machine.

## Table types used

| Table | Type | Why |
|-------|------|-----|
| `fix_raw` | **blink** Kafka consume | Default Kafka type. Bounded memory. New FIX rows live one UG cycle. |
| `parsed_fix` | blink (derived `update`) | Parsed tags for debugging; not the cache. |
| `order_state_updates` | **append-only** (`DynamicTableWriter`) | Every applied state snapshot. Index-stable, ordered, safe under `i`/`ii`. |
| `orders_latest` | standard streaming (`last_by`) | One row per `OrderKey`. The blotter. |
| `order_events` | **append-only** or **ring(N)** | Full tape of D/G/F/8/9/Q with resolved `OrderKey`. Ring if memory must be capped. |
| `executions` | **append-only** | Fill / bust / correct ERs only (`150` in `1,2` or `20` in `1,2`). |
| `clord_index` | `last_by("ClOrdID")` | Alias → current `OrderKey`. |
| `exec_index` | `last_by("ExecID")` | Event → `OrderKey`. |
| `orders_by_account` / `_symbol` | derived `where` / `last_by` not required | Filter `orders_latest`. |
| `child_rollup` | `agg_by` on children | On-read parent totals. |

Not used as the store:

- **Static** — Kafka is live.
- **Ring** on latest state — would drop working orders.
- **Blink** as the blotter — rows would vanish each cycle.
- **Keyed input tables** — users must not edit the book.

## DAG

```
Kafka topic fix42.dropcopy
        │  consume(blink, simple_spec RawFix)
        ▼
   fix_raw  (blink)
        │  table listener (do_replay=true)
        │  Java OmsCache.ingest(raw)
        ▼
   engine heap (indexes + OrderState)
        │
        ├──── DynamicTableWriter ──► order_state_updates  (append)
        │                                    │
        │                                    ▼
        │                            orders_latest = last_by("OrderKey")
        │                                    │
        │                    ┌───────────────┼────────────────┐
        │                    ▼               ▼                ▼
        │            where(Account)   where(Symbol)    where(OrderKey=sel)
        │
        ├──── DynamicTableWriter ──► order_events  (append / ring)
        │                                    │
        │                                    ▼
        │                         where(OrderKey=sel)  → history panel
        │
        └──── DynamicTableWriter ──► executions  (append)
                                             │
                                             ▼
                                  where(OrderKey=sel)  → exec panel
```

Secondary index tables (`clord_index`, `exec_index`) are `last_by` on
alias columns written from the same `ProcessResult`. They exist so a
Deephaven query `clord_index.where("ClOrdID = `C1`")` can resolve to
`OrderKey` without calling Java.

Rekey (`C1` → `O9`): the engine writes one more `order_state_updates`
row with `OrderKey=O9` and `PreviousOrderKey=C1`. `orders_latest` then
has both keys until we filter `where("Tombstone = false")` after
emitting a tombstone on the old key. Implementation: write a tombstone
row (`Tombstone=true`) for `PreviousOrderKey`, then `orders_latest =
order_state_updates.last_by("OrderKey").where("Tombstone = false")`.

## Deephaven features this app uses

| Feature | Use |
|---------|-----|
| `kafka.consumer.consume` + `TableType.blink()` | Ingest |
| `simple_spec("RawFix", string)` | Raw FIX value, no registry |
| `table_listener.listen` + `do_replay=True` | Drive Java ingest, including rows already in the blink window at start |
| `DynamicTableWriter` | Publish engine output as append-only tables |
| `last_by` | Latest order + alias indexes |
| `where` | Selected-order panels, account/symbol filters |
| `agg_by` | Parent rollup |
| Application Mode (`*.app` + `-Ddeephaven.application.dir`) | Start the DAG on container boot |
| `/apps/libs` JAR mount | Put `oms-engine` on the Deephaven JVM classpath |
| `jpy.get_type` | Python scripts call `OmsCache` |
| `deephaven.ui` dashboard + shared state | Click / pick an order → filter exec + history panels |
| Code Studio Linker | Same tables are also bound globally for ad-hoc linking |

## Alternatives considered

| Option | Verdict |
|--------|---------|
| Pure DH formulas / `update` UDF as the state machine | Rejected. Stateful UDFs are not safe across UG cycles and cannot keep five indexes consistent. |
| Custom Kafka parser that emits `OrderState` inside `consume` | Tempting, but the parser would still need the same heap engine, and Kafka parse failures are harder to unit-test than `OmsCache.ingest`. |
| Protobuf / Avro on Kafka | Deferred. Drop-copy is a string today. Adding a schema registry is an independent PR. |
| Engine implements Deephaven `TablePublisher` in Java | Couples `oms-engine` to the Deephaven API. Writer stays in Python so the engine remains a plain library with JUnit tests. |
| AMPS / file WAL | Out of scope. Kafka is the tape; Deephaven heap + `last_by` is the live cache. Restart = Kafka replay from earliest (demo) or a configured offset. |

## Query API mapping

The Java API is the system of record for correctness tests. Deephaven
tables are the same data, live:

| Java | Deephaven |
|------|-----------|
| `getByClOrdId(id)` | `clord_index.where("ClOrdID = id")` → `OrderKey` → `orders_latest` |
| `getByOrderId(id)` | `orders_latest.where("OrderID = id \|\| OrderKey = id")` |
| `getByExecId(id)` | `exec_index.where("ExecID = id")` |
| `findByAccount(a)` | `orders_latest.where("Account = a")` |
| `findBySymbol(s)` | `orders_latest.where("Symbol = s")` |
| `getHistory(key)` | `order_events.where("OrderKey = key")` |
| `getExecutions(key)` | `executions.where("OrderKey = key")` |

Python application-mode helpers (`get_by_cl_ord_id`, …) wrap the Java
API so a Code Studio user can type them without writing `where`.
