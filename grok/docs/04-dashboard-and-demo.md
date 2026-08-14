# Dashboard, demo, and tests

## Dashboard

Application Mode starts a `deephaven.ui` dashboard plus the unbound
tables (so Code Studio Linker still works).

Layout:

```
┌─────────────────────────────────────────────────────────────┐
│  Controls: Account / Symbol / OrderKey|ClOrdID picker       │
├──────────────────────────────────────────┬──────────────────┤
│  orders_latest (blotter)                 │  selected detail │
│  formatted OrdStatus colors              │  key fields      │
├──────────────────────────────────────────┼──────────────────┤
│  executions for selected OrderKey        │  D/G/F/9/Q tape  │
│  LastQty / LastPx / ExecType / ExecID    │  for that order  │
└──────────────────────────────────────────┴──────────────────┘
```

Selection is lifted into `create_dashboard` as `selected_key` state
(doc: sharing state across panels). A picker lists current `OrderKey`s
and current `ClOrdID`s. Typing a `ClOrdID` resolves through
`clord_index`.

Row click: `ui.table(..., on_row_double_press=...)` sets
`selected_key` from the `OrderKey` column when the hook is available;
the picker is the always-on path.

Status colors (`ui.TableFormat` + `if_`):

- Filled (`2`) → positive / green
- Canceled (`4`) / Rejected (`8`) → negative / red
- Pending (`A`,`6`,`E`) → notice / amber
- Partial (`1`) → accent

## Demo

`compose/compose.yaml` (Podman or Docker Compose):

- `redpanda` — Kafka API on `9092` (in-network) / `19092` (host)
- `deephaven` — `ghcr.io/deephaven/server` extended with:
  - `/app.d` Application Mode scripts
  - `/apps/libs` `oms-engine` JAR
  - `-Ddeephaven.application.dir=/app.d`
  - anonymous auth **or** PSK `deephaven` for local demo

`fix-demo-producer` publishes a scripted lifecycle:

1. Parent algo `D` (optional)
2. Child new → pending new → new ack
3. Partial fill
4. Amend → pending replace → replaced **or** cancel reject
5. Second fill → filled
6. Sibling cancel path (ack + reject)
7. Don't-know on one `ExecID`

The producer uses `FixSerializer` so checksums are legal.

## Tests

### Unit (`fix-codec`, `oms-engine`)

- Parser: `|` and SOH, checksum, strict header, unknown tags kept
- Serializer: recomputes `9` and `10`
- Linker: OrderID wins, ClOrdID chain, rekey C1→O9, ExecID index
- State: each scenario in the TODO (new ack/reject, amend ack/reject,
  cancel ack/reject, partial/full/bust/correct, `9`, `Q`)
- Stale ER ignored; duplicate ExecID no-ops qty
- Missing-New ER still creates
- Parent/child + rollup
- Blank fields do not wipe

### Integration

- `OmsCache` driven by the same scripted tape as the demo producer;
  assert blotter fields after the last message
- `fix-demo-producer` dry-run (no Kafka) builds the same tape
- Compose smoke (optional, tagged): produce → wait for
  `orders_latest` via `pydeephaven` — run only when the stack is up

## Implementation order

1. Gradle wrapper + Java 21 conventions
2. `fix-codec` + tests
3. `oms-engine` + tests (this is the product)
4. `dh-app` Application Mode DAG + dashboard
5. `fix-demo-producer` + `compose/`
6. README with Podman steps
