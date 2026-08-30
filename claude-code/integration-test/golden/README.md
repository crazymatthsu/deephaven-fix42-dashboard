# Expected-data contract (`--emit-expected`)

The integration test does **not** carry a checked-in golden file. The Java generator
is deterministic given `--seed`, so it exports its own expectations on every run and
the test asserts against those. This keeps the golden data honest: it can never drift
from the scenario engine that produced the messages.

## Where it comes from

`run_integration.sh` invokes:

```
./gradlew :fix-mock-generator:run --args="\
  --bootstrap-servers localhost:19092 --topic fix42.messages \
  --seed 42 --orders 12 --rate 200 --scenario all \
  --emit-expected <repo>/integration-test/.out/expected-all.json"
```

`.out/` is wiped at the start of each run and is git-ignored.

## File shape

A JSON list, one object per order chain (doc 05 §2.1). A `{"orders": [...]}` wrapper
is also accepted — `test_e2e.py::_unwrap` tolerates `orders`, `expected`, `chains`,
and `results` keys.

```json
[
  {
    "ChainKey":   "ORD-0001",
    "OrderID":    "ORD-0001",
    "Scenario":   "amend_ack",
    "OrdStatus":  "FILLED",
    "CumQty":     500.0,
    "LeavesQty":  0.0,
    "ClOrdID":    "C-1-2"
  }
]
```

| Field | Meaning |
|---|---|
| `ChainKey` | Kafka message key for the chain — the venue OrderID the generator assigned |
| `OrderID` | tag 37; the join key the test uses against `order_state_latest.OrderID` |
| `Scenario` | catalog name that produced the chain (doc 05 §2.2) |
| `OrdStatus` | terminal status as an enum **name** (`FILLED`, `CANCELED`, …), not the FIX code |
| `CumQty` / `LeavesQty` | absolute end-of-chain quantities |
| `ClOrdID` | the chain's **final** ClOrdID (post-amend rotation), not the root |

### `Scenario` is what makes the rare-path assertions deterministic

A single `--scenario all` run draws from a *weighted* catalog, so nothing guarantees
that a bust or a DK appears. Because each chain carries the name of the scenario that
built it, the test can look up exactly which chain is supposed to have been busted and
assert on that chain — instead of hoping one exists:

- `fill_bust` chain → its `OrderKey` must have a `FillStatus = BUSTED` row in `executions_latest`
- `dk_trade` chain → likewise `DK`
- `amend_ack` chain → must have rotated its ClOrdID (`RootClOrdID != ClOrdID`), and both
  ids must resolve through `clordid_index` to the same `OrderKey`

If the field is absent (older generator) the test falls back to a presence check and
skips with a copy-pasteable `--scenario <name>` command when the path never ran.

## Why the test joins on `OrderID`, not `OrderKey`

`OrderKey` is the *first* identifier that created the chain (doc 01 §3). Because the
`D` arrives before any `8`, it carries no tag 37, so the chain is usually created under
the **ClOrdID of the `D`** — `OrderKey` therefore does not equal `ChainKey` in general.
The `OrderID` column is bound onto the chain as soon as the first execution report
lands, which makes it the stable join key between generator expectations and the cache.

## Root ClOrdID

`--emit-expected` records only the final `ClOrdID`, so the amend-chain assertion reads
the root from the server instead: `order_state_latest.RootClOrdID` (doc 01 §4). Any row
where `RootClOrdID != ClOrdID` has been amended, and both ids must resolve through
`clordid_index` to that row's `OrderKey`.

## Rerun semantics

Replay idempotence (doc 03 §3.3) is about re-*reading* the journal: the cache is a pure
function of the records on the topic, so restarting Deephaven rebuilds it exactly. That is
not the same as re-*publishing*. The generator restarts its venue-side counters
(`ORD-0001`, `EXEC-000001`, …) on every invocation, so a second run's messages carry chain
keys that already exist in the journal — the state machine folds both batches into one
chain, and `expected-*.json` only ever describes the newest batch:

```
ORD-0001: CumQty expected 500.0 got 3300.0
ORD-0003: OrdStatus expected 'FILLED' got 'PENDING_REPLACE'
```

`run_integration.sh` therefore empties `fix42.messages` and restarts Deephaven before it
generates, so a run asserts against exactly the batch it just published — including
`KEEP_STACK=1` reruns against a stack that has been up for days. `RESET_TOPIC=0` skips the
reset and refuses to start against a dirty topic rather than producing the diff above.

The assertions still match on **expected keys**, never on total row counts —
`status_summary` is checked for internal consistency against `order_state_latest` and for
being a superset of the expected chains. That keeps a chain published into the topic by
hand from breaking the run, which is why the count check is `>=`.
