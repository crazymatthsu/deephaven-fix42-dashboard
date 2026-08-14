# Deephaven FIX 4.2 Trading Dashboard — Overview

## Goal

Consume a FIX 4.2 drop-copy stream from Kafka, maintain the **latest
state of every order** as a live Deephaven DAG, expose a query API
(`Account`, `Symbol`, `ClOrdID`, `OrderID`, `ExecID`), and render a
dashboard where selecting an order shows its executions and its
new / amend / cancel history.

This is a **Deephaven application**, not a FIX session engine. It does
not speak logon / heartbeat / resend. Producers already have raw FIX
payloads (`8=FIX.4.2|9=...|35=...|10=...`) and publish them to Kafka.

Domain linking and apply rules are taken from the sibling analysis in
`/Users/maojenhsu/ai-code/fix42-oms-cache/{grok,claude-code}/docs/` and
adapted to Deephaven table types. That project is a heap cache library;
this one is a live table DAG plus dashboard.

## In-scope message types

| MsgType | Name | Role |
|---------|------|------|
| `D` | New Order - Single | Creates a working order |
| `G` | Order Cancel/Replace Request | Pending replace + new `ClOrdID` |
| `F` | Order Cancel Request | Pending cancel + new `ClOrdID` |
| `8` | Execution Report | Authoritative venue / broker state |
| `9` | Order Cancel Reject | Rejects cancel or replace; reverts status |
| `Q` | Don't Know Trade | Dispute flag on an `ExecID` |

`H` (Order Status Request) is parsed and recorded on the tape but does
not mutate economics. It is accepted so a mixed feed does not fail.

## Scenarios the state machine must handle

- New order ack / reject
- Amend (`G`) ack / reject
- Cancel (`F`) ack / reject
- Execution report: new, partial fill, full fill, amend-fill (correct),
  cancel-fill (bust)
- Cancel reject (`9`)
- Don't know trade (`Q`)

## Module map

```
grok/
  docs/                 this analysis
  fix-codec/            FIX 4.2 parser / serializer / dictionary
  oms-engine/           linker, state machine, in-process query API
  fix-demo-producer/    mock FIX 4.2 → Kafka
  dh-app/               Deephaven Python application-mode scripts
  compose/              Podman / Docker Compose (Deephaven + Redpanda)
```

Build is Gradle, Java 21. Python lives in `:dh-app` and is **not**
compiled by javac; Gradle only packages it for the Deephaven image.

## Design principles

1. **Java owns identity and apply.** `last_by` is not a state machine.
   Linking `ClOrdID` chains, promoting `OrderID`, merging blank fields,
   stale `TransactTime`, and bust/correct all run in `oms-engine`.
2. **Deephaven owns the queryable cache and the UI.** The engine emits
   typed rows; the DAG projects latest state, indexes, executions, and
   history as ticking tables.
3. **Raw FIX is the Kafka value.** No schema registry in v1. The value
   is the FIX string (`|` or SOH). The key is optional (`ClOrdID`).
4. **Latest state is required; history is bounded.** Every applied
   message updates latest state. The tape is an append/ring table.
5. **Work only under `grok/`.** Sibling folders in this repo belong to
   other models and must not be read or written.

## How to build and run

```bash
./gradlew test
./gradlew :fix-demo-producer:installDist
podman compose -f compose/compose.yaml up --build
```

Then open http://localhost:10000/ide/ (PSK printed in the Deephaven
log, or `deephaven` if set).
