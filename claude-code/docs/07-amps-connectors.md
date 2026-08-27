# AMPS Connectors — Design & Contract

Binding spec for `:amps-connectors`, the Spring Boot application that bridges
[60East AMPS](https://www.crankuptheamps.com/) topics into Deephaven tables. Independent of
the FIX 4.2 pipeline in docs 01–05: it publishes into the *same* Deephaven server, so its
tables appear alongside `order_state_latest` and friends, but it shares no code with it.

---

## 1. What it is

```
   AMPS server(s)                    amps-connectors (Spring Boot, java 21)                Deephaven
 ┌────────────────┐   sow_and_subscribe   ┌──────────────────────────────────────┐   ┌────────────────────┐
 │ SOW topic      ├──────────────────────►│ AmpsClientSubscriber                 │   │                    │
 │   Orders (fix) │                       │      │                               │   │  amps_orders       │
 ├────────────────┤   sow_and_delta_sub   │      ▼  RecordDecoder (fix/nvfix/json)   │    (keyed)         │
 │ SOW topic      ├──────────────────────►│   tag -> value                        │   │                    │
 │  Positions(nv) │                       │      │                               │   │  amps_positions    │
 ├────────────────┤   subscribe(epoch)    │      ▼  FieldMapper  (allowlist)     │   │    (keyed)         │
 │ journal topic  ├──────────────────────►│   Object[] in schema order           │   │                    │
 │  Trades (json) │                       │      │                               │   │  amps_trades       │
 └────────────────┘                       │      ▼  DeltaRowMerger (publish DELTA)│  │    (append-only)   │
                                          │      ▼  RowBatcher                    │  │                    │
                                          │      ▼  FlightDeephavenGateway        ├─►│                    │
                                          └──────────────────────────────────────┘   └────────────────────┘
                                                     ▲                                        │
                                                     └───── DeephavenLifecycleMonitor ◄────────┘
                                                            polls; a generation change
                                                            restarts every connector
```

One application, one Deephaven server, **one or more connectors**. Each connector is one AMPS
topic bridged into one Deephaven table, with its own connection, subscription and mapping.

## 2. Configuration model (`application.yml`)

```yaml
amps:
  enabled: true
  deephaven:                     # one server, shared by every connector
    host: localhost
    port: 10000
    authentication: Anonymous
    console-type: python
    health-check-interval: 5s
  connectors:
    - name: orders-fix           # unique; also the default AMPS client name
      enabled: true
      format: FIX                # FIX | NVFIX | JSON
      source:
        driver: AMPS             # AMPS | SIMULATED
        host: localhost          # or an explicit `uri:`
        port: 9007
        transport: tcp
        topic: Orders
        sow: true                # SOW topic (keyed) vs journal topic (append-only)
        subscription-mode: FULL  # FULL | DELTA
        bookmark: epoch          # journal topics only
        filter: "/Symbol = 'AAPL'"
      deephaven:
        table: amps_orders       # the global name; must be an identifier
        publish-mode: FULL       # FULL | DELTA
        key-columns: [ClOrdID]   # non-empty => keyed table
        ingest-timestamp-column: IngestTs
        sow-key-column: SowKey   # optional; carries the AMPS SOW key
        create-if-missing: true
        max-batch-rows: 5000
        flush-interval: 250ms
      fields:                    # the allowlist
        - { tag: "11", column: ClOrdID, type: STRING }
```

Bound by `AmpsConnectorsProperties` (`@ConfigurationProperties("amps")`) and validated at
startup by `ConnectorValidator` (§7).

## 3. SOW topic vs journal topic — the pivot

`source.sow` decides the shape of everything downstream.

| | **SOW topic** (`sow: true`) | **Journal topic** (`sow: false`) |
|---|---|---|
| AMPS command | `sow_and_subscribe`, or `sow_and_delta_subscribe` in DELTA mode | `subscribe` from a bookmark |
| replay on connect | the whole state of the world | the whole transaction log, from `epoch` |
| Deephaven table | `input_table(col_defs=…, key_cols=[…])` — **keyed** | `input_table(col_defs=…)` — **append-only** |
| an update to an existing record | replaces that key's row | appends another row |
| record removal | out-of-focus (`oof`) message → `deleteFromInputTable` | n/a |
| `key-columns` | required | must be empty |

The default bookmark is `epoch`, i.e. "resubscribe from the beginning", which is what makes a
journal-backed table rebuild identically after a restart — the same property doc 03 §3.3 relies
on for the Kafka side of the FIX pipeline.

## 4. FULL vs DELTA — two independent knobs

`source.subscription-mode` is what **AMPS sends**; `deephaven.publish-mode` is what **we
publish**. They are separate settings because they solve different problems, but they interact:

| subscription | publish | behaviour |
|---|---|---|
| FULL | FULL | every message is a whole record, published as a whole row |
| FULL | DELTA | harmless; the merge is a no-op over complete rows |
| DELTA | DELTA | AMPS sends changed fields only; `DeltaRowMerger` fills the rest from the last row for that key |
| DELTA | FULL | **rejected at startup** |

That last row is the reason the merger exists. Adding a row to a Deephaven keyed input table
replaces the key's row *wholesale*, so publishing a three-field delta as a row would null out
every other column. `ConnectorValidator` refuses the combination rather than let it corrupt the
table quietly.

`MappedRow` carries a `present[]` mask alongside its values so the merge can tell **"the payload
did not mention this field"** (keep the stored value) from **"the payload sent it empty"** (clear
it). Both look like `null` in the value array; only the mask separates them.

### 4.1 Delta end to end — a SOW topic delta-subscribed and delta-published

The `DELTA`/`DELTA` row above is a supported and fully wired path, not a theoretical one: it is
what the shipped `positions-nvfix` connector does.

```yaml
      source:
        topic: Positions
        sow: true
        subscription-mode: DELTA     # AMPS sends only the fields that changed
      deephaven:
        table: amps_positions
        publish-mode: DELTA          # merge each partial over the stored row
        key-columns: [Account, Symbol]
```

**On the AMPS side**, `sow: true` plus `subscription-mode: DELTA` selects the delta form of the
SOW subscription:

```java
return source.getSubscriptionMode() == UpdateMode.DELTA
        ? Message.Command.SOWAndDeltaSubscribe
        : Message.Command.SOWAndSubscribe;
```

**On the Deephaven side**, every mapped row goes through `DeltaRowMerger` before it is batched,
so what reaches the keyed input table is always a complete row. A second message carrying only
`Quantity` keeps the `AvgCost` the first one set:

```
in:  Account=ACC-1  Symbol=AAPL  Quantity=100  AvgCost=185.5
in:  Account=ACC-1  Symbol=AAPL  Quantity=150
out: ["ACC-1", "AAPL", 150.0, 185.5]
```

**The pairing is mandatory, not advisory.** Three validator rules box it in: a delta subscription
requires a SOW topic, delta publishing requires a keyed table, and a delta subscription requires
delta publishing. The third is the destructive one — see the table above.

#### The seeding assumption

The merger takes whatever arrives first for a key as that key's base row:

```java
Object[] previous = lastByKey.get(key);
if (previous == null) {
    merged = row.values().clone();     // first message for this key becomes the base
} else {
    merged[i] = row.present()[i] ? row.values()[i] : previous[i];
}
```

This is correct **because `sow_and_delta_subscribe` replays the SOW first**, and SOW records are
complete: the base is a whole record and deltas layer onto it. If a key's first message were
itself a partial delta, the columns it did not mention would start null and stay null until a
complete record for that key arrived.

Nothing in the connector depends on that never happening beyond the replay itself, and a
connector restart re-replays the SOW (§6), so the merger's memory is rebuilt from complete
records on every start. `Connector.start` calls `merger.clear()` for exactly this reason —
carrying merged state across a restart would let a stale value from the previous life survive
into a table the replay was supposed to rebuild.

## 5. Field mapping is an allowlist

`fields` is the complete set of columns. A field in the payload with no mapping is dropped — it
never reaches Deephaven. A mapped field the payload omits publishes as null.

`tag` is interpreted per format, which is the only place the three formats differ:

| format | `tag` means | decoder |
|---|---|---|
| `FIX` | the FIX tag number, e.g. `"11"` | `DelimitedRecordDecoder` (SOH-separated `tag=value`) |
| `NVFIX` | the field name, e.g. `ClOrdID` | `DelimitedRecordDecoder` — same algorithm, named keys |
| `JSON` | the field name, or a dotted path such as `execution.venue` | `JsonRecordDecoder` |

All three decode to `Map<String, String>`, so mapping, merging, batching and publishing are
written once. `JsonRecordDecoder` flattens nested objects to dotted paths and also registers each
leaf under its bare name, so a flat document maps with `price` and a nested one with either
`order.price` or `price`.

`type` is one of `STRING BOOLEAN BYTE SHORT INT LONG FLOAT DOUBLE CHAR INSTANT` (aliases such as
`integer`, `bool`, `timestamp` bind too). `INSTANT` accepts FIX UTCTimestamp
(`20240115-14:30:00.123`), ISO-8601, and bare epoch numbers whose unit follows from the digit
count (10/13/16/19 → s/ms/µs/ns).

Two optional synthetic columns are appended after the mapped fields: `sow-key-column` (the AMPS
SOW key of the message) and `ingest-timestamp-column` (when the connector processed it).

### 5.1 Keying on the AMPS SOW key

`sow-key-column` may itself be named in `key-columns`, which is the answer for a SOW topic whose
key AMPS assigns — a `KeyGenerator`, or any configuration where the key is not reconstructible
from the record body:

```yaml
deephaven:
  table: amps_orders
  sow-key-column: SowKey
  key-columns: [SowKey]        # key on what AMPS assigned, not on a mapped field
```

`Message.getSowKey()` travels on the record as `AmpsRecord.sowKey` and is written to that column
like any other value, so the keyed table's identity matches the SOW's exactly. It is also the
only workable key for **removals**: an out-of-focus message may carry no body at all, so a key
derived from mapped fields would have nothing to resolve against, while the SOW key is still on
the message.

The business fields stay mapped as ordinary columns — the SOW key is opaque, so it identifies
rows without describing them.

**A row is only published when every key column has a value.** A record whose key value is
missing is counted in `rejectedRecords` and dropped. Rendering the gap as text instead would
give every such record the same key and collapse them onto one row of the table, which is why
`TableSchema.rowKey` returns `null` the moment any key component is null.

## 6. Deephaven lifecycle → connector lifecycle

> *"when starting or restarting deephaven server, it needs to start or restart AMPS connectors as
> well, once deephaven is up, to rehydrate data from AMPS"*

`DeephavenLifecycleMonitor` polls every `health-check-interval` and hands
`DeephavenGateway.refresh()`'s **generation** to `ConnectorManager`:

- `0` — unreachable. Every connector is stopped; nothing publishes into the void.
- unchanged — steady state. Nothing happens (except retrying any connector that failed to start).
- **changed** — a different incarnation of the server. Every connector is stopped, then started
  again, which re-creates its table and **replays its subscription from the start**.

The generation changes when the client had to rebuild the session, *or* when the probe finds the
connectors' tables missing from the python scope. The second case is what catches a Deephaven
restart: the server answers gRPC again, but its globals are gone. Both paths land on the same
response, so nothing has to distinguish "reconnected" from "restarted".

Restarting a connector is a full rehydration by construction, not by special-casing: a SOW topic
replays the state of the world, a journal topic replays from `epoch`, and the `DeltaRowMerger`
forgets everything it remembered from the previous life.

Connectors are started independently and retried on every tick, so one unreachable AMPS server
does not hold up the others and recovers on its own.

## 7. Startup validation (`ConnectorValidator`)

Cross-field rules bean validation cannot express. All are checked once, and a failure stops the
application with the full list rather than a stack trace:

- connector names unique; tags and columns unique within a connector
- `deephaven.table` and every column name is an identifier — the table name is interpolated into
  generated python
- FIX tags are tag *numbers*
- `sow: true` ⟹ `key-columns` non-empty; `sow: false` ⟹ `key-columns` empty
- `key-columns` ⊆ mapped columns (plus the synthetic ones)
- `publish-mode: DELTA` requires a keyed table
- `subscription-mode: DELTA` requires a SOW topic **and** `publish-mode: DELTA` (§4)
- synthetic columns must not collide with mapped ones
- at least one field mapping

## 8. How rows actually reach Deephaven

Two server APIs, split by what each is good for:

- **Table creation — a python console session.** `input_table` is a server-side constructor with
  no gRPC equivalent, so `TableBootstrapScript` generates a small python snippet and
  `ConsoleSession.executeCode` runs it. Creation is guarded by a `try: <name> / except NameError`
  so it is idempotent, and an existing table whose columns disagree with the current
  configuration raises rather than accepting mismatched rows.
- **Row publishing — Arrow Flight.** `FlightSession.addToInputTable` takes an Arrow batch
  directly; `deleteFromInputTable` takes a batch of key columns. Rows never travel as generated
  python.

`TableSchema` is the single source of truth for column *order*: the generated `col_defs`, the
`Object[]` a mapped row is built into, and the Arrow batch all index by the same positions.

`RowBatcher` buffers rows and publishes on whichever comes first — `max-batch-rows` (flushed by
the submitting thread, which also back-pressures a fast feed) or `flush-interval`. Within a batch,
upserts and deletes are published as consecutive runs in arrival order; regrouping them would
resurrect a record deleted and re-added in the same batch. A failed flush is logged and counted,
not thrown: losing Deephaven is precisely what triggers the reconnect and replay that
republishes current state.

## 9. Module layout

```
amps-connectors/
├── build.gradle.kts                    # spring boot 3.5, java 21, amps-client, deephaven client 42.4
└── src/main/java/com/fix42/dashboard/amps/
    ├── AmpsConnectorsApplication.java  # headless boot app (spring.main.keep-alive holds the JVM open)
    ├── config/                         # the application.yml model + ConnectorValidator
    ├── decode/                         # RecordDecoder: delimited (fix/nvfix) + json
    ├── mapping/                        # TableSchema, FieldMapper, MappedRow, DeltaRowMerger
    ├── deephaven/                      # DeephavenGateway, FlightDeephavenGateway, TableBootstrapScript
    ├── source/                         # AmpsSubscriber: AmpsClientSubscriber + SimulatedAmpsSubscriber
    └── runtime/                        # Connector, ConnectorManager, RowBatcher, DeephavenLifecycleMonitor
```

Dependency versions are pinned to what the rest of the repo already runs against:
`io.deephaven:deephaven-java-client-flight-dagger:42.4` matches
`ghcr.io/deephaven/server:42.4` in `docker/docker-compose.yml`, and Arrow 18.3.0 matches the
version that client publishes with. Arrow needs `--add-opens=java.base/java.nio=ALL-UNNAMED` on
JDK 21; `bootRun` and `test` set it, and the runbook gives it for `java -jar`.

## 10. The simulated source

AMPS is commercial software with no public image, so `source.driver: SIMULATED` provides an
in-process generator. It synthesises payloads in the connector's **own** format from the
connector's **own** field mappings — nesting dotted JSON tags so a mapping like
`execution.venue` resolves — and replays one record per key on start when the topic is a SOW
topic. Decode, mapping, delta merge, batching and the Deephaven publish all run exactly as they
do against a real server; only the bytes' origin differs.

That is what makes the `demo` profile and the end-to-end tests runnable with nothing but a
Deephaven container. It is a test and demo affordance, not a production path.

## 11. Testing

`./gradlew :amps-connectors:test` — 153 JUnit 5 tests, no servers required.

| Suite | Covers |
|---|---|
| `ColumnTypeTest` | alias parsing, coercion per type, the three timestamp encodings, blank-is-null |
| `ConnectorValidatorTest` | every §7 rule, including the destructive DELTA/FULL combination |
| `DelimitedRecordDecoderTest` / `JsonRecordDecoderTest` | delimiters, first-`=` split, dotted paths, explicit JSON null |
| `TableSchemaTest` / `FieldMapperTest` | column order, allowlist behaviour, present-vs-null, composite keys |
| `DeltaRowMergerTest` | merge, explicit clear, per-key independence, delete forgets the key |
| `TableBootstrapScriptTest` | the generated python: keyed vs append-only, dtypes, idempotence, the column check |
| `RowBatcherTest` | size and timer flush, upsert/delete run ordering, failure is counted not thrown |
| `ConnectorTest` | the per-message pipeline for all three formats |
| `ConnectorManagerTest` | **the §6 lifecycle contract**: start, steady state, restart-rehydrate, unavailable, per-connector retry |
| `EndToEndPipelineTest` | the whole application with the simulated source and a recording gateway |
| `ApplicationYamlBindingTest` | the shipped `application.yml` binds, means what this doc says, and validates |

**Verified against a live server** (`ghcr.io/deephaven/server:42.4`, `--spring.profiles.active=demo`):
all three tables created; the two SOW connectors settle at exactly their key counts (25 and 12
rows) while the journal connector's table grows without bound — i.e. keyed-upsert and append-only
semantics both confirmed on the server, not just in the fakes. Restarting the container while the
connectors ran produced:

```
Deephaven probe failed, treating it as a restart: UNAUTHENTICATED: Authentication details invalid
Connected to Deephaven at localhost:10001 (generation 2)
Deephaven generation 1 -> 2: restarting 3 connector(s) to rehydrate
```

after which all three tables were re-created and refilled to the same counts.
