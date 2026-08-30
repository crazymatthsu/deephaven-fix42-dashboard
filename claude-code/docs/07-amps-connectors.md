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
      format: FIX                # FIX | NVFIX | JSON | COMPOSITE
      composite-parts: []        # COMPOSITE only: each part's format, in wire order (s5.3)
      source:
        driver: AMPS             # AMPS | SIMULATED
        host: localhost          # or an explicit `uri:`
        port: 9007
        transport: tcp
        message-type: ""         # URI message type; defaults to the format's name. COMPOSITE
                                 # requires it: the server-registered composite type name
        topic: Orders
        sow: true                # SOW topic (keyed) vs journal topic (append-only)
        subscription-mode: FULL  # FULL | DELTA
        bookmark: epoch          # journal topics only
        filter: "/Symbol = 'AAPL'"
      deephaven:
        table: amps_orders       # the global name; must be an identifier
        table-type: KEYED        # KEYED | APPEND_ONLY | BLINK | RING; default follows `sow`
        ring-capacity: 100000    # RING only: rows retained
        publish-mode: FULL       # FULL | DELTA
        key-columns: [ClOrdID]   # KEYED only, and required by it
        ingest-timestamp-column: IngestTs
        sow-key-column: SowKey   # optional; carries the AMPS SOW key
        create-if-missing: true
        max-batch-rows: 5000
        flush-interval: 250ms
      fields:                    # the allowlist
        - { tag: "11", column: ClOrdID, type: STRING }
        - { tag: "54", column: Side,    type: STRING, decode: SIDE }
        - { tag: "1",  column: Account, type: STRING, default-value: DUMMY }
      explode:                   # optional: a row per member of an object field (s5.4)
        tag: value
        key-column: Symbol
        fields:
          - { tag: qty, column: Qty, type: DOUBLE }
```

Bound by `AmpsConnectorsProperties` (`@ConfigurationProperties("amps")`) and validated at
startup by `ConnectorValidator` (§7).

## 3. Two independent choices: the AMPS side and the Deephaven side

`source.sow` decides how the topic is read. `deephaven.table-type` decides what it is read
*into*. They used to be the same decision; they are not any more.

### 3.1 The AMPS side — `source.sow`

| | **SOW topic** (`sow: true`) | **Journal topic** (`sow: false`) |
|---|---|---|
| AMPS command | `sow_and_subscribe`, or `sow_and_delta_subscribe` in DELTA mode | `subscribe` from a bookmark |
| replay on connect | the whole state of the world | the whole transaction log, from `epoch` |
| an update to an existing record | a new message for that SOW key | another message |
| record removal | out-of-focus (`oof`) message | n/a |

The default bookmark is `epoch`, i.e. "resubscribe from the beginning", which is what makes a
journal-backed table rebuild identically after a restart — the same property doc 03 §3.3 relies
on for the Kafka side of the FIX pipeline.

### 3.2 The Deephaven side — `deephaven.table-type`

Deephaven has exactly one remotely writable table, the `input_table`, so every other shape has
to be built on the server out of something that *can* be fed from off-box. That splits the four
types into two families:

| `table-type` | created as | rows arrive via | retains | removals |
|---|---|---|---|---|
| `KEYED` | `input_table(col_defs=…, key_cols=[…])` | `addToInputTable` | one row per key | yes |
| `APPEND_ONLY` | `input_table(col_defs=…)` | `addToInputTable` | everything | no |
| `BLINK` | `table_publisher()` | `TablePublisher.add` | one update cycle | no |
| `RING` | `ring_table(blink, ring-capacity)` | `TablePublisher.add` | the last `ring-capacity` rows | no |

**Unset means "whatever the topic implies"** — `KEYED` for a SOW topic, `APPEND_ONLY` for a
journal topic. That is exactly what this module did before the setting existed, so every
configuration written against the old rules keeps its old behaviour.

`BLINK` and `RING` are the bounded-memory answers, and they are bounded for real: nothing
upstream holds the rows. Deriving a blink table from an append-only `input_table` instead
(`add_only_to_blink`) would give the same *semantics* while the input table went on retaining
every row — Deephaven's own documentation warns that combination **increases** memory rather
than saving it, which is why this module builds blink tables from a `TablePublisher`.

`key-columns` and `KEYED` imply each other. Keys on a table that has none, or a keyed table with
no keys, are configurations with no meaning, and §7 rejects both.

### 3.3 Combinations the defaults do not reach

Naming a type overrides the topic, and two of those overrides are useful rather than merely
legal:

- **SOW topic → `BLINK` or `RING`.** A live view of *updates* rather than of state: every
  message the SOW sends, seen once. The SOW's `oof` removals have nowhere to land, so they are
  counted (`ignoredRemovals`) and a warning is logged once at startup rather than being dropped
  in silence.
- **Journal topic → `KEYED`.** Latest-by-key over a log, with the whole log replayed from
  `epoch` on every start.

`publish-mode: DELTA` still requires `KEYED`, whatever the topic: a partial row needs a stored
row to merge into.

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

What lands in the column is the raw value coerced to `type` — unless the mapping shapes it first
with `decode`, `values` or `default-value` (§5.2).

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

## 5.2 Shaping the value: `decode`, `values`, `default-value`

Three optional per-field knobs sit between the payload and the column. All are off unless
configured, so a mapping that names none of them behaves exactly as it always did.

```yaml
fields:
  # 1 -> BUY, 2 -> SELL, 5 -> SELL_SHORT, ... the full FIX 4.2 table
  - { tag: "54", column: Side,    type: STRING, decode: SIDE }
  # a field the venue does not always send
  - { tag: "1",  column: Account, type: STRING, default-value: DUMMY }
  # a built-in table with one venue-specific code layered over it
  - tag: "39"
    column: OrdStatus
    type: STRING
    decode: ORD_STATUS
    values: { "Z": VENUE_HELD }
```

### `decode` — a built-in FIX 4.2 code → name table

A FIX enumerated value is a character chosen for the wire, not for a reader: `54=1` is a buy,
`39=E` is a pending replace. `decode` names one of the tables in `FixValueDecode` and publishes
the name instead:

| `decode` | tag | | `decode` | tag |
|---|---|---|---|---|
| `SIDE` | 54 | | `HANDL_INST` | 21 |
| `ORD_STATUS` | 39 | | `SETTLMNT_TYP` | 63 |
| `EXEC_TYPE` | 150 | | `OPEN_CLOSE` | 77 |
| `EXEC_TRANS_TYPE` | 20 | | `ORD_REJ_REASON` | 103 |
| `ORD_TYPE` | 40 | | `CXL_REJ_REASON` | 102 |
| `TIME_IN_FORCE` | 59 | | `CXL_REJ_RESPONSE_TO` | 434 |
| `MSG_TYPE` | 35 | | | |

These are the **full** FIX 4.2 tables, deliberately not shared with `fixcache.FixEnums` in
`deephaven-app-java`. That one narrows the same tags to the subset the dashboard's state machine
handles (doc 01) — a connector bridging an arbitrary topic has no such licence to drop values.

**A code the table does not name passes through unchanged**, so an unrecognised value stays
visible in the column rather than turning into a null. Since a table always yields a name,
`decode` requires `type: STRING` (§7).

### `values` — inline rewrites

The general form, and not FIX-specific: an NVFIX or JSON feed that spells its side `B`/`S`, or a
venue that deviates from the spec. Applied **over** `decode` when both are set, so a named table
can be extended or corrected one code at a time. Unlike `decode` it carries no type restriction —
rewriting `Y` to `1` for an `INT` column is a perfectly sensible normalisation, because the
rewrite happens *before* coercion.

### `default-value` — what to publish when the field is absent

Written as the finished value, not as a wire code: it is coerced to the column's `type` but never
passed through `decode`. A default that does not coerce is rejected at startup, not on the first
message that needs it.

Two things it deliberately does **not** do:

- **A field the payload sends *empty* is not defaulted.** That is an explicit clear, and delta
  publishing has to be able to tell it from an absent field (§4). Only a field the payload does
  not carry at all gets the default.
- **A default does not mark the field as present.** This is what keeps delta publishing correct:
  the value seeds a key's first row, and every later message that omits the field still reads as
  "unchanged" rather than overwriting the stored value with the default.

  ```
  in:  Account=ACC-1 Symbol=AAPL Quantity=100          -> Currency defaults to USD
  in:  Account=ACC-1 Symbol=AAPL Currency=EUR          -> Currency is EUR
  in:  Account=ACC-1 Symbol=AAPL Quantity=150          -> Currency stays EUR, not USD
  ```

A **key column may not have a default** (§7). Every record missing that key would share the
default and collapse onto one row of the table — the exact failure `TableSchema.rowKey` returns
null to prevent (§5.1).

## 5.3 Composite message types: `format: COMPOSITE`

AMPS composite message types (`composite-local` / `composite-global` server modules) carry
several length-prefixed **parts** in one message, each part of a constituent type — say JSON
metadata alongside a FIX body. Two things make them different from every other format:

- **The framing is binary** (a 4-byte length prefix per part), so `AmpsClientSubscriber` cannot
  hand the payload on as one string. It unframes the message with the client's own
  `CompositeMessageParser` — reading the raw message, not `getData()` — and delivers the parts
  on `AmpsRecord.parts()`; `CompositeRecordDecoder` then decodes each part with the decoder for
  its configured format.
- **Tags are part-indexed**: `0.orderId` is part 0's `orderId`, `1.54` is part 1's FIX tag 54 —
  deliberately the same addressing as the `/0/orderId` XPaths AMPS itself uses to filter and
  SOW-key these topics. An **unprefixed** tag reads from the merged namespace (the first part
  carrying it wins, the same first-writer rule as JSON's bare-name aliases), which is the
  natural spelling against a `composite-global` topic — one merged namespace is exactly what
  that module gives filters server-side. Both spellings work against both modules; the module
  choice changes server-side filter/key semantics, not the connector's decoding.

```yaml
- name: orders-composite
  format: COMPOSITE
  composite-parts: [JSON, FIX]         # the server type's constituent list, in order
  source:
    message-type: composite-json-fix   # the server-REGISTERED name; goes in the URI
    topic: orders.composite
  fields:
    - { tag: "0.orderId", column: OrderId, type: STRING }
    - { tag: "1.54",      column: Side,    type: STRING, decode: SIDE }
```

`message-type` (or a full `uri`) is **required**: `composite` is a module, not an AMPS message
type name — the URI has to name whatever the server config registered:

```xml
<MessageType>
  <Name>composite-json-fix</Name>
  <Module>composite-local</Module>
  <MessageType>json</MessageType>
  <MessageType>fix</MessageType>
</MessageType>
```

Part-count mismatches are not errors, in either direction: a message with fewer parts than
configured simply lacks those parts' fields (absent, like any field a payload omitted), and
parts beyond the configured list are unmapped by definition — the field list is an allowlist.
Everything downstream of the decoder — value shaping, delta, batching, all four table types —
is format-agnostic and applies to composite connectors unchanged.

## 5.4 One row per map entry: `explode`

A JSON field that is itself a map with **dynamic keys** —

```json
{"key": "portfolio-1", "value": {"AAPL": {"qty": 250}, "MSFT": {"qty": 100}}}
```

— cannot be mapped by a static column list: the member names are data. (This is the "nested"
map-of-maps representation; its flattened sibling — one record per `(outerKey, innerKey)` pair
on a composite SOW key, as in amps-demo's `cache.nested.entries` — is already tabular and needs
none of this.) `explode` publishes one Deephaven row per member:

```yaml
fields:
  - { tag: key, column: OuterKey, type: STRING }   # repeats on every member row
deephaven:
  key-columns: [OuterKey, Symbol]                  # must include the explode key column
explode:
  tag: value             # the object whose members become rows; must resolve to JSON
  key-column: Symbol     # the member's name
  fields:                # resolved inside each member's value
    - { tag: qty, column: Qty,      type: DOUBLE }
    - { tag: ".", column: Position, type: STRING } # "." = the member value itself
```

Mechanically each member goes through the ordinary `FieldMapper` over an augmented copy of the
decoded fields — the member name and its flattened value registered under synthetic tags only
the explode columns read — so member rows get everything a plain row gets: `decode`/`values`
rewrites, `default-value`, presence flags, key building. Member names are treated as data, never
as paths: `"BRK.B"` is one member, not a nesting.

**Deletion is the part that needs machinery.** On a keyed target, `RecordExploder` remembers
which members each record last published (by AMPS SOW key when the topic has one, else by the
record-level key columns):

- a member missing from the record's next publish → that member's row is **deleted**
- `"value": null` (an explicit clear) → **every** member row is deleted
- the record leaving the SOW (`sow_delete`, out-of-focus) → every member row is deleted; if the
  record was never tracked (a restart), the members named by the delete's own payload are used
- a payload that omits the exploded field entirely → nothing changes, like any absent field

The memory is per-connector, one entry per live record, cleared on every restart — the replay
that follows rebuilds it. Non-keyed targets skip tracking entirely: nothing can be deleted from
an append-only, blink or ring table anyway (§3.2).

`explode` needs the **whole** record every time, so `subscription-mode: DELTA` is refused with
it (§7): a delta that omitted the map would be indistinguishable from the map emptying. Delta
*publishing* is fine — each member row merges independently under its own key.

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
- the resolved `table-type` (§3.2) and `key-columns` agree: `KEYED` requires them, every other
  type forbids them. The message names the default when `table-type` was not configured, since
  that is where a `sow`-shaped mistake surfaces
- `key-columns` ⊆ mapped columns (plus the synthetic ones)
- `publish-mode: DELTA` requires a keyed table
- `table-type: BLINK` / `RING` requires `create-if-missing`: the only way into a blink table is
  the `TablePublisher` the bootstrap creates, so turning the bootstrap off leaves nothing able to
  publish at all
- `subscription-mode: DELTA` requires a SOW topic **and** `publish-mode: DELTA` (§4)
- `decode` requires `type: STRING`; an inline `values` map does not, because it is applied
  before coercion
- `default-value` coerces to its column's type, and is not set on a key column
- synthetic columns must not collide with mapped ones
- at least one field mapping
- `format: COMPOSITE` requires `composite-parts` (which must not nest `COMPOSITE`) and an
  explicit `message-type` or `uri` — `composite` is a module, not a type name (§5.3);
  `composite-parts` on any other format is refused as dead configuration
- a part-indexed tag must name a declared part, and a tag into a `FIX` part must be a tag
  number after its prefix
- `explode` requires a JSON payload to enumerate (`format: JSON`, or `COMPOSITE` with the
  exploded tag in a JSON part) and `subscription-mode: FULL` (§5.4); its `key-column` is an
  identifier, collides with nothing, and — on a keyed table — appears in `key-columns`, since
  member rows share every other key value; its fields follow the same tag/column/shaping rules
  as ordinary mappings

## 8. How rows actually reach Deephaven

Two server APIs, split by what each is good for:

- **Table creation — a python console session.** `input_table` and `table_publisher` are
  server-side constructors with no gRPC equivalent, so `TableBootstrapScript` generates a small
  python snippet and `ConsoleSession.executeCode` runs it. Creation is guarded by a
  `try: <name> / except NameError` so it is idempotent, and an existing table that disagrees
  with the configuration raises rather than accepting mismatched rows — on **columns and their
  order**, on **column types**, and on **keys**. All three are load-bearing: order because rows
  are published positionally, types because a mismatch would otherwise surface much later inside
  `addToInputTable` with nothing pointing at the configuration, and keys because a keyed table
  adopted for an append-only connector would quietly collapse rows onto their keys.
- **Row publishing — Arrow Flight.** Rows never travel as generated python, but *how* the batch
  lands depends on the table type:
  - **input tables** (`KEYED`, `APPEND_ONLY`) take it directly:
    `FlightSession.addToInputTable`, and `deleteFromInputTable` for a batch of key columns.
  - **publisher-backed tables** (`BLINK`, `RING`) cannot — a blink table is not an input table,
    and the only way into one is the `TablePublisher` that created it. So the batch is uploaded
    with `putExportManual`, bound into the script scope under a scratch name with
    `Session.publish`, and moved into the publisher by one line of python. Three round trips per
    *batch* rather than one, and the rows are still Arrow.

The scratch name carries a per-batch sequence number. Two flushes of one connector can overlap —
the scheduled one, and a full-buffer flush by a submitting thread — and a shared name would let
one batch overwrite the other's rows before either was published. The generated python deletes
the scratch global in a `finally`, so a failed `add` does not pin the rows in the script scope.

An out-of-focus removal only means something for `KEYED`. For every other type `Connector` counts
it as an `ignoredRemovals` and drops it, rather than letting it inflate `publishedRows` with a
row that was never published.

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
    ├── decode/                         # RecordDecoder: delimited (fix/nvfix) + json + composite
    ├── mapping/                        # TableSchema, FieldMapper, MappedRow, DeltaRowMerger, RecordExploder
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

It also honours the §5.2 knobs, so the demo shows them working rather than inert: a field with a
code → value table gets one of its **codes** (`BUY`, not `Side-3`), and a field with a
`default-value` is left out of one record in four so the default has something to cover.

Both index on the emitted `tick` alone, not on `key + tick`. The runtime derives `key` from the
same counter as `tick`, so their sum has a fixed parity for an even `simulated-keys` — an index
built on it addressed only half of a two-entry table, and fired the omission on every record or
none. `SimulatedAmpsSubscriberTest` drives a real replay rather than calling `encode` with values
of its own choosing, because that correlation is invisible to a test that picks both.

A `COMPOSITE` connector's mappings are routed to the part their index prefix names and each
part is rendered in its own format, handed over unframed — the shape the real subscriber
produces after `CompositeMessageParser`. An `explode` connector gets an object at its tag whose
membership **shifts across ticks** (each candidate member sits out one tick in five), so the
exploder's vanish-deletes run in the demo rather than only its upserts.

That is what makes the `demo` profile and the end-to-end tests runnable with nothing but a
Deephaven container. It is a test and demo affordance, not a production path.

## 11. Testing

`./gradlew :amps-connectors:test` — 237 JUnit 5 tests, no servers required, plus 6 opt-in
tests that need one (`LiveTableTypeTest`, below).

| Suite | Covers |
|---|---|
| `ColumnTypeTest` | alias parsing, coercion per type, the three timestamp encodings, blank-is-null |
| `ConnectorValidatorTest` | every §7 rule, including the destructive DELTA/FULL combination |
| `DelimitedRecordDecoderTest` / `JsonRecordDecoderTest` | delimiters, first-`=` split, dotted paths, explicit JSON null |
| `CompositeRecordDecoderTest` / `CompositeWireRoundTripTest` | §5.3: part-indexed tags, bare aliases, part-count leniency; the 60East builder → parser framing contract the subscriber relies on |
| `RecordExploderTest` | §5.4: member rows, `.` scalars, dotted member names, vanish/clear/OOF deletion, SOW-key vs key-column identity, unkeyed targets |
| `TableSchemaTest` / `FieldMapperTest` | column order, allowlist behaviour, present-vs-null, composite keys |
| `DeltaRowMergerTest` | merge, explicit clear, per-key independence, delete forgets the key |
| `ValueShapingTest` | §5.2: decode, inline overrides, pass-through of unknown codes, defaults, and that a default seeds a delta's base row without ever clobbering it |
| `DeephavenTableTypeTest` | the four types, the default drawn from `sow`, and the schema each resolves to |
| `TableBootstrapScriptTest` | the generated python for all four types, dtypes, idempotence, the column/type/key checks, the per-batch scratch name |
| `RowBatcherTest` | size and timer flush, upsert/delete run ordering, failure is counted not thrown |
| `ConnectorTest` | the per-message pipeline for all three formats |
| `ConnectorManagerTest` | **the §6 lifecycle contract**: start, steady state, restart-rehydrate, unavailable, per-connector retry |
| `EndToEndPipelineTest` | the whole application with the simulated source and a recording gateway |
| `ApplicationYamlBindingTest` | the shipped `application.yml` binds, means what this doc says, and validates |
| `LiveTableTypeTest` | **opt-in**: the generated python and both publish paths, against a real server |

`LiveTableTypeTest` is the one suite the fakes cannot stand in for — a table type that has to be
built out of generated python is only correct if a server says so. It is skipped unless you ask:

```bash
podman run -d --name dh -p 10000:10000 \
  -e START_OPTS="-Ddeephaven.console.type=python \
     -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler" \
  ghcr.io/deephaven/server:42.4
./gradlew :amps-connectors:test --tests '*LiveTableTypeTest' -Damps.live=true
```

It asserts *in* python (`executeCode` reports failures, not values) that a keyed table upserts,
an append-only table accumulates, a blink table receives its rows and retains none of them, a
ring table keeps the last `capacity` and not the first, and that a mistyped or foreign existing
table is refused rather than adopted.

**Verified against a live server** (`ghcr.io/deephaven/server:42.4`, `--spring.profiles.active=demo`):
all four tables created, and each type behaving as §3.2 claims —

```
amps_orders      rows=25      amps_orders      rows=25      keyed, = simulated key count
amps_positions   rows=12      amps_positions   rows=12      keyed, = simulated key count
amps_trades      rows=2195    amps_trades      rows=2497    append-only, unbounded
amps_ticks       rows=5000    amps_ticks       rows=5000    ring, capped at ring-capacity
```

— the two samples 20 seconds apart. The ring table holding at exactly its capacity while the
append-only table beside it kept growing is the whole point of the setting, measured rather than
asserted. Restarting the container while the connectors ran produced:

```
Deephaven probe failed, treating it as a restart: UNAUTHENTICATED: Authentication details invalid
Connected to Deephaven at localhost:10001 (generation 2)
Deephaven generation 1 -> 2: restarting 3 connector(s) to rehydrate
```

after which all the tables were re-created and refilled to the same counts.
