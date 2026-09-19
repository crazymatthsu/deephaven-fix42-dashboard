# dh-connectors — Design & Contract

Binding spec for `dh-connectors` — the multi-source connector framework and the Spring Boot
applications built on it (§9) — bridging live feeds into Deephaven tables.
[60East AMPS](https://www.crankuptheamps.com/) topics, Kafka topics and raw framed TCP streams
all arrive through the same pipeline (§11). Independent of the FIX 4.2 pipeline in docs 01–05:
it publishes into the *same* Deephaven server, so its tables appear alongside
`order_state_latest` and friends, but it shares no code with it.

---

## 1. What it is

```
   sources                            dh-connectors (Spring Boot, java 21)                 Deephaven
 ┌────────────────┐   sow_and_subscribe   ┌──────────────────────────────────────┐   ┌────────────────────┐
 │ AMPS SOW topic ├──────────────────────►│ RecordSource (amps / kafka / tcp)    │   │                    │
 │   Orders (fix) │                       │      │        SourceRecord           │   │  amps_orders       │
 ├────────────────┤   sow_and_delta_sub   │      ▼  RecordDecoder (fix/nvfix/json)   │    (keyed)         │
 │ AMPS SOW topic ├──────────────────────►│   tag -> value                        │   │                    │
 │  Positions(nv) │                       │      ▼  RecordTransform chain (§12)   │   │  amps_positions    │
 ├────────────────┤   assign + seek       │      ▼  FieldMapper  (allowlist)     │   │    (keyed)         │
 │ Kafka topic    ├──────────────────────►│   Object[] in schema order           │   │                    │
 │  trades (json) │                       │      │                               │   │  kafka_trades      │
 ├────────────────┤   framed socket       │      ▼  DeltaRowMerger (publish DELTA)│  │    (append-only)   │
 │ TCP feed       ├──────────────────────►│      ▼  RowBatcher                    │  │  tcp_ticks         │
 │  ticks (json)  │                       │      ▼  FlightDeephavenGateway        ├─►│    (ring)          │
 └────────────────┘                       └──────────────────────────────────────┘   └────────────────────┘
                                                     ▲                                        │
                                                     └───── DeephavenLifecycleMonitor ◄────────┘
                                                            polls; a generation change
                                                            restarts every connector
```

One application, one Deephaven server, **one or more connectors**. Each connector is one
upstream feed bridged into one Deephaven table, with its own connection, subscription and
mapping. Everything to the right of `RecordSource` is transport-agnostic: the source decides
what a record *is*, and the rest of the pipeline never learns where it came from.

## 2. Configuration model (`application.yml`)

```yaml
dh-connectors:
  enabled: true
  deephaven:                     # one server, shared by every connector
    host: localhost
    port: 10000
    authentication: Anonymous
    console-type: python
    health-check-interval: 5s
  connectors:
    - name: orders-fix           # unique; also the default source client name
      enabled: true
      format: FIX                # FIX | NVFIX | JSON | COMPOSITE
      composite-parts: []        # COMPOSITE only: each part's format, in wire order (s5.3)
      source:                    # the transport: driver + EXACTLY ONE of amps/kafka/tcp (s11)
        driver: REAL             # REAL | SIMULATED
        simulated-rate: 5        # SIMULATED only: records per second
        simulated-keys: 8        # SIMULATED only: distinct keys cycled through
        amps:
          host: localhost        # or an explicit `uri:`
          port: 9007
          transport: tcp
          message-type: ""       # URI message type; defaults to the format's name. COMPOSITE
                                 # requires it: the server-registered composite type name
          topic: Orders
          sow: true              # SOW topic (keyed) vs journal topic (append-only)
          subscription-mode: FULL  # FULL | DELTA
          bookmark: epoch        # journal topics only
          filter: "/Symbol = 'AAPL'"
      transforms: []             # optional: RecordTransform bean names, applied in order (s12)
      deephaven:
        table: amps_orders       # the global name; must be an identifier
        table-type: KEYED        # KEYED | APPEND_ONLY | BLINK | RING; default follows the source
        ring-capacity: 100000    # RING only: rows retained
        publish-mode: FULL       # FULL | DELTA
        key-columns: [ClOrdID]   # KEYED only, and required by it
        ingest-timestamp-column: IngestTs
        source-key-column: SourceKey  # optional; the source's own key (AMPS SOW key, Kafka
                                      # message key). Refused for source.tcp: no such thing
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

Bound by `ConnectorsProperties` (`@ConfigurationProperties("dh-connectors")`) and validated at
startup by `ConnectorValidator` (§7).

The transport blocks are **siblings under `source:`, and exactly one is configured**. The block
that is present is what selects the driver — there is no `type:` discriminator to keep in step
with it — and `driver: SIMULATED` keeps the block rather than replacing it, so the demo
validates the same configuration the real deployment will (§10). The Kafka and TCP blocks are
spelled out in §11; the rest of this document is written against the AMPS block because it is
the one with the most to say, and every section from §4 on applies to all three unchanged.

## 3. Two independent choices: the source side and the Deephaven side

`source.amps.sow` decides how the topic is read. `deephaven.table-type` decides what it is read
*into*. They used to be the same decision; they are not any more.

The first question generalises past AMPS: **is this feed state, or a stream of events?**
`SourceProperties.stateful()` is that question asked transport-independently — `amps.sow` for
AMPS, `kafka.compacted` for Kafka, always `false` for a socket — and it is what the table-type
default below is actually drawn from (§11 has the full mapping).

### 3.1 The AMPS side — `source.amps.sow`

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

`source.amps.subscription-mode` is what **AMPS sends**; `deephaven.publish-mode` is what **we
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

**On the AMPS side**, `sow: true` plus `subscription-mode: DELTA` selects the delta form of
the SOW subscription:

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

Two optional synthetic columns are appended after the mapped fields: `source-key-column` (the
key the *source* gave the message) and `ingest-timestamp-column` (when the connector processed
it).

### 5.1 Keying on the source's own key

`source-key-column` may itself be named in `key-columns`, which is the answer for a feed whose
key the server assigns — an AMPS `KeyGenerator`, a Kafka producer's message key, or any
configuration where the key is not reconstructible from the record body:

```yaml
deephaven:
  table: amps_orders
  source-key-column: SourceKey
  key-columns: [SourceKey]     # key on what the source assigned, not on a mapped field
```

The key travels on the record as `SourceRecord.key` — `Message.getSowKey()` for AMPS, the
message key for Kafka, and `null` for a socket, which is why §7 refuses the setting for
`source.tcp` — and is written to that column like any other value, so the keyed table's
identity matches the source's exactly. It is also the only workable key for **removals**: an
out-of-focus message may carry no body at all and a Kafka tombstone carries none by
definition, so a key derived from mapped fields would have nothing to resolve against, while
the source key is still on the message.

The business fields stay mapped as ordinary columns — the source key is opaque, so it
identifies rows without describing them.

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

- **The framing is binary** (a 4-byte length prefix per part), so `AmpsRecordSource` cannot
  hand the payload on as one string. It unframes the message with the client's own
  `CompositeMessageParser` — reading the raw message, not `getData()` — and delivers the parts
  on `SourceRecord.parts()`; `CompositeRecordDecoder` then decodes each part with the decoder for
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

`explode` needs the **whole** record every time, so `source.amps.subscription-mode: DELTA` is
refused with it (§7): a delta that omitted the map would be indistinguishable from the map emptying. Delta
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
replays the state of the world, a journal topic replays from `epoch`, a Kafka source re-seeks
(§11), and the `DeltaRowMerger` forgets everything it remembered from the previous life. A
source that cannot replay — a raw socket — is the one case where the rebuilt table starts empty
and fills from the live stream; §11 says why that is a property of the feed, not a gap here.

Connectors are started independently and retried on every tick, so one unreachable server does
not hold up the others and recovers on its own.

## 7. Startup validation (`ConnectorValidator`)

Cross-field rules bean validation cannot express. All are checked once, and a failure stops the
application with the full list rather than a stack trace.

A rule that only one transport can satisfy **names that transport in its message**: with three
source kinds sharing one connector model, "requires a SOW topic" is only actionable once you
can see which block was supposed to provide one.

**The source:**

- `source` configures **exactly one** of `amps` / `kafka` / `tcp` — none means nothing would be
  dialled, several means one table fed from two feeds. The rule holds for `driver: SIMULATED`
  too: the block says what the simulator stands in for, and dropping it under the demo profile
  would let the demo validate configurations the real deployment rejects
- `format: COMPOSITE` and `composite-parts` require `source.amps`: composite message types are
  an AMPS feature, not a wire format the other transports frame
- `deephaven.source-key-column` is refused for `source.tcp` — a socket carries no per-message
  key, so the column could only ever be null (§5.1)
- `source.kafka.compacted: true` with a keyed table requires `key-columns` to **include**
  `source-key-column`: a tombstone carries no payload to rebuild the key from, so the message
  key has to be the key (§11)
- `transforms` entries are non-blank and unique — a transform is stateless, so applying it
  twice either does nothing or is a copy-paste slip (§12). An *unknown* name is not checked
  here: it fails the connector at start, the way an unreachable broker does

**Everything else:**

- connector names unique; tags and columns unique within a connector
- `deephaven.table` and every column name is an identifier — the table name is interpolated into
  generated python
- FIX tags are tag *numbers*
- the resolved `table-type` (§3.2) and `key-columns` agree: `KEYED` requires them, every other
  type forbids them. The message names the default when `table-type` was not configured, since
  that is where a mistake about whether the feed is stateful surfaces
- `key-columns` ⊆ mapped columns (plus the synthetic ones)
- `publish-mode: DELTA` requires a keyed table
- `table-type: BLINK` / `RING` requires `create-if-missing`: the only way into a blink table is
  the `TablePublisher` the bootstrap creates, so turning the bootstrap off leaves nothing able to
  publish at all
- `source.amps.subscription-mode: DELTA` requires a SOW topic **and** `publish-mode: DELTA`
  (§4). There is deliberately no "DELTA requires AMPS" rule: the setting lives *on* the AMPS
  block, so a connector without one cannot ask for deltas at all
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
  exploded tag in a JSON part) and `source.amps.subscription-mode: FULL` (§5.4); its
  `key-column` is an identifier, collides with nothing, and — on a keyed table — appears in `key-columns`, since
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

## 9. Module layout: framework + drivers + applications

The submodule separates the **framework** (a `java-library`), the **source drivers** (one
module per transport) and the **applications** built on them. An application is a deployment
unit: most are the generic runner plus one configuration directory, and a *custom* application
(own decoder, enricher) is its own Gradle module. This is what lets the fleet grow to dozens of
Spring Boot applications without dozens of modules: the build produces one framework, three
drivers and one runner artifact however many times they are deployed.

```
dh-connectors/
├── core/                               # :dh-connectors:core — java-library + test fixtures
│   └── src/main/java/com/fix42/dashboard/connectors/
│       ├── ConnectorsAutoConfiguration.java  # contributes the pipeline's beans to any Boot app
│       ├── config/                     # the dh-connectors: model + ConnectorValidator
│       ├── decode/                     # RecordDecoder: delimited (fix/nvfix) + json + composite
│       ├── transform/                  # RecordTransform SPI + TransformRegistry (§12)
│       ├── mapping/                    # TableSchema, FieldMapper, MappedRow, DeltaRowMerger, RecordExploder
│       ├── deephaven/                  # DeephavenGateway, FlightDeephavenGateway, TableBootstrapScript
│       ├── source/                     # the SPI: RecordSource, SourceRecord, SourceFactory,
│       │                               #   SourceResolver + the in-process SimulatedSource
│       └── runtime/                    # Connector, ConnectorManager, RowBatcher, DeephavenLifecycleMonitor
├── source-amps/                        # :dh-connectors:source-amps — 60East client (§11)
├── source-kafka/                       # :dh-connectors:source-kafka — kafka-clients consumer (§11)
├── source-tcp/                         # :dh-connectors:source-tcp — framed socket, java.net only (§11)
├── connector-app/                      # :dh-connectors:connector-app — the generic runner,
│   └── src/{main,test,integrationTest} #   which depends on ALL THREE drivers
│                                       # ConnectorApplication + baked defaults + demo profile;
│                                       # ApplicationYamlBindingTest, ConfigTreeTest; LiveTableTypeTest
├── apps/                               # custom-code applications, auto-discovered by settings.gradle.kts
├── config/<env>/<flow>/<app-name>/     # the DEPLOYABLE applications (application.yml each) +
│                                       # <env>/common/ for per-environment shared settings
├── docker/spring-boot.Dockerfile       # one shared layered-jar Dockerfile for every app image
└── scripts/dh-connectors-compose.sh    # generates + drives podman compose from the config tree
```

**Core knows nothing about any transport.** Each driver module carries its own client
dependency and contributes a `SourceFactory` through its own auto-configuration, so adding a
transport is adding a module to the classpath and an application carries only the clients it
actually dials. The generic runner depends on all three deliberately — it is the *one* image
the whole fleet deploys, and a driver missing from it would turn "someone wrote a different
`source:` block" into a startup failure. An app under `apps/` that only ever dials one broker
depends on just that module.

Core is deliberately **not** a Boot application (an executable module drags its main
class and baked yml onto every consumer's classpath); it contributes its beans through
`ConnectorsAutoConfiguration` and ships `TestConnectors`, `FakeRecordSource`,
`FakeSourceFactory` and `RecordingDeephavenGateway` as `testFixtures`. Applications apply the
`dh.connector-app` convention plugin (build-logic), which mandates web + actuator — the
container healthcheck depends on `/actuator/health` existing in all of them — and registers the `integrationTest`
suite and the `dockerBuildLocal` image task. Configuration layers baked defaults →
`config/<env>/common/` → `config/<env>/<flow>/<app-name>/` (`SPRING_CONFIG_ADDITIONAL_LOCATION`,
later wins); a connector list may only ever live in the instance file, because two
`dh-connectors.connectors` lists merge by index — `ConfigTreeTest` enforces that and binds +
validates every file in the tree.

Dependency versions are pinned to what the rest of the repo already runs against:
`io.deephaven:deephaven-java-client-flight-dagger:42.4` matches
`ghcr.io/deephaven/server:42.4` in `docker/docker-compose.yml`, and Arrow 18.3.0 matches the
version that client publishes with; the driver modules take their client versions the same way
(`com.crankuptheamps:amps-client:5.3.4.1`, and `kafka-clients` from the Spring Boot BOM).
Arrow needs `--add-opens=java.base/java.nio=ALL-UNNAMED` on JDK 21; `bootRun` and `test` set
it, and the runbook gives it for `java -jar`.

## 10. The simulated source

`source.driver` picks the implementation: `REAL` dials whichever transport block the connector
configures, `SIMULATED` runs an in-process generator instead. AMPS in particular is commercial
software with no public image, which is why the demo profile and the end-to-end tests need
one.

The driver is a **sibling** of the transport blocks, not a replacement for one: a simulated
connector still names `amps:`, `kafka:` or `tcp:`, still goes through every §7 rule, and is one
line away from being the real thing. Its two knobs live beside it, because they describe the
generator rather than any transport:

```yaml
source:
  driver: ${source-driver:REAL}   # the demo profile flips this one placeholder
  simulated-rate: 5               # records per second
  simulated-keys: 8               # distinct keys cycled through
  amps: { host: localhost, port: 9007, topic: Orders, sow: true }
```

It synthesises payloads in the connector's **own** format from the connector's **own** field
mappings — nesting dotted JSON tags so a mapping like `execution.venue` resolves — and replays
one record per key on start when the feed is stateful (§11), the analogue of the SOW replay
that rehydrates the table. Decode, transforms, mapping, delta merge, batching and the Deephaven
publish all run exactly as they do against a real server; only the bytes' origin differs.

It also honours the §5.2 knobs, so the demo shows them working rather than inert: a field with a
code → value table gets one of its **codes** (`BUY`, not `Side-3`), and a field with a
`default-value` is left out of one record in four so the default has something to cover.

Both index on the emitted `tick` alone, not on `key + tick`. The runtime derives `key` from the
same counter as `tick`, so their sum has a fixed parity for an even `simulated-keys` — an index
built on it addressed only half of a two-entry table, and fired the omission on every record or
none. `SimulatedSourceTest` drives a real replay rather than calling `encode` with values
of its own choosing, because that correlation is invisible to a test that picks both.

A `COMPOSITE` connector's mappings are routed to the part their index prefix names and each
part is rendered in its own format, handed over unframed — the shape the real subscriber
produces after `CompositeMessageParser`. An `explode` connector gets an object at its tag whose
membership **shifts across ticks** (each candidate member sits out one tick in five), so the
exploder's vanish-deletes run in the demo rather than only its upserts.

That is what makes the `demo` profile and the end-to-end tests runnable with nothing but a
Deephaven container. It is a test and demo affordance, not a production path.

## 11. Sources: one pipeline, three transports

A source's whole job is to answer one question — *what is a record?* — and hand the answer to
the pipeline as a `SourceRecord(data | parts, key, action)`. Everything downstream is written
against that record and nothing else, which is why the three transports share a decoder, a
mapper, a merger, a batcher and four table types between them.

The SPI is four types in `core`:

| type | contract |
|---|---|
| `SourceRecord` | one message before decoding: payload (or composite parts), the source's key or `null`, and `UPSERT` / `DELETE` |
| `RecordSource` | a live subscription: `start(RecordHandler)`, `isConnected()`, `close()`. **It owns its reconnect behaviour** — once started, a dropped connection is the implementation's problem, not the caller's |
| `SourceFactory` | `supports(ConnectorProperties)` / `create(...)`. In practice `supports` is "is my block under `source:` non-null" |
| `SourceResolver` | `SIMULATED` short-circuits to `SimulatedSource`; otherwise **exactly one** factory must claim the connector. Zero means the driver module is not on the classpath; more than one means two transports were configured. Both would otherwise surface as an empty table, so both throw |

A driver module contributes its factory as a bean through its own auto-configuration, so
`core` holds no list of drivers and an application supports the transports it depends on.

### 11.1 The same ideas in three vocabularies

Every concept the pipeline pivots on exists in each transport under a different name. This is
the whole mapping:

| pipeline concept | AMPS | Kafka | TCP |
|---|---|---|---|
| **state** (`stateful()`, and the `KEYED` default) | a SOW topic (`amps.sow: true`) | a compacted topic (`kafka.compacted: true`) | — never; a socket has no state |
| **record key** (`source-key-column`) | the SOW key, `Message.getSowKey()` | the message key | — none; §7 refuses the setting |
| **removal** (`DELETE`) | `sow_delete`, or an out-of-focus (`oof`) message | a tombstone: a record with a **null value** | — never |
| **history** (what a first connect reads) | `bookmark: epoch` replays the transaction log | `from: EARLIEST` replays the retained log; `LATEST` starts at the end | the live stream, from the moment the socket opens |
| **rehydration** (§6, every restart) | the SOW replay, or the bookmark again | assign + seek to the beginning again | redial; nothing is replayed |
| **framing** | the message type, plus binary part prefixes for `COMPOSITE` (§5.3) | one record is one message | `DELIMITED` or `LENGTH_PREFIXED` (below) |

Read the "state" row and the "rehydration" row together and the design falls out: **a compacted
Kafka topic replayed from the beginning is the Kafka spelling of an AMPS SOW replay**, and a
tombstone is the Kafka spelling of an `oof`. That correspondence is why a Kafka connector needs
no concepts of its own — `compacted: true` sets `stateful()`, which defaults the table to
`KEYED`, which is what makes tombstones mean something.

### 11.2 Offsets: the connector owns its position

The Kafka source deliberately does **not** `subscribe()`. It assigns every partition of the
topic itself, seeks, and never commits — `enable.auto.commit=false` and no `group.id`.

That follows from what the connector is for: **the Deephaven table is the state**, not a
consumer group's offsets. `ConnectorManager` restarts every connector when the Deephaven server
bounces (§6) and each restart rebuilds the target table from nothing; resuming from a committed
offset would then leave the table holding whatever the topic published since, which is not the
state of the world. So rehydration builds a fresh source that re-seeks:

| configuration | seek | the AMPS bookmark it corresponds to |
|---|---|---|
| `compacted: true` | beginning, whatever `from` says — state has to be replayed | the SOW replay |
| `from: EARLIEST` | beginning | `epoch` |
| `from: LATEST` | end | `now` |

Not committing has a second consequence worth naming: two instances of the same connector
would each read *every* partition rather than splitting them, because there is no group to
balance. That is the correct behaviour here — two instances mean two tables — but it is the
opposite of what a consumer-group application expects.

Everything the class does not name goes in `kafka.properties`, applied **last** so an operator
can override anything (security, SSL, fetch sizing) without a code change. Credentials arrive
as environment placeholders, never as literals in the config tree.

### 11.3 TCP: framing, and nothing else

A socket delivers bytes, so the only real question is where one message ends:

| `framing` | on the wire |
|---|---|
| `DELIMITED` | messages separated by `delimiter`, matched as a **byte sequence** in the configured charset, so CRLF and other multi-byte separators work. A frame split across two reads is reassembled; empty frames (a trailing delimiter) are skipped |
| `LENGTH_PREFIXED` | a 4-byte big-endian length, then that many payload bytes — the same framing AMPS composite parts use. A length below 0 or above 16 MB is a **protocol error**, not a big message: once the stream is misframed every following length is garbage, so the source drops the connection and redials |

A delimiter that is a control character is written with the YAML escape for its code point,
never the byte itself — a literal control character does not survive an editor, a diff or a
paste.

Every frame becomes a keyless `UPSERT`. This source never emits a `DELETE`, because a raw feed
has no notion of a record leaving it, and it replays nothing on a redial. `RING`,
`APPEND_ONLY` and `BLINK` are therefore its natural targets — a bounded view of a live stream
is exactly what the transport can back. A feed that must survive a restart belongs behind a
broker.

### 11.4 Scope: when this module earns its keep

Deephaven ingests Kafka natively — `deephaven.stream.kafka.consumer.consume` builds a blink
table from a topic in a few lines of python, inside the server, with no application to deploy.
When that is all you need, use it.

This module earns its keep when you want what sits *between* the wire and the table: the
mapping allowlist, `decode` / `values` / `default-value` shaping (§5.2), `explode` (§5.4),
startup validation of the whole configuration (§7), transforms (§12), and input-table **delete**
semantics — plus the same configuration language across AMPS, Kafka and a socket, and one
runtime whose restart contract is written down (§6). The overlap with `kc.consume` is real and
worth being honest about; the answer is "use the connector when the table needs shaping the
python one-liner would have to grow into".

## 12. Transforms: `RecordTransform`

A per-record rewrite of the decoded tag namespace, named by a connector's `transforms:` list:

```yaml
transforms: [normalise-venue, derive-notional]
```

Each name resolves to a `RecordTransform` bean, and they are folded over the record **in the
order listed**, between decode and explode/map:

```
decode → transform₁ → transform₂ → explode/map → merge → batch → publish
```

Working in the `tag -> value` space is the point of the seam. A tag a transform derives is
mappable by an ordinary `fields:` entry and goes through the same allowlist, coercion, shaping
and validation as one the payload carried — so an enrichment costs a bean plus a mapping, not
a fork of the pipeline. It is the same idea as a Kafka Connect single-message transform, and
the same shape: stateless, per-record, composable.

The contract:

- **Stateless.** One instance serves every connector that names it, it is called on the
  source's delivery thread, and every restart replays the feed from the beginning (§6). A
  transform that accumulated anything would accumulate it twice.
- **`null` drops the record** — the filtering spelling. The connector counts it as *dropped*
  rather than *rejected*: it is a decision, not a failure.
- **`DELETE` records go through too.** A delete's key columns are extracted from its fields the
  same way an upsert's are, so a transform that derives a key column has to derive it for
  deletes as well, or the removal cannot be addressed.
- **The map passed in may be immutable.** Return a new map rather than mutating the argument.
- **An unknown name fails the connector at start**, not the application at build:
  `TransformRegistry.resolve` throws naming the missing bean and listing what is registered,
  and `Connector.start` resolves before it creates anything. §7 catches only blanks and
  duplicates, which can never be right whatever beans exist.

Custom transforms are code, so they live in a custom application module under `apps/` (§9) —
the generic runner registers none, and an application with no transforms gets an empty
registry rather than a missing-bean failure.

**Where the line is.** A transform is the right home for anything decidable from the record in
front of it: renaming a venue code, splitting a compound field, deriving `notional = qty ×
price`, dropping heartbeats. **Stateful** enrichment — joining against another feed, an as-of
lookup against a price table, aggregation — belongs in **Deephaven**, as a table operation
downstream of the connector's table. Deephaven is an incremental join engine and the connector
is not; a transform that reached for another table's state would be re-implementing, badly,
the thing the rows are being published into.

## 13. Testing

```bash
./gradlew :dh-connectors:core:test :dh-connectors:source-amps:test \
          :dh-connectors:source-kafka:test :dh-connectors:source-tcp:test \
          :dh-connectors:connector-app:test
```

**292 JUnit 5 tests, no servers required** — core 236, source-amps 10, source-kafka 9,
source-tcp 7, connector-app 30 — plus 6 opt-in tests that need one (`LiveTableTypeTest`, in the
runner's `integrationTest` suite, below). No transport test needs a broker: the Kafka suite
drives a `MockConsumer` and the TCP suite a loopback `ServerSocket` the test starts itself.

| Suite | Covers |
|---|---|
| `ColumnTypeTest` | alias parsing, coercion per type, the three timestamp encodings, blank-is-null |
| `ConnectorValidatorTest` | every §7 rule, including the destructive DELTA/FULL combination and the one-transport rules |
| `SourceResolverTest` | §11: `SIMULATED` short-circuits, exactly one factory claims a connector, zero and two both throw |
| `DelimitedRecordDecoderTest` / `JsonRecordDecoderTest` | delimiters, first-`=` split, dotted paths, explicit JSON null |
| `CompositeRecordDecoderTest` / `CompositeWireRoundTripTest` | §5.3: part-indexed tags, bare aliases, part-count leniency; the 60East builder → parser framing contract the subscriber relies on |
| `RecordExploderTest` | §5.4: member rows, `.` scalars, dotted member names, vanish/clear/OOF deletion, SOW-key vs key-column identity, unkeyed targets |
| `TableSchemaTest` / `FieldMapperTest` | column order, allowlist behaviour, present-vs-null, composite keys |
| `DeltaRowMergerTest` | merge, explicit clear, per-key independence, delete forgets the key |
| `ValueShapingTest` | §5.2: decode, inline overrides, pass-through of unknown codes, defaults, and that a default seeds a delta's base row without ever clobbering it |
| `DeephavenTableTypeTest` | the four types, the default drawn from the source's `stateful()`, and the schema each resolves to |
| `TableBootstrapScriptTest` | the generated python for all four types, dtypes, idempotence, the column/type/key checks, the per-batch scratch name |
| `RowBatcherTest` | size and timer flush, upsert/delete run ordering, failure is counted not thrown |
| `ConnectorTest` | the per-message pipeline for all three formats, transforms included (order, drop, DELETE) |
| `AmpsRecordSourceTest` | §11: the AMPS command each topic/mode resolves to, URI and bookmark building — no server |
| `KafkaRecordSourceTest` | §11: upserts keyed by the message key, tombstone → DELETE, where each configuration seeks, assign-not-subscribe, passthrough properties, close — a `MockConsumer`, no broker |
| `TcpRecordSourceTest` | §11.3: delimited and length-prefixed framing, a frame split across two writes, a multi-byte delimiter, redial after the peer hangs up, close — a loopback `ServerSocket` |
| `ConnectorManagerTest` | **the §6 lifecycle contract**: start, steady state, restart-rehydrate, unavailable, per-connector retry |
| `SimulatedSourceTest` | §10: the replay, the generated payloads decoding back to every mapped tag, the §5.2 knobs |
| `EndToEndPipelineTest` | the whole application with the simulated source and a recording gateway |
| `ApplicationYamlBindingTest` | the shipped demo examples bind, mean what this doc says, and validate |
| `ConfigTreeTest` | every `config/<env>/<flow>/<app>/application.yml` binds, layers and validates the way the container will run it; the tree's cross-file rules (§9) |
| `LiveTableTypeTest` | **opt-in** (`integrationTest`): the generated python and both publish paths, against a real server |

`LiveTableTypeTest` is the one suite the fakes cannot stand in for — a table type that has to be
built out of generated python is only correct if a server says so. It is skipped unless you ask:

```bash
podman run -d --name dh -p 10000:10000 \
  -e START_OPTS="-Ddeephaven.console.type=python \
     -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler" \
  ghcr.io/deephaven/server:42.4
./gradlew :dh-connectors:connector-app:integrationTest -Damps.live=true
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
