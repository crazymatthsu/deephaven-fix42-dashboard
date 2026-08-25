# AMPS connectors

Spring Boot sidecar that maps one or more AMPS topics into Deephaven
tables. Lives in `:amps-connectors`. It is **not** the FIX 4.2 state
machine; that remains in `:oms-engine`. This module is a configured
ingest path: AMPS payload → selected fields → Deephaven input table.

## Why a sidecar

Deephaven Community `TablePublisher` / input tables are created in the
server query scope. The AMPS Java client is a long-lived TCP subscriber.
A Spring Boot process next to Deephaven:

1. Waits until Deephaven gRPC is up (PSK).
2. Creates each mapped table if it does not exist.
3. Subscribes to AMPS and **rehydrates**:
   - SOW: `sow_and_subscribe` / `sow_and_delta_subscribe` (snapshot + live).
   - Journal: bookmark `EPOCH` (`0`) when the table was just created.
4. If Deephaven restarts, the supervisor tears down AMPS subscriptions,
   waits for Deephaven, recreates tables, and rehydrates again.

Compose starts this service with Deephaven (`depends_on` + healthcheck).
`restart: on-failure` plus the supervisor reconnect loop cover both
process and session loss.

## Config (`application.yml`)

One process, many connectors:

```yaml
deephaven:
  host: localhost
  port: 10000
  psk: deephaven

amps:
  default-host: localhost
  default-port: 9007
  connectors:
    - name: orders-json
      enabled: true
      host: amps.example            # optional; falls back to default-host
      port: 9007
      topic: ORDERS
      topic-kind: sow               # sow | journal
      data-format: json             # fix | nvfix | json
      subscriber-mode: delta        # delta | full
      publisher-mode: delta         # delta | full (keyed tables)
      table-name: amps_orders
      key-columns: [OrderId]        # required for sow
      filter: ""                    # optional AMPS content filter
      fields:
        - source: OrderId           # json name, nvfix tag, or FIX tag number
          column: OrderId
          type: string
```

URI used to connect: `tcp://{host}:{port}/amps/{message-type}` where
`message-type` defaults to the data format (`json` / `fix` / `nvfix`).
Override with `uri:` or `message-type:`.

### Mapping rules

- Only `fields` entries are published. Extra AMPS tags/JSON keys are dropped.
- `type` is one of: `string`, `byte`, `short`, `int`, `long`, `float`,
  `double`, `boolean`, `char`, `instant`.
- JSON `source` may be a top-level name, dotted path (`order.id`), or
  JSON pointer (`/order/id`).
- FIX `source` must be the numeric tag (`"55"`).
- NVFIX `source` is the tag name (`Symbol`).
- AMPS FIX/NVFIX accept SOH or `|` delimiters; last value wins.

### SOW vs journal

| AMPS topic | AMPS command | Deephaven table |
|------------|--------------|-----------------|
| `topic-kind: sow` | `sow_and_subscribe` or `sow_and_delta_subscribe` + `oof,send_keys` | **keyed** input table (`key-columns`) |
| `topic-kind: journal` | `subscribe` / `delta_subscribe` from `EPOCH` if the table was created this cycle, else `NOW` | **append-only** input table |

Out-of-focus (`oof`) on a SOW topic deletes the key from the keyed table.

### Delta vs full

- **AMPS subscriber `delta`**: SOW uses `sow_and_delta_subscribe` so AMPS
  sends key + changed fields after the initial snapshot.
- **AMPS subscriber `full`**: each publish is the whole record.
- **Deephaven publisher `delta`** (keyed only): merge changed columns into
  the last published row so omitted fields are not wiped. Initial SOW
  records are always treated as a full snapshot.
- **Deephaven publisher `full`**: every publish replaces the whole row;
  mapped columns missing from the payload become null.

Journal tables always append; missing mapped columns are null.

Journal replay from the beginning happens when Deephaven is empty (table
created now). If only the connector restarts and the table already
exists, the journal subscription starts at `NOW` to avoid duplicates.

## Lifecycle

```
compose up
   ├─ redpanda
   ├─ deephaven   (health: gRPC :10000)
   └─ amps-connectors
         wait until Deephaven PSK session opens
         for each enabled connector:
            create table if missing
            connect AMPS, subscribe, rehydrate
         on Deephaven drop → close AMPS → wait → repeat
```

Enable example connectors by setting `enabled: true` and pointing
`amps.default-host` / `AMPS_HOST` at a running AMPS server. This repo
does not ship an AMPS broker (60East is licensed separately).

## Run

```bash
./gradlew :amps-connectors:test
./gradlew :dh-app:prepareDeephavenImage :amps-connectors:bootJar
podman compose -f compose/compose.yaml up --build
```

Standalone:

```bash
./gradlew :amps-connectors:bootRun
```

JVM needs `--add-opens=java.base/java.nio=ALL-UNNAMED` (set in `bootRun`
and the compose image) for Arrow.
