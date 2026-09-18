# `amps-connectors`

A **framework** for subscribing to [60East AMPS](https://www.crankuptheamps.com/) topics and
publishing the fields you map into Deephaven tables, plus the **Spring Boot applications** built
on it. One application runs one or more connectors; everything is driven from configuration.

Design and contract: [../docs/07-amps-connectors.md](../docs/07-amps-connectors.md).

---

## Layout: framework vs applications

```
amps-connectors/
├── framework/        :amps-connectors:framework — the pipeline as a java-library
│                     (decode, map, explode, batch, publish) + auto-configuration
│                     + test fixtures. Not a Boot app: apps depend on it.
├── connector-app/    :amps-connectors:connector-app — the GENERIC runner. One image,
│                     deployed N times: a config-only application is this app under
│                     its own name with its own mounted configuration.
├── apps/             custom-code applications (auto-discovered by settings.gradle.kts);
│                     the escape hatch when an app needs its own decoder/enricher.
│                     See apps/README.md. Empty is the normal state.
├── config/           the DEPLOYABLE applications: config/<env>/<flow>/<app-name>/
│   └── local/        one directory per application; `common/` per env for shared
│       ├── common/   endpoints. Adding application #51 = mkdir + one application.yml.
│       ├── default/{orders-and-positions,trades-and-ticks}/
│       └── cache/{portfolios,orders-composite}/
├── docker/           ONE shared spring-boot.Dockerfile for every app image
└── scripts/          dh-connectors-compose.sh — generates + drives podman compose
                      from the config tree
```

Configuration layers, later sources winning (**the jar knows no environment**):
baked `application.yml` (safe defaults, no connectors) → `config/<env>/common/` →
`config/<env>/<flow>/<app-name>/`, the mounted pair arriving as
`SPRING_CONFIG_ADDITIONAL_LOCATION=file:/app/config/common/,file:/app/config/instance/`.
Endpoints are `${AMPS_HOST:localhost}`-style placeholders so the same files serve IDE runs
and containers. Never define `amps.connectors` in `common/` — two lists merge **by index**
— and never bake one; `ConfigTreeTest` enforces the tree's rules (every instance binds and
validates, one table per app per env, no credentials) without booting anything.

---

## Architecture and data flow

One connector = one AMPS subscription = one Deephaven table. Everything between the two ends is
format-agnostic: the wire format only decides how a payload becomes `tag → value`, and the table
type only decides what the rows land in — so any format can feed any table type.

```mermaid
flowchart LR
    subgraph AMPS["AMPS server — source.topic + source.sow"]
        direction TB
        SOW["SOW topic (sow: true)<br/>state of the world: last record per SOW key<br/>replayed with sow_and_subscribe (+OOF, so<br/>deletes and filter-exits arrive as out-of-focus)"]
        JRN["journal topic (sow: false)<br/>transaction log, no SOW<br/>subscribe from the epoch bookmark:<br/>every restart replays everything"]
    end

    subgraph FMT["AMPS data type — format: (how one payload reads)"]
        direction TB
        FIX["FIX<br/>tag=value pairs, SOH-delimited<br/>tags are FIX tag numbers: 54=1<br/>built-in decode tables turn codes into names"]
        NVFIX["NVFIX<br/>Name=value pairs in FIX framing<br/>tags are field names: Side=1"]
        JSONF["JSON<br/>one object; tags are names or dotted paths<br/>(execution.venue); a nested object is also<br/>addressable whole, as its JSON text"]
        COMP["COMPOSITE (composite-local / composite-global)<br/>several length-prefixed parts, each of a constituent<br/>format listed in composite-parts: [JSON, FIX]<br/>part-indexed tags: 0.orderId, 1.54 — like AMPS's own<br/>/0/orderId XPaths; a bare tag reads the merged<br/>namespace, first part wins (the composite-global spelling)"]
    end

    SUB["AmpsClientSubscriber (HAClient:<br/>reconnect + resubscribe survive AMPS restarts)<br/>or SimulatedAmpsSubscriber (demo profile)<br/>COMPOSITE: CompositeMessageParser unframes the<br/>raw message — the binary length prefixes do not<br/>survive as a string"]

    DEC["RecordDecoder<br/>payload → tag → raw value<br/>a key present means the payload carried the field:<br/>absent ≠ empty, which is what delta correctness rides on"]

    MAP["FieldMapper — the allowlist<br/>only mapped tags are published<br/>type coercion per column, plus value shaping:<br/>decode: SIDE (54=1 → BUY) · values: inline rewrites<br/>default-value when the field is absent"]

    EXPL["RecordExploder (optional — explode:)<br/>an object with dynamic keys → one row per member<br/>member name → key-column, '.' = the member value<br/>remembers each record's members: a vanished member,<br/>a null value, or the record leaving the SOW → row deletes"]

    MRG["DeltaRowMerger (optional — publish-mode: DELTA)<br/>partial update merged over the last full row for its key,<br/>so omitted columns keep their stored values"]

    BAT["RowBatcher<br/>flush by max-batch-rows or flush-interval<br/>upserts and deletes, in arrival order"]

    GW["FlightDeephavenGateway<br/>Arrow Flight + generated python bootstrap<br/>creates the table if missing, refuses to adopt a<br/>table whose columns, types or keys disagree"]

    subgraph DH["Deephaven table type — deephaven.table-type"]
        direction TB
        KEYED["KEYED — input_table(key_cols=…)<br/>one row per key: an add is an upsert<br/>the only type with removal, so the only target for<br/>OOF / sow_delete, explode's deletes, DELTA merges<br/>default for SOW topics"]
        AO["APPEND_ONLY — input_table()<br/>every row kept forever: audit / history shape<br/>removals are ignored (counted, not published)<br/>default for journal topics"]
        BLINK["BLINK — table_publisher()<br/>rows live for one update-graph cycle, then vanish<br/>downstream aggregations see each row exactly once<br/>bounded memory"]
        RING["RING — ring_table(blink, ring-capacity)<br/>the last ring-capacity rows of the stream<br/>bounded for real: nothing upstream retains the rows"]
    end

    SOW --> SUB
    JRN --> SUB
    SUB --> DEC
    FMT -. "format: selects the decoder" .-> DEC
    DEC --> MAP
    MAP --> EXPL
    EXPL --> MRG
    MRG --> BAT
    BAT --> GW
    GW -->|"addToInputTable / delete rows"| KEYED
    GW -->|"addToInputTable"| AO
    GW -->|"TablePublisher.add"| BLINK
    GW -->|"TablePublisher.add"| RING
```

Reading the two ends against each other:

- **Any format can feed any table type** — the middle of the pipeline never asks which format
  produced the fields, and never asks which table type will receive the row.
- **The `sow` flag and the table type are independent choices.** Left unset, `table-type`
  follows the topic (SOW → `KEYED`, journal → `APPEND_ONLY`); set it to override — a SOW topic
  rendered as `BLINK` is a live view of updates rather than of state, a journal topic as `RING`
  is a bounded tail of the log.
- **Removal only exists on `KEYED`**, which is why everything that deletes — out-of-focus
  messages, `sow_delete`, `explode`'s vanished members, `DELTA` merging — requires it.
- **`BLINK`/`RING` go through a different publish path** (a server-side `TablePublisher`, not an
  input table), which is why they require `create-if-missing`: the bootstrap is the only thing
  that can create their publisher.

## Run

The demo profile — the six documented example connectors with every source swapped for the
in-process simulator, so the full pipeline runs against Deephaven alone:

```bash
./gradlew :amps-connectors:connector-app:bootRun --args="--spring.profiles.active=demo"
```

A deployable application from the config tree, from the IDE/CLI (endpoints default to
`localhost`; the bare app without extra config boots idle with zero connectors):

```bash
./gradlew :amps-connectors:connector-app:bootRun --args="--spring.config.additional-location=file:amps-connectors/config/local/common/,file:amps-connectors/config/local/cache/portfolios/"
```

As a jar (Arrow needs the `--add-opens` on JDK 21; the image bakes the same flag):

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED -jar amps-connectors/connector-app/build/libs/connector-app-0.1.0.jar --spring.profiles.active=demo
```

Any setting can be overridden on the command line, e.g. a Deephaven on another port:

```bash
./gradlew :amps-connectors:connector-app:bootRun --args="--spring.profiles.active=demo --amps.deephaven.port=10001"
```

### The fleet, under podman

`scripts/dh-connectors-compose.sh` generates a compose file from `config/<env>/` — one service
per application directory, each flow a compose profile — and drives `podman compose` with it:

```bash
amps-connectors/scripts/dh-connectors-compose.sh local build      # gradle → podman images
amps-connectors/scripts/dh-connectors-compose.sh local up cache   # one flow's apps
amps-connectors/scripts/dh-connectors-compose.sh local up         # every flow
amps-connectors/scripts/dh-connectors-compose.sh local ps
amps-connectors/scripts/dh-connectors-compose.sh local logs portfolios
amps-connectors/scripts/dh-connectors-compose.sh local down
```

Services publish no ports (connectors are outbound clients; the actuator healthcheck runs
inside the container network) and dial AMPS/Deephaven on the host via
`host.containers.internal` — override with `AMPS_HOST`/`DEEPHAVEN_HOST`. Config-only apps run
`localhost/dh-connector-app:local`; an app with a module under `apps/<name>/` runs
`localhost/dh-<name>:local` instead. Mind the podman machine's memory before starting many
flows at once: each app is a JVM capped by `DH_CONNECTOR_MEM` (default `384m`), so a 6 GB VM
comfortably runs a flow or two, not fifty apps.

## Test

```bash
./gradlew :amps-connectors:framework:test :amps-connectors:connector-app:test
```

247 tests, no AMPS server and no Deephaven server required: 224 framework unit tests plus the
configuration tests (the shipped demo examples in `ApplicationYamlBindingTest`, the whole
`config/` tree in `ConfigTreeTest`). Six more check the generated python against a real server;
they live in the `integrationTest` suite and are skipped unless you ask for them:

```bash
podman run -d --name dh -p 10000:10000 \
  -e START_OPTS="-Ddeephaven.console.type=python \
     -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler" \
  ghcr.io/deephaven/server:42.4
./gradlew :amps-connectors:connector-app:integrationTest -Damps.live=true
```

### Running suites manually

Add `--rerun` whenever you re-run by hand: without it Gradle sees nothing changed and skips
the task as up-to-date, which looks like the tests passed instantly when in fact nothing ran.

One feature at a time, by suite:

```bash
# Composite message types: part-indexed decoding, and the 60East builder -> parser
# framing contract the subscriber relies on
./gradlew :amps-connectors:framework:test --rerun --tests '*CompositeRecordDecoderTest' --tests '*CompositeWireRoundTripTest'
```

```bash
# explode: member rows, '.' scalars, dotted member names, vanish/clear/OOF deletion
./gradlew :amps-connectors:framework:test --rerun --tests '*RecordExploderTest'
```

```bash
# The configuration rules and the simulator's composite/explode payloads
./gradlew :amps-connectors:framework:test --rerun --tests '*ConnectorValidatorTest' --tests '*SimulatedAmpsSubscriberTest'
```

```bash
# The shipped demo examples bind and validate; every config/<env>/<flow>/<app>/ does too
./gradlew :amps-connectors:connector-app:test --rerun --tests '*ApplicationYamlBindingTest' --tests '*ConfigTreeTest'
```

The HTML reports land at `amps-connectors/framework/build/reports/tests/test/index.html` and
`amps-connectors/connector-app/build/reports/tests/test/index.html`.

There is no AMPS-side live suite to ask for — AMPS has no public image — which is why the
wire-level facts are pinned where they can be: `CompositeWireRoundTripTest` exercises the real
60East client's framing, and everything downstream runs against the simulator.

### Watching it instead of asserting it

The demo profile runs the full pipeline against nothing but the Deephaven container above:

```bash
./gradlew :amps-connectors:connector-app:bootRun --args="--spring.profiles.active=demo"
```

Then open http://localhost:10000/ide and watch:

- **`amps_composite`** — one row per key, `OrderId`/`Account` from the JSON part joined with
  `Side`/`Qty`/`Price` from the FIX part, `Side` showing decoded names (`BUY`, `SELL_SHORT`,
  ...) rather than wire codes.
- **`amps_portfolios`** — one row per map member, member names in `Symbol`, `Position`
  carrying the member value as JSON text. The simulator shifts membership across ticks, so
  rows for a given `OuterKey` appear *and disappear* — the disappearances are the exploder's
  vanish-deletes running, the part a screenshot cannot show.

## Configure

The demo profile (`connector-app/src/main/resources/application-demo.yml`) is the worked,
commented example of all four formats and four table types; the same six connectors, grouped
into four deployable applications, live under `config/local/`:

| Connector | Format | AMPS topic | Deephaven table |
|---|---|---|---|
| `orders-fix` | FIX, full subscription | `Orders` (SOW) | `amps_orders`, **keyed** on `ClOrdID` |
| `positions-nvfix` | NVFIX, **delta** subscription and publish | `Positions` (SOW) | `amps_positions`, **keyed** on `Account`+`Symbol` |
| `trades-json` | JSON, from the `epoch` bookmark | `Trades` (journal) | `amps_trades`, **append-only** |
| `ticks-json` | JSON, from the `epoch` bookmark | `Ticks` (journal) | `amps_ticks`, **ring**, 5 000 rows |
| `portfolios-json` | JSON with **`explode`**: a row per map entry | `cache.entries` (SOW) | `amps_portfolios`, **keyed** on `OuterKey`+`Symbol` |
| `orders-composite` | **COMPOSITE** (`[JSON, FIX]` parts), part-indexed tags | `orders.composite` (SOW) | `amps_composite`, **keyed** on `OrderId` |

### Table types

`deephaven.table-type` picks what gets created. Left unset it follows the topic — `KEYED` for a
SOW topic, `APPEND_ONLY` for a journal topic — which is what this module did before the setting
existed, so existing configuration keeps its behaviour.

| `table-type` | what you get | retains | removals |
|---|---|---|---|
| `KEYED` | `input_table(key_cols=…)`; an add replaces that key's row | one row per key | yes |
| `APPEND_ONLY` | `input_table()`; every row appended | everything | no |
| `BLINK` | a blink table fed by a `TablePublisher` | one update cycle | no |
| `RING` | `ring_table` over that blink table | the last `ring-capacity` rows | no |

`BLINK` and `RING` bound memory for real: nothing upstream keeps the rows. `KEYED` is the only
type that can apply an out-of-focus removal, and the only one `publish-mode: DELTA` can merge
into.

Add a connector by appending to `amps.connectors` in an application's file (a new application
is a new `config/<env>/<flow>/<app-name>/application.yml`). The essentials:

```yaml
amps:
  connectors:
    - name: my-connector
      format: NVFIX                 # FIX | NVFIX | JSON
      source:
        host: amps.example.com
        port: 9007
        topic: MyTopic
        sow: true                   # SOW topic (state) vs journal topic (log)
        subscription-mode: FULL     # DELTA makes AMPS send only changed fields
      deephaven:
        table: my_table             # the global name in the Deephaven IDE
        table-type: KEYED           # KEYED | APPEND_ONLY | BLINK | RING; default follows `sow`
        key-columns: [Id]           # required by KEYED, forbidden by everything else
        publish-mode: FULL          # must be DELTA if subscription-mode is DELTA
      fields:                       # an allowlist -- anything not listed is never published
        - { tag: Id,    column: Id,    type: STRING }
        - { tag: Price, column: Price, type: DOUBLE }
```

### Making values readable, and filling in the gaps

Three optional per-field knobs sit between the payload and the column:

```yaml
      fields:
        # 1 -> BUY, 2 -> SELL, 5 -> SELL_SHORT, ... (the full FIX 4.2 table)
        - { tag: "54", column: Side,    type: STRING, decode: SIDE }
        # published when the payload does not carry the field at all
        - { tag: "1",  column: Account, type: STRING, default-value: DUMMY }
        # inline rewrites, applied over `decode` -- for a feed the tables do not cover
        - tag: side
          column: Side
          type: STRING
          values: { "B": BUY, "S": SELL }
```

`decode` names a built-in FIX 4.2 code → name table: `SIDE` `ORD_STATUS` `EXEC_TYPE`
`EXEC_TRANS_TYPE` `ORD_TYPE` `TIME_IN_FORCE` `MSG_TYPE` `HANDL_INST` `SETTLMNT_TYP` `OPEN_CLOSE`
`ORD_REJ_REASON` `CXL_REJ_REASON` `CXL_REJ_RESPONSE_TO`. A code the table does not name passes
through unchanged, so an unrecognised value stays visible instead of becoming null.

`default-value` is written as the finished value, not a wire code — it is coerced to `type` but
never passed through `decode`, and a default that does not coerce is rejected at startup. Two
deliberate limits: a field the payload sends **empty** is not defaulted (that is an explicit
clear), and a **key column** may not have one, since every record missing the key would then
share the default and collapse onto a single row.

Full reference: [docs 07 §5.2](../docs/07-amps-connectors.md).

### Composite message types

`format: COMPOSITE` subscribes to an AMPS composite message type (`composite-local` /
`composite-global`): one message, several length-prefixed parts, each of a constituent format
listed in `composite-parts`. Tags are part-indexed — `0.orderId`, `1.54` — the same addressing
as the `/0/orderId` XPaths AMPS filters and SOW keys use; an unprefixed tag reads the merged
namespace, the natural spelling for `composite-global`. `source.message-type` must name the
type the **server** registers (it goes in the connection URI). [Docs 07 §5.3](../docs/07-amps-connectors.md).

### A row per map entry

`explode` renders a map with dynamic keys — `{"key": "portfolio-1", "value": {"AAPL": {...},
"MSFT": {...}}}` — as one Deephaven row per member: the member name lands in `key-column`, the
explode `fields` resolve inside the member's value (`"."` is the value itself), and on a keyed
table the connector deletes rows for members that vanish from a republished record, for a
`"value": null` clear, and for records leaving the SOW. [Docs 07 §5.4](../docs/07-amps-connectors.md).

To key on the SOW key AMPS assigns — the case for a topic with a `KeyGenerator`, where the key
cannot be rebuilt from the record body — name the SOW key column in `key-columns`:

```yaml
      deephaven:
        sow-key-column: SowKey
        key-columns: [SowKey]
```

`tag` is a FIX tag number for `FIX`, a field name for `NVFIX`, and a field name or dotted path
(`execution.venue`) for `JSON`. `type` is one of `STRING BOOLEAN BYTE SHORT INT LONG FLOAT DOUBLE
CHAR INSTANT`; `integer`, `bool` and `timestamp` bind too.

The full option list, with the reasoning behind each, is in
[docs 07 §2–§5](../docs/07-amps-connectors.md#2-configuration-model-applicationyml).

## What to expect at startup

```
Watching Deephaven at localhost:10000 every 5000ms for 4 connector(s)
Connected to Deephaven at localhost:10000 (generation 1)
[orders-fix] started: FIX Orders -> keyed table amps_orders (13 columns, publish FULL)
[positions-nvfix] started: NVFIX Positions -> keyed table amps_positions (7 columns, publish DELTA)
[trades-json] started: JSON Trades -> append-only table amps_trades (9 columns, publish FULL)
[ticks-json] started: JSON Ticks -> ring table amps_ticks (5 columns, publish FULL)
```

Open <http://localhost:10000/ide> and the tables are in the Panels menu.

## Troubleshooting

**Configuration is rejected at startup** — the message lists every problem at once:
```
invalid amps-connectors configuration:
  - connector 'x': deephaven.table-type=KEYED requires deephaven.key-columns (source.sow=true defaults deephaven.table-type to KEYED)
```
The rules and why each exists: [docs 07 §7](../docs/07-amps-connectors.md#7-startup-validation-connectorvalidator).

**`Deephaven at localhost:10000 is not available`** — the server is down or on another port. The
connectors stay stopped and the poll keeps retrying; nothing is lost, because reconnecting
replays every subscription from the start.

**A connector logs `start failed, retrying on the next health check`** — that connector's AMPS
server is unreachable. The others keep running and this one recovers on its own.

**Rows are rejected and nothing is published** — a keyed connector drops any record whose key
columns are not all populated, rather than collapsing them onto one row; the count shows in the
status line as `rejected`. Keying on `sow-key-column` when AMPS is not sending a SOW key does
this to every message.

**Columns come back null** — the field is not in `fields`, or its `tag` does not match what the
payload carries. For JSON, check whether the document is flat (`venue`) or nested
(`execution.venue`); both forms resolve, but the `tag` has to name one of them.

**`java -jar` fails inside Arrow** — the `--add-opens=java.base/java.nio=ALL-UNNAMED` flag is
missing. `bootRun` and `test` already set it.

**The app will not start: port 8080 already in use** — the actuator's web server needs a port
even though connectors are outbound-only clients. Pick another with `--server.port=0` for a
throwaway run; in containers each app has its own network namespace, so nothing to configure.

**An existing table has the wrong columns** — table creation refuses to adopt a table whose
columns disagree with the configuration:
```
[amps-connectors] orders-fix: existing table amps_orders has columns [...] but the connector is configured for [...]
```
Rename the table in configuration, or drop the global in the Deephaven console and let the
connector re-create it.

## AMPS server

AMPS is commercial software with no public image, so the demo stack in `../docker/` does not
include one. Point `source.host`/`source.port` (or `source.uri`) at your own server, or use
`source.driver: SIMULATED` — see
[docs 07 §10](../docs/07-amps-connectors.md#10-the-simulated-source).
