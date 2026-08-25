# `:amps-connectors`

Spring Boot application that subscribes to [60East AMPS](https://www.crankuptheamps.com/) topics
and publishes the fields you map into Deephaven tables. One application runs one or more
connectors; everything is driven from `application.yml`.

Design and contract: [../docs/07-amps-connectors.md](../docs/07-amps-connectors.md).

---

## Run

Against a Deephaven server on `localhost:10000` with a real AMPS server on `localhost:9007`:

```bash
./gradlew :amps-connectors:bootRun
```

Without an AMPS server — the `demo` profile swaps every source for the in-process simulator, so
the full pipeline runs against Deephaven alone:

```bash
./gradlew :amps-connectors:bootRun --args="--spring.profiles.active=demo"
```

As a jar (Arrow needs the `--add-opens` on JDK 21):

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED -jar amps-connectors/build/libs/amps-connectors-0.1.0.jar
```

Any setting can be overridden on the command line, e.g. a Deephaven on another port:

```bash
./gradlew :amps-connectors:bootRun --args="--spring.profiles.active=demo --amps.deephaven.port=10001"
```

## Test

```bash
./gradlew :amps-connectors:test
```

153 tests, no AMPS server and no Deephaven server required.

## Configure

The shipped `src/main/resources/application.yml` is a worked example of all three formats:

| Connector | Format | AMPS topic | Deephaven table |
|---|---|---|---|
| `orders-fix` | FIX, full subscription | `Orders` (SOW) | `amps_orders`, keyed on `ClOrdID` |
| `positions-nvfix` | NVFIX, **delta** subscription and publish | `Positions` (SOW) | `amps_positions`, keyed on `Account`+`Symbol` |
| `trades-json` | JSON, from the `epoch` bookmark | `Trades` (journal) | `amps_trades`, append-only |

Add a connector by appending to `amps.connectors`. The essentials:

```yaml
amps:
  connectors:
    - name: my-connector
      format: NVFIX                 # FIX | NVFIX | JSON
      source:
        host: amps.example.com
        port: 9007
        topic: MyTopic
        sow: true                   # SOW topic -> keyed table; false -> append-only
        subscription-mode: FULL     # DELTA makes AMPS send only changed fields
      deephaven:
        table: my_table             # the global name in the Deephaven IDE
        key-columns: [Id]           # required when sow: true, forbidden when sow: false
        publish-mode: FULL          # must be DELTA if subscription-mode is DELTA
      fields:                       # an allowlist -- anything not listed is never published
        - { tag: Id,    column: Id,    type: STRING }
        - { tag: Price, column: Price, type: DOUBLE }
```

`tag` is a FIX tag number for `FIX`, a field name for `NVFIX`, and a field name or dotted path
(`execution.venue`) for `JSON`. `type` is one of `STRING BOOLEAN BYTE SHORT INT LONG FLOAT DOUBLE
CHAR INSTANT`; `integer`, `bool` and `timestamp` bind too.

The full option list, with the reasoning behind each, is in
[docs 07 §2–§5](../docs/07-amps-connectors.md#2-configuration-model-applicationyml).

## What to expect at startup

```
Watching Deephaven at localhost:10000 every 5000ms for 3 connector(s)
Connected to Deephaven at localhost:10000 (generation 1)
[orders-fix] started: FIX Orders -> keyed table amps_orders (13 columns, publish FULL)
[positions-nvfix] started: NVFIX Positions -> keyed table amps_positions (7 columns, publish DELTA)
[trades-json] started: JSON Trades -> append-only table amps_trades (9 columns, publish FULL)
```

Open <http://localhost:10000/ide> and the tables are in the Panels menu.

## Troubleshooting

**Configuration is rejected at startup** — the message lists every problem at once:
```
invalid amps-connectors configuration:
  - connector 'x': source.sow=true requires deephaven.key-columns (a SOW topic maps to a keyed table)
```
The rules and why each exists: [docs 07 §7](../docs/07-amps-connectors.md#7-startup-validation-connectorvalidator).

**`Deephaven at localhost:10000 is not available`** — the server is down or on another port. The
connectors stay stopped and the poll keeps retrying; nothing is lost, because reconnecting
replays every subscription from the start.

**A connector logs `start failed, retrying on the next health check`** — that connector's AMPS
server is unreachable. The others keep running and this one recovers on its own.

**Columns come back null** — the field is not in `fields`, or its `tag` does not match what the
payload carries. For JSON, check whether the document is flat (`venue`) or nested
(`execution.venue`); both forms resolve, but the `tag` has to name one of them.

**`java -jar` fails inside Arrow** — the `--add-opens=java.base/java.nio=ALL-UNNAMED` flag is
missing. `bootRun` and `test` already set it.

**The process exits immediately** — `spring.main.keep-alive: true` was removed. There is no web
server and every connector thread is a daemon, so nothing else holds the JVM open.

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
