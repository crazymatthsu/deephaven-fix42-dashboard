# Deephaven FIX 4.2 Trading Dashboard

Live order-state cache and blotter built on Deephaven. Kafka carries raw
FIX 4.2 (`35=D,G,F,8,9,Q`). A Java state machine links identifiers and
applies venue state; Deephaven tables are the queryable cache and UI.

Work lives only under `grok/`. Analysis is in [`docs/`](docs/).

## Layout

| Module | Role |
|--------|------|
| `fix-codec` | FIX 4.2 parser / serializer |
| `oms-engine` | Linker, state machine, `OmsCache` query API |
| `fix-demo-producer` | Mock tape → Kafka |
| `dh-app` | Deephaven Application Mode (Python) |
| `compose` | Podman / Docker: Deephaven + Redpanda |

## Build and test

Requires Java 21+ and Gradle (wrapper included).

```bash
./gradlew test
./gradlew :dh-app:prepareDeephavenImage
```

## Local demo (Podman Desktop)

```bash
./gradlew :dh-app:prepareDeephavenImage
podman compose -f compose/compose.yaml up --build
```

`docker compose` works the same way.

1. Open http://localhost:10000/ide/ and sign in with PSK `deephaven`.
2. The `fix42_dashboard` app should already be running. Tables
   `orders_latest`, `executions`, `order_events` are also in the session.
3. In another terminal, publish the scripted tape:

```bash
./gradlew :fix-demo-producer:run --args="localhost:19092"
```

Type an `OrderKey` / `ClOrdID` (try `C1`, `B1`, `C1b`) or double-click a
blotter row. The executions panel and the new/amend/cancel history panel
filter to that order.

## Query API

Java (`OmsCache`): `getByClOrdId`, `getByOrderId`, `getByExecId`,
`findByAccount`, `findBySymbol`, `getChildren`, `getHistory`.

Same names are bound in Python Application Mode (`get_by_cl_ord_id`, …).

## Kafka contract

| | |
|--|--|
| Topic | `fix42.dropcopy` |
| Value | raw FIX 4.2 (`\|` or SOH) |
| In-compose bootstrap | `redpanda:9092` |
| Host bootstrap | `localhost:19092` |
