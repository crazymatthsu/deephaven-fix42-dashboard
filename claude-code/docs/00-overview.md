# Deephaven FIX 4.2 Trading Dashboard — Design Overview (authoritative)

This project consumes **FIX 4.2** order-flow messages (`35=D, G, F, 8, 9, Q`) from **Kafka**,
maintains a **latest-order-state cache** inside **Deephaven** via a FIX 4.2 state machine,
exposes a **query API** (by Account, Symbol, ClOrdID, OrderID, ExecID), and renders a
**real-time dashboard**: a master orders grid where clicking an order shows its executions
in one panel and its new/amend/cancel history in another.

It builds on the FIX 4.2 domain analysis from the `fix42-oms-cache` project
(`/Users/maojenhsu/ai-code/fix42-oms-cache/*/docs/`), re-targeted from a Java in-memory
cache to a **Deephaven streaming-table architecture**.

---

## 1. High-level architecture

```
┌──────────────────────┐   raw FIX 4.2 strings    ┌─────────────────────────────────────────────┐
│ fix-mock-generator   │   (SOH tag=value)        │ Deephaven server (python)                   │
│ (Java 21, Gradle)    ├──────► Kafka topic ──────►  kafka consume (blink)                      │
│ scenario engine +    │   fix42.messages         │    └► FixStateMachine listener (stateful)   │
│ KafkaProducer CLI    │   key = chain key        │        ├► pub: fix_messages   (audit)       │
└──────────────────────┘                          │        ├► pub: order_state    (snapshots)   │
                                                  │        ├► pub: executions     (per 35=8/Q)  │
         podman compose:                          │        └► pub: order_events   (lifecycle)   │
         apache/kafka (KRaft)                     │    derived DAG (declarative table ops):     │
         deephaven server + deephaven.ui          │      order_state_latest = last_by(OrderKey) │
                                                  │      indexes, summaries, filtered views     │
                                                  │    query API (python fns over tables)       │
                                                  │    deephaven.ui dashboard (3-panel linked)  │
                                                  └─────────────────────────────────────────────┘
```

Implementation modules:

| Module | Language | Role |
|---|---|---|
| `fix-mock-generator` | Java 21 (Gradle module) | Builds valid FIX 4.2 messages, generates realistic order lifecycles for every required scenario, publishes them to Kafka. |
| `deephaven-scripts` | Python (Gradle-wrapped submodule) | `fix42cache` — pure-python FIX parser + order state machine (no Deephaven imports, unit-testable). `dh_app` — Deephaven server scripts: ingestion (Kafka or an AMPS transaction log, `FIX42_SOURCE`), DAG, query API, dashboard. |
| `deephaven-app-java` | Java 21 (Gradle module) | The same app against the Deephaven **Java engine API**: `fixcache` (the state machine, no Deephaven imports) + `dh` (publishers, DAG, query API). Runs in the same server, exports the same globals, and is asserted by the same integration suite. Doc 06 §3's escape hatch, implemented. |
| `deephaven-app-multi-oms-blotter` | Python (Gradle-wrapped submodule) | `multi_oms` — four OMS drop-copy tapes linked across hubs and reconciled per edge in one server (doc 09). |
| `deephaven-remote-uri` | Python (Gradle-wrapped submodule) | `remote_uri` — the same flow across **several** servers: leaves fold hub tapes from AMPS and export projections; a collector re-links families through `deephaven.uri` / Barrage subscriptions, marks exposure against market data, answers (source OMS, account, symbol) lookups and fetches history from the owning leaf by remote query (doc 10). |

The two dashboard apps are alternatives, not layers: pick one with `DH_APP`
(`fix42-dashboard` or `fix42-dashboard-java`).

**Key architectural decision — hybrid DAG.** Deephaven's declarative table operations
(`where`, `last_by`, joins, aggregations) cannot express FIX chain resolution
(ClOrdID → OrigClOrdID amend chains, late-arriving OrderID, per-request reject
reverts, ExecID dedupe). That logic is inherently a *stateful fold* over the message
stream. So the DAG contains exactly **one stateful node** — a table listener running the
`fix42cache` state machine, republishing normalized rows through `TablePublisher`s —
and everything upstream/downstream of it is declarative, incrementally-computed
Deephaven table operations. See [03-deephaven-dag.md](03-deephaven-dag.md).

## 2. The state machine scenarios (from TODO)

All handled by `fix42cache.state_machine` and observable in the dashboard:

1. **New order ack / reject** — `D` → `8(150=0)` or `8(150=8)`.
2. **Amend ack / reject** — `G` → `8(150=E)`/`8(150=5)` or `9(434=2)`.
3. **Cancel ack / reject** — `F` → `8(150=6)`/`8(150=4)` or `9(434=1)`.
4. **Execution reports** — new (`150=0`), partial fill (`150=1`), full fill (`150=2`),
   **amend fill** (trade correct, `20=2` + `19=ExecRefID`), **cancel fill** (trade bust,
   `20=1` + `19=ExecRefID`).
5. **Cancel reject** — `9` reverts the pending transition (per-request snapshot).
6. **Don't-know trade** — `Q` marks the referenced execution disputed; no economic change.

Full transition rules: [01-fix42-messages-and-state-machine.md](01-fix42-messages-and-state-machine.md).

## 3. Answers to the TODO analysis questions

1. **Which Deephaven table types?** Blink tables for Kafka ingestion and publisher
   outputs (bounded memory); append-only tables for audit/history panels; a
   `last_by` aggregation over a blink stream for the latest-state cache (blink
   aggregation semantics retain per-key state without retaining rows); partitioned
   tables optionally for per-symbol fan-out. Full analysis:
   [02-deephaven-table-types.md](02-deephaven-table-types.md).
2. **What DAG structure?** Kafka source → parse/state-machine listener node → four
   published streams → derived cache/index/summary/filter nodes → dashboard leaves.
   Full node-by-node spec: [03-deephaven-dag.md](03-deephaven-dag.md).
3. **Which Deephaven features/APIs?** `deephaven.stream.kafka.consumer`,
   `table_publisher`, `table_listener`, blink/append conversions, `last_by`/`agg`,
   execution contexts, `deephaven.ui` (dashboard + linked panels), Application Mode,
   `pydeephaven` (integration tests). Survey: [04-deephaven-features-api.md](04-deephaven-features-api.md).

## 4. Repository layout (this folder)

```
claude-code/
├── docs/                       # analysis & design (this doc set)
├── settings.gradle.kts         # gradle multi-module root (Java 21 toolchain)
├── build.gradle.kts
├── fix-mock-generator/         # Java 21: FIX builder + scenario engine + Kafka producer CLI
│   └── src/{main,test}/java/com/fix42/dashboard/gen/...
├── deephaven-scripts/          # python submodule (gradle-wrapped pytest)
│   ├── pyproject.toml
│   ├── src/fix42cache/         # pure python: tags, parser, model, state machine
│   ├── src/dh_app/             # deephaven server scripts: ingest (kafka|amps), dag, query api, dashboard
│   └── tests/                  # pytest unit tests
├── deephaven-app-java/         # Java 21: the same deephaven app on the java engine API
│   ├── src/main/java/com/fix42/dashboard/fixcache/  # state machine (no deephaven imports)
│   ├── src/main/java/com/fix42/dashboard/dh/        # publishers, DAG, query api, app entry
│   └── parity/                 # regenerates the python-vs-java golden asserted by the tests
├── deephaven-remote-uri/       # python: N leaf servers (AMPS tapes) + a remote-URI collector (doc 10)
│   ├── src/remote_uri/         #   config, ingest, leaf, remote, collector, exposure, dashboard
│   ├── amps/amps-config.xml    #   the demo AMPS broker config
│   └── e2e/                    #   run_e2e.sh + pydeephaven assertions against leaves + collector
├── amps-connectors/            # Spring Boot: AMPS topics -> Deephaven tables (doc 07)
│   └── src/main/{java,resources}/  # connectors + application.yml
├── docker/                     # podman-compose stack: kafka (KRaft) + deephaven (+ui)
│   ├── apps/<name>/            # one folder per deephaven app; pick with DH_APP
│   ├── deephaven-amps.Dockerfile   # server image + amps-python-client (remote-uri stack)
│   └── docker-compose.remote-uri.yml   # amps + dh1 + dh2 + collector (doc 10)
├── integration-test/           # e2e: generator → kafka → deephaven, asserted via pydeephaven
└── README.md                   # build + demo runbook
```

## 5. Conventions

- Kafka topic **`fix42.messages`**; value = raw FIX 4.2 string (SOH `\x01` delimited);
  key = the order chain key (generator uses the venue OrderID it assigns per chain) so
  all messages of one order land in one partition, preserving per-order ordering.
- Order chain key (`OrderKey`) inside the cache: first identifier that creates the
  chain — OrderID if present, else ClOrdID of the `D`; all later identifiers alias to it.
- Enum-ish columns (`OrdStatus`, `ExecType`, `Side`, …) are stored as readable strings
  (`PARTIALLY_FILLED`, not `1`) for direct display in the dashboard.
- Timestamps: Kafka ingest time (`KafkaTimestamp`, Instant) + FIX `TransactTime`/
  `SendingTime` parsed to Instant when present.
- Java: 21 (Gradle toolchain, foojay auto-provisioning), JUnit 5. Python: 3.10+
  (Deephaven server images bundle 3.10/3.12; core package is version-agnostic), pytest.

## 6. Document index

- [01 — FIX 4.2 messages, linking & state machine](01-fix42-messages-and-state-machine.md) *(contract for all implementations)*
- [02 — Deephaven table-type analysis](02-deephaven-table-types.md)
- [03 — Deephaven DAG design](03-deephaven-dag.md)
- [04 — Deephaven features & API survey](04-deephaven-features-api.md)
- [05 — Implementation plan, build & testing](05-implementation-and-testing.md) *(module APIs, scenario catalog, demo runbook)*
- [06 — State machine language choice: Python vs Java](06-state-machine-language-analysis.md) *(trade-offs + measured in-container throughput ceiling)*
- [07 — AMPS connectors](07-amps-connectors.md) *(AMPS → Deephaven bridge: config model, SOW vs journal, table types, delta handling, lifecycle)*
- [08 — On-demand executions from AMPS](08-on-demand-executions-idea.md) *(**tabled idea, not a contract** — explored and set aside: two sources of truth, partial memory win)*
- [09 — Multi-OMS drop-copy blotter](09-multi-oms-blotter.md) *(contract for `deephaven-app-multi-oms-blotter`: cross-hub linking via configurable tags, per-edge CumQty/LeavesQty/notional recon, break taxonomy, paged blotter UI)*
- [10 — Multi-server Deephaven: remote-URI leaves and collector](10-deephaven-remote-uri.md) *(contract for `deephaven-remote-uri`: sharding by hub/chain key, the 400M-message sizing analysis, remote subscription/snapshot/query mechanisms, leaf exports, collector DAG, exposure semantics)*
