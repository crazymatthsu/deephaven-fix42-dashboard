# Deephaven FIX 4.2 Trading Dashboard

A real-time order-state dashboard built on [Deephaven](https://deephaven.io): FIX 4.2
order-flow messages (`35=D, G, F, 8, 9, Q`) stream from Kafka into a streaming DAG whose
single stateful node folds them through a FIX 4.2 state machine into a latest-order-state
cache. The result is a three-panel live dashboard — a master orders grid where clicking an
order reveals its executions and its new/amend/cancel history — plus a point-lookup query
API by Account, Symbol, ClOrdID, OrderID and ExecID. A seeded Java mock generator scripts
every interesting lifecycle (fills, amends, cancels, rejects, trade busts, corrections and
DK trades) so the whole thing is reproducible from a cold start in about two minutes.

---

## Architecture

```
  ┌───────────────────────────┐                    ┌───────────────────────────────────────────────┐
  │  fix-mock-generator       │  raw FIX 4.2       │  Deephaven server (python, app mode)          │
  │  Java 21 · Gradle         │  SOH tag=value     │                                               │
  │                           │                    │   kc.consume / AMPS ──►  fix_raw  (blink)     │
  │  scenario engine          ├──► Kafka topic ───►│        │                                      │
  │  KafkaProducer CLI        │   fix42.messages   │        ▼  table listener (THE stateful node)  │
  │                           │   key = ChainKey   │   FixStateMachine  (fix42cache, pure python)  │
  └───────────────────────────┘                    │        │                                      │
         host ──► localhost:19092                  │        ├─► fix_messages_blink   (audit)       │
                                                   │        ├─► order_state_blink    (snapshots)   │
  ┌───────────────────────────┐                    │        ├─► executions_blink     (35=8 / Q)    │
  │  podman compose           │                    │        └─► order_events_blink   (lifecycle)   │
  │                           │                    │                    │                          │
  │  kafka   (KRaft, 1 node)  │◄── kafka:9092 ─────┤   declarative DAG  ▼                          │
  │  deephaven + deephaven.ui │                    │     order_state_latest = last_by(OrderKey)    │
  │  kafka-ui  :8080          │                    │                                               │
  └───────────────────────────┘                    │     executions_latest  = last_by(ExecID)      │
                                                   │     clordid_index · execid_index              │
                                                   │     status_summary · symbol_summary           │
                                                   │                    │                          │
                                                   │     query API  ·  fix42_dashboard (3 panels)  │
                                                   └───────────────────────────────────────────────┘
                                                              http://localhost:10000/ide
```

`fix_raw` is fed by either Kafka or an AMPS transaction log, selected with
`FIX42_SOURCE` — see [AMPS transaction log as the source](#amps-transaction-log-as-the-source-optional).
Both replay their journal from the beginning on every boot, which is what makes a
Deephaven restart rebuild the identical cache; nothing downstream can tell the
difference, because the state-machine listener reads exactly one column, `RawFix`.

Exactly one node in the graph is stateful. FIX chain resolution — amend chains
(`ClOrdID → OrigClOrdID`), late-arriving `OrderID`, per-request reject reverts, `ExecID`
dedupe — is a stateful fold that `where`/`last_by`/joins cannot express, so it lives in a
single table listener that republishes normalized rows through `TablePublisher`s.
Everything up- and downstream is declarative, incrementally-computed table operations.
Rationale in [docs/00-overview.md](docs/00-overview.md#2-the-state-machine-scenarios-from-todo).

---

## Prerequisites

| Tool | Notes |
|---|---|
| **podman** (or Docker) | podman 5.x with `podman-compose`, or `docker` with `compose`. On macOS the podman VM must be running: `podman machine start`. |
| **JDK** | Not required up front — the Gradle toolchain auto-provisions **Java 21** via foojay. |
| **python3** | 3.10+ on the host, for the `deephaven-scripts` unit tests and the integration-test client venv. |
| **RAM** | The Deephaven container is configured with `-Xmx4g`; give the podman machine ≥6 GB. |

Nothing else is installed globally: the Gradle wrapper is committed, and the integration
test builds its own throwaway virtualenv.

---

## Quickstart

### 1. Build and run every unit test

```bash
./gradlew build
```

Compiles the Java generator, runs its JUnit 5 suite, and runs the `fix42cache` pytest
suite through the Gradle-wrapped python module.

### 2. Bring up the stack

```bash
podman compose -f docker/docker-compose.yml up -d
```

Starts Kafka (KRaft, single node), Deephaven, and **kafka-ui** on
<http://localhost:8080>. Deephaven waits for Kafka's healthcheck
to pass before starting, so no ordering flags are needed. The healthcheck also pre-creates
the `fix42.messages` topic — required, because Deephaven's Kafka consumer resolves the
topic's partitions once at startup and would sit idle forever if the topic only appeared
on the generator's first produce. First run pulls ~2 GB of images.

To skip the UI, name the services you want: `podman compose -f docker/docker-compose.yml up -d kafka deephaven`.

Watch the Deephaven server come up and load the application:

```bash
podman logs -f fix42-deephaven
```

You are looking for the banner the app prints when the DAG is wired:

```
FIX 4.2 Order State Dashboard -- ready
  kafka bootstrap : kafka:9092
  topic           : fix42.messages (seek to beginning)
  tables          : order_state_latest, executions, ...
  dashboard       : fix42_dashboard
```

### 3. Publish an order flow

```bash
./gradlew :fix-mock-generator:run --args="--orders 25 --rate 50 --seed 42"
```

The generator connects to the **host-side** listener `localhost:19092` by default and
publishes to `fix42.messages`, keyed by chain so per-order ordering is preserved. For a
continuous demo that keeps the dashboard moving, add `--loop`:

```bash
./gradlew :fix-mock-generator:run --args="--orders 40 --rate 20 --loop"
```

### 4. Open the dashboard

Go to **<http://localhost:10000/ide>** (anonymous auth — no login).

In the **Panels** menu, open **`fix42_dashboard`**. You get three linked panels:

- **Orders** (top) — the cache, one row per order chain: `OrderKey`, `Symbol`, `Side`,
  `OrdStatus`, `CumQty`/`LeavesQty`, `AvgPx`, `PendingAction`. It updates live as the
  generator runs.
- **Executions** (bottom left) — every `35=8` for the selected order, with `FillStatus`
  showing the current disposition of each `ExecID` (`NORMAL`, `BUSTED`, `CORRECTED`, `DK`).
- **Order History** (bottom right) — the lifecycle trail: `NEW_REQUEST`, `NEW_ACK`,
  `PARTIAL_FILL`, `AMEND_REQUEST`, `AMEND_ACK`, `CANCEL_REJECT`, `FILL_BUST`, …

**Click any row in the Orders grid** and both lower panels re-filter to that order. That
click-through is pure UI state driving `where` filters on the two append-only history
tables — the DAG never changes shape at runtime.

Individual tables also appear in the Panels menu (`order_state_latest`, `executions`,
`status_summary`, …), which is the fallback UX if the `deephaven.ui` plugin is unavailable.

**Dashboard-only view (no IDE chrome)** — Deephaven's iframe-embed endpoint serves any
global by name, so the dashboard can be opened standalone (for a wall monitor, or to
embed in another page):

- <http://localhost:10000/iframe/widget/?name=fix42_dashboard> — the full 3-panel dashboard
- `http://localhost:10000/iframe/table/?name=order_state_latest` — a single live table

With anonymous auth no extra parameter is needed; under PSK auth append `&psk=<key>`.

### 4b. Inspect the broker (kafka-ui)

Go to **<http://localhost:8080>** — no login. Cluster **`fix42`** → **Topics** →
**`fix42.messages`** → the **Messages** tab shows every record: key (the chain key, e.g.
`ORD-0003`), partition, offset, timestamp, and the raw FIX payload.

Both key and value are read with the `String` serde, configured in the compose file, because
the payloads are SOH-delimited FIX text rather than Avro or Protobuf. Without that they would
render as base64. SOH shows as an escape (`\u0001`) between tags.

This is a read-only inspection tool, deliberately outside the pipeline — nothing depends on it.
Its value is splitting one ambiguous symptom into two answerable questions. An empty dashboard
means either the generator never published or Deephaven never consumed, and those have
completely different fixes:

| kafka-ui shows | Diagnosis |
|---|---|
| no messages | the generator never reached the broker — check it used `localhost:19092`, not `kafka:9092` |
| messages present, dashboard empty | ingestion is the problem — check `podman logs fix42-deephaven` |

The **Consumers** tab lists Deephaven's group, `dh-fix42-dashboard`, with its committed offsets
and per-partition lag — the direct read on whether ingestion is keeping up.

Expect it to report **no active members** even while ingestion is healthy. Deephaven assigns
partitions explicitly (`ALL_PARTITIONS_SEEK_TO_BEGINNING`, doc 03 §2.1) rather than joining the
group for a rebalance, so it commits offsets without ever becoming a group member. Read the lag
column, not the member list:

```
GROUP              TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
dh-fix42-dashboard fix42.messages  0          37              37              0
```

### 5. Tear down

```bash
podman compose -f docker/docker-compose.yml down -v
```

---

## Query API (Deephaven IDE console)

Every function is a global in the server's python session and returns a **live** table —
still a DAG node, so it keeps ticking and you can open it as a panel.

```python
# Point lookups. Aliases resolve through the index tables first, so any ClOrdID in an
# amend chain (root or current) finds the order, and ExecIDs resolve to their chain.
get_by_order_id("ORD-0007")          # tag 37 — the venue id
get_by_clordid("C-3-1")              # tag 11 — any id in the chain, including the root
get_by_execid("EXEC-0042")           # tag 17 — resolves the exec to its order

# Set lookups.
find_by_account("ACC-2")
find_by_symbol("AAPL")

# The three-way drill-down behind the dashboard panels.
detail = order_detail("ORD-0007")
detail["state"]         # one row: the current cached state
detail["executions"]    # every execution report, newest first
detail["events"]        # the lifecycle trail, newest first

# The DAG nodes are globals too — filter them directly.
open_orders.where("Symbol = `MSFT`")
status_summary
executions_latest.where("FillStatus != `NORMAL`")   # busted / corrected / DK'd fills
```

Full node-by-node spec: [docs/03-deephaven-dag.md](docs/03-deephaven-dag.md#25-query-api-functions-over-the-dag).

---

## Scenario catalog

The generator scripts one message sequence per scenario. Pick one with `--scenario <name>`,
or use `all` (the default) for a weighted mix. `--list-scenarios` prints them at runtime.

| Name | Sequence | Demonstrates |
|---|---|---|
| `new_ack_fill_full` | `D → 8(A) → 8(0) → 8(1)×k → 8(2)` | the happy path to `FILLED` |
| `new_reject` | `D → 8(150=8, 103)` | terminal `REJECTED`, never `NEW` |
| `amend_ack` | `D → 8(0) → [8(1)] → G → 8(E) → 8(5) → fills` | staged terms applied on confirm; `ClOrdID` rotation |
| `amend_reject` | `D → 8(0) → G → 9(434=2, 102, 58)` | pending replace reverts to prior status |
| `cancel_ack` | `D → 8(0) → [8(1)] → F → 8(6) → 8(4)` | `PENDING_CANCEL` → `CANCELED` |
| `cancel_reject` | `D → 8(0) → 8(1) → F → 9(434=1, 39=1)` | venue's tag 39 wins over the snapshot revert |
| `fill_bust` | `D → 8(0) → 8(1) → 8(20=1, 19=prior ExecID)` | trade bust; restated absolute `14/151/6/39` |
| `fill_correct` | `D → 8(0) → 8(1) → 8(20=2, 19=prior ExecID, new 31/32)` | trade correction adopting restated snapshots |
| `dk_trade` | `D → 8(0) → 8(1) → Q(37, 17, 127)` | disputed execution, **no** economic change |
| `partial_then_cancel` | `D → 8(0) → 8(1) → F → 8(6) → 8(4)` | partial fill then cancel |

Given a `--seed`, the sequence, ids and quantities are fully deterministic — which is what
the integration test asserts against.

---

## Integration test

End-to-end: stack up → generator publishes → assertions over the live cache through
`pydeephaven` → teardown.

```bash
cd integration-test
./run_integration.sh
```

It detects the compose implementation, starts the podman machine if needed, waits for
Deephaven to serve, builds a client venv (`.venv-it`), runs the generator with
`--seed 42 --orders 12 --emit-expected`, then runs `pytest`.

```bash
KEEP_STACK=1 ./run_integration.sh    # leave the stack up to poke at afterwards
SEED=7 ORDERS=30 ./run_integration.sh
PYTEST_ARGS="-k restart" ./run_integration.sh
```

What it asserts ([docs/05](docs/05-implementation-and-testing.md#6-integration-test-integration-test)):

1. every DAG global from doc 03 exists on the server;
2. per-chain final `OrdStatus` / `CumQty` / `LeavesQty` / `ClOrdID` match the generator's
   own `--emit-expected` export;
3. the `fill_bust` chain has a `BUSTED` execution and the `dk_trade` chain a `DK` one in
   `executions_latest` — located by the `Scenario` tag in the expected file, so the
   assertion is deterministic rather than dependent on what the weighted mix drew;
4. `clordid_index` resolves an amended chain's **root** and **final** `ClOrdID` to the same
   `OrderKey`;
5. `status_summary` counts partition the cache exactly;
6. **restart resilience** — the test restarts the Deephaven container and asserts the cache
   rebuilds identically. This is the payoff of consuming with
   `ALL_PARTITIONS_SEEK_TO_BEGINNING`: the topic is the journal, and idempotent id binding
   plus `ExecID` dedupe make the cache a pure function of it.

The suite **skips** (never fails) when the stack is unreachable or the expected file is
missing, so it is safe to run in CI without containers. Details on the expected-data
contract: [integration-test/golden/README.md](integration-test/golden/README.md).

> **Rerun semantics.** The pipeline is idempotent by design, so rerunning without
> `down -v` replays the same chain keys and converges to the same rows rather than
> double-counting. Assertions therefore match on **expected keys**, never on total row
> counts. Rerunning with a *different* `--seed` leaves the older chains in the topic, so
> the cache legitimately holds more orders than the newest expected file describes.

---

## The Deephaven image

Pinned to **`ghcr.io/deephaven/server:42.4`**. Verified on this machine:

- `:42.4` is the concrete version behind `:latest` (from the image's
  `io.deephaven.server.version` label); both tags pull successfully.
- **`deephaven.ui` ships in the base image** — `deephaven-plugin-ui 0.40.2` and
  `deephaven-plugin-plotly-express 0.20.0` are already installed
  (`podman run --rm --entrypoint pip ghcr.io/deephaven/server:42.4 show deephaven-plugin-ui`).
  **No derived Dockerfile is needed.** If a future pin drops them, add
  `docker/deephaven/Dockerfile` with
  `RUN pip install --no-cache-dir deephaven-plugin-ui deephaven-plugin-plotly-express`
  and swap `image:` for `build:` in the compose file.
- The client pin must track the server: **`pydeephaven==42.4`** in
  `integration-test/requirements.txt`. Bump both together.

### Application mode wiring

`docker/app.d/dashboard.app` points at `loader.py`, which executes
`/scripts/dh_app/app.py`. Both an absolute `file_0=/scripts/dh_app/app.py` and a path
relative to the application dir were verified to work on 42.4 — the loader is used anyway
because **app-mode scripts run with `__file__` unset** (`__name__` is `"__main__"`). The
loader defines `__file__` before executing the entrypoint, puts `/scripts` on `sys.path`,
and turns a missing entrypoint into one actionable log line instead of an injector stack
trace. `PYTHONPATH=/scripts` in the compose file is the belt-and-braces backup.

---

## Troubleshooting

**`podman machine` is not running** — everything fails with a connection error:
```bash
podman machine start
podman info | head        # should print host details
```

**Port already in use** (`10000`, `19092`, `8080`) — find the squatter, or remap the host side of
the port in `docker/docker-compose.yml` (`"10001:10000"`):
```bash
lsof -nP -iTCP:10000 -sTCP:LISTEN
```
`8080` is the most likely clash, since plenty of things want it. Only kafka-ui uses it, so
remapping it (`"8090:8080"`) or dropping the service affects nothing else.

**Image pull fails / is slow** — pre-pull, then retry compose:
```bash
podman pull apache/kafka:3.9.1
podman pull ghcr.io/deephaven/server:42.4
```

**No tables or dashboard in the IDE** — the app-mode script did not load. The loader logs
its own failures:
```bash
podman logs fix42-deephaven | grep -E '\[fix42-loader\]|\[fix42\]'
```
`entrypoint not found` means the `../deephaven-scripts/src:/scripts` bind mount did not
resolve — run compose with `-f docker/docker-compose.yml` from the repo root so the
relative path is correct. A python traceback means the entrypoint itself raised; the
server stays up, so you can fix the file and re-run it from the IDE console.

**Dashboard panel missing but tables are present** — `deephaven.ui` failed to import. The
startup banner prints `dashboard : unavailable (deephaven.ui missing)`. Every table is
still an independent panel, so the demo degrades gracefully. Check:
```bash
podman exec fix42-deephaven pip show deephaven-plugin-ui
```

**Generator cannot reach Kafka** — it uses the *external* listener. From the host the
bootstrap server is `localhost:19092`, never `kafka:9092` (that name only resolves inside
the compose network). Confirm Kafka is healthy:
```bash
podman ps --filter name=fix42-kafka        # want "(healthy)"
podman exec fix42-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Deephaven starts before Kafka is ready** — it shouldn't: `depends_on` with
`condition: service_healthy` gates it, and podman-compose 1.6.0 honors that. If your
compose implementation ignores conditions, the Kafka consumer simply retries, and
`ALL_PARTITIONS_SEEK_TO_BEGINNING` means nothing is lost once it connects.

**Cache looks stale after a restart** — it rebuilds by replaying the topic from offset 0,
which takes a moment on a large backlog. Watch row counts settle in `order_state_latest`.

**`FIX42_SOURCE='...' is not a known source`** — the app refuses to start rather than
falling back to Kafka. A deployment that meant to read AMPS and silently got Kafka would
look perfectly healthy while rebuilding a cache from the wrong journal.

**`ModuleNotFoundError: No module named 'AMPS'`** — `FIX42_SOURCE=amps` but the AMPS
python client is not in the Deephaven image. Install it (see the section below); note a
`pip install` into a running container does not survive `compose down`.

**AMPS connects but no rows arrive** — the topic name or the filter does not match. The
banner prints exactly what was subscribed, and every connection transition is logged:
```bash
podman logs fix42-deephaven | grep -E 'AMPS (subscribed|connection state)'
```

---

## AMPS transaction log as the source (optional)

The pipeline reads raw FIX from Kafka by default. Set `FIX42_SOURCE=amps` and it reads the
same raw FIX from an AMPS transaction log instead — everything downstream is unchanged,
because the state-machine listener only ever reads the `RawFix` column.

```yaml
# docker/docker-compose.yml, the deephaven service
environment:
  FIX42_SOURCE: "amps"
  FIX42_AMPS_URI: "tcp://amps:9007/amps/fix"   # comma-separated for an HA pair
  FIX42_AMPS_TOPIC: "fix42.messages"           # defaults to FIX42_TOPIC
  FIX42_AMPS_BOOKMARK: "epoch"                 # epoch | now | most_recent | literal
```

The `epoch` bookmark is the AMPS analogue of Kafka's seek-to-beginning: the subscription
replays the whole transaction log and then cuts over to live messages on that same
subscription, so a restart deterministically rebuilds the cache exactly as the Kafka path
does. Ordering comes from the transaction log's own sequence — a single total order per
topic, which does not depend on how the publisher keyed anything. A mid-life disconnect
resumes at the last bookmark instead of replaying, and the duplicates a resume can deliver
are absorbed by the same `ExecID` dedupe and idempotent id binding the Kafka replay
already relies on.

Two prerequisites, neither of which the demo stack can supply:

- **An AMPS server.** AMPS is commercial software with no public image, so `docker-compose.yml`
  has no AMPS service. Point the URI at your own broker.
- **The AMPS python client**, which is not in the Deephaven image. It is a commercial
  binary wheel, published on PyPI for `manylinux` x86_64 and aarch64:
  ```bash
  podman exec fix42-deephaven pip install amps-python-client
  ```
  That does not survive a `compose down`; bake it into a derived image to keep it.

This was verified against a live AMPS 5.3.5.135 broker: an `epoch` replay of a journalled
`fix` topic rebuilt 25 executions and 7 orders, and a second cold start over the same
journal produced a byte-identical cache. Two caveats worth knowing. AMPS `fix` messages
arrive as **bodies only** — no `8=FIX.4.2` header, no `10=` checksum — which the lenient
`fix42cache` parser handles by design. And the filter syntax was not exercised.

Because the client is not bundled, `dh_app.ingest` and `dh_app.amps_ingest` import both
`deephaven` and `AMPS` lazily — a Kafka deployment never touches the AMPS import, and
source selection, configuration and the AMPS→update-graph hand-off stay unit-tested with
neither installed (`deephaven-scripts/tests/test_ingest_source.py`).

Design and contract: [docs/03-deephaven-dag.md §2.1](docs/03-deephaven-dag.md).

> Not to be confused with the next section. **This** replaces where the FIX 4.2 pipeline
> gets its raw messages. **`amps-connectors`** is a separate application that bridges
> *other* AMPS topics into *their own* Deephaven tables, and does not touch this pipeline.

---

## AMPS connectors (optional)

Separate from the FIX 4.2 pipeline above, `amps-connectors` is a Spring Boot application that
subscribes to [60East AMPS](https://www.crankuptheamps.com/) topics and publishes the fields you
map into Deephaven tables in the same server — so they appear in the IDE alongside
`order_state_latest`. One application runs one or more connectors, all configured in
`application.yml`.

- **Formats** — `FIX`, `NVFIX` and `JSON`, each with its own tag → column → type mapping. The
  mapping is an allowlist: an unmapped field is never published.
- **SOW topic → keyed table**, replayed with `sow_and_subscribe`. **Journal topic → append-only
  table**, resubscribed from the `epoch` bookmark so a restart replays everything.
- **Delta** subscriptions and delta publishing, so a partial AMPS update merges over the stored
  row instead of blanking the columns it omits.
- **Restarting Deephaven restarts the connectors.** A poll detects the new server, re-creates the
  tables and replays every subscription from the start, rehydrating the tables.

```bash
# with an AMPS server on localhost:9007
./gradlew :amps-connectors:bootRun

# without one -- the demo profile swaps in an in-process simulator
./gradlew :amps-connectors:bootRun --args="--spring.profiles.active=demo"
```

AMPS is commercial software with no public image, so the compose stack does not include one.
Runbook and configuration reference: [amps-connectors/README.md](amps-connectors/README.md).
Design and contract: [docs/07-amps-connectors.md](docs/07-amps-connectors.md).

---

## Repository layout

```
claude-code/
├── docs/                          # analysis & design — the binding contracts
│   ├── 00-overview.md … 05-implementation-and-testing.md
├── settings.gradle.kts            # gradle multi-module root (Java 21 toolchain)
├── build.gradle.kts
├── fix-mock-generator/            # Java 21: FIX builder + scenario engine + Kafka CLI
│   └── src/{main,test}/java/com/fix42/dashboard/gen/
├── deephaven-scripts/             # python module, gradle-wrapped pytest
│   ├── src/fix42cache/            #   pure python: tags, parser, model, state machine
│   ├── src/dh_app/                #   deephaven server scripts: ingest (kafka|amps), dag, api, dashboard
│   └── tests/                     #   pytest unit suite
├── amps-connectors/               # Spring Boot: AMPS topics -> Deephaven input tables
│   ├── src/main/java/com/fix42/dashboard/amps/
│   └── src/main/resources/application.yml   # the whole configuration surface
├── docker/
│   ├── docker-compose.yml         # kafka (KRaft) + deephaven, pinned images
│   └── app.d/                     # application mode: dashboard.app + loader.py
├── integration-test/
│   ├── run_integration.sh         # up → generate → pytest → down
│   ├── test_e2e.py                # pydeephaven assertions incl. restart idempotence
│   ├── requirements.txt           # pydeephaven pinned to the server version
│   └── golden/README.md           # the --emit-expected data contract
└── README.md
```

---

## Documentation index

| Doc | Contents |
|---|---|
| [00 — Overview](docs/00-overview.md) | architecture, the hybrid-DAG decision, conventions |
| [01 — FIX 4.2 messages & state machine](docs/01-fix42-messages-and-state-machine.md) | **the contract**: wire format, chain keying, transition rules, edge cases |
| [02 — Deephaven table types](docs/02-deephaven-table-types.md) | blink vs append vs ring; why `last_by` over a blink stream is the cache |
| [03 — DAG design](docs/03-deephaven-dag.md) | node-by-node spec, table/global names, query API, consistency notes |
| [04 — Features & API survey](docs/04-deephaven-features-api.md) | Kafka consumer, table publishers, listeners, `deephaven.ui`, app mode, `pydeephaven` |
| [05 — Implementation & testing](docs/05-implementation-and-testing.md) | module APIs, scenario catalog, build layout, demo runbook |
| [06 — State machine language choice](docs/06-state-machine-language-analysis.md) | python vs java for the stateful fold, with a measured throughput ceiling |
| [07 — AMPS connectors](docs/07-amps-connectors.md) | the AMPS → Deephaven bridge: config model, SOW vs journal, delta handling, lifecycle |
| [08 — On-demand executions](docs/08-on-demand-executions-idea.md) | **tabled idea, not a contract** — fetching executions from AMPS per click; why it was set aside, and the cheaper alternatives |
