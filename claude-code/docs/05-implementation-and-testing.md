# Implementation Plan, Build & Testing (contract for implementers)

Binding interface/spec doc. Schemas referenced here are defined in
[01-fix42-messages-and-state-machine.md](01-fix42-messages-and-state-machine.md);
DAG node names in [03-deephaven-dag.md](03-deephaven-dag.md).

## 1. Gradle build (root `claude-code/`)

- Gradle 9.x Kotlin DSL, wrapper committed. `settings.gradle.kts` includes
  `:fix-mock-generator`, `:deephaven-scripts`, `:amps-connectors` and
  `:deephaven-app-java`; plugin `org.gradle.toolchains.foojay-resolver-convention`
  (JDK auto-provisioning).
- **Java 21** via toolchain: `java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }`
  (host has JDK 23; foojay downloads 21).
- `:deephaven-scripts` is a python module wrapped in Gradle: task `pytest` (Exec) that
  creates `.venv` via `python3 -m venv`, `pip install -e .[test]`, runs `pytest`; task
  `check` depends on it so `./gradlew build` runs **all** java + python unit tests.
  Must degrade gracefully (clear error) if `python3` missing.

## 2. Java module `:fix-mock-generator`

Package `com.fix42.dashboard.gen`. Deps: `org.apache.kafka:kafka-clients:3.9.x`,
`org.slf4j:slf4j-simple`, JUnit 5 + kafka-clients `MockProducer` for tests.
No framework, plain `main()`.

### 2.1 Classes

| Class | Responsibility |
|---|---|
| `FixMessage` | ordered tag→value builder (LinkedHashMap semantics; header first) |
| `FixSerializer` | render with SOH, compute/insert `9 BodyLength` + `10 CheckSum` per doc 01 §1; static `serialize(FixMessage)` |
| `OrderScenario` | one order chain's scripted message sequence (list of `FixMessage` + think-time gaps); knows its ChainKey (venue OrderID) |
| `ScenarioEngine` | seeded `Random`; builds N chains across the scenario catalog (§2.2 weights or explicit `--scenario`); maintains venue-side counters (OrderID `ORD-xxxx`, ExecID `EXEC-xxxx`, ClOrdID `C-<chain>-<n>`), correct absolute CumQty/LeavesQty/AvgPx math, monotone TransactTime |
| `KafkaFixPublisher` | wraps `Producer<String,String>`; key = ChainKey, value = serialized FIX; flush/close |
| `GeneratorMain` | CLI: `--bootstrap-servers` (default `localhost:19092`), `--topic` (`fix42.messages`), `--orders N` (default 20), `--seed` (default random), `--rate msgs/sec` (default 50), `--scenario all|<name>`, `--loop` (regenerate forever), `--list-scenarios`, `--dry-run` (print to stdout, no Kafka) |

Interleaving: engine round-robins messages across concurrently "live" chains so the
dashboard shows many orders progressing at once; per-chain order strictly preserved.

### 2.2 Scenario catalog (names are CLI values; each maps to TODO scenarios)

| Name | Sequence |
|---|---|
| `new_ack_fill_full` | D → 8(A) → 8(0) → 8(1)×k → 8(2) |
| `new_reject` | D → 8(150=8, 103 set) |
| `amend_ack` | D → 8(0) → [8(1)] → G(qty/price) → 8(E) → 8(5, new terms) → fills to filled |
| `amend_reject` | D → 8(0) → G → 9(434=2, 102, 58) |
| `cancel_ack` | D → 8(0) → [8(1)] → F → 8(6) → 8(4) |
| `cancel_reject` | D → 8(0) → 8(1) → F → 9(434=1, 39=1) |
| `fill_bust` | D → 8(0) → 8(1) → 8(20=1, 19=prior ExecID, restated 14/151/6/39) |
| `fill_correct` | D → 8(0) → 8(1) → 8(20=2, 19=prior ExecID, new 31/32, restated snapshots) |
| `dk_trade` | D → 8(0) → 8(1) → Q(37, 17=that exec, 127) |
| `partial_then_cancel` | D → 8(0) → 8(1) → F → 8(6) → 8(4) (partial then canceled) |

`all` = weighted mix. Deterministic given `--seed`: same message sequence & keys —
integration test relies on `--seed 42`.

### 2.3 Unit tests (JUnit 5)

Serializer: checksum/bodylength on doc 01's example vector; SOH placement; zero-pad.
Engine (seed-fixed): every scenario emits the exact 35=/150=/39=/434=/20= skeleton
above; CumQty monotone & LeavesQty = OrderQty−CumQty (post-amend math too); AvgPx
consistency; ExecID/ClOrdID uniqueness; bust restates lower CumQty; chain ids link
(41 = prior 11). Publisher: `MockProducer` receives key=ChainKey per message.

## 3. Python package `fix42cache` (pure — **no deephaven imports**)

Location `deephaven-scripts/src/fix42cache/`. Python ≥3.10, stdlib only.

```python
# fixtags.py  — Tag int constants; enums as str-valued Enums:
OrdStatus, ExecType, ExecTransType, Side, OrdType, TimeInForce, CxlRejResponseTo
# name mapping per doc 01 §2 (from_fix(code) -> enum, .name used in rows)

# parser.py
def parse_fix(raw: str) -> dict[int, str]      # SOH or '|', first-'=' split, skips empties
def render_pipe(raw: str) -> str               # SOH -> '|' for display
def checksum_ok(raw: str) -> bool | None       # None if no tag 10
def parse_transact_time(v: str) -> datetime | None   # UTC

# model.py
@dataclass OrderState: ...                     # exactly doc 01 §4 (python-typed)
    def to_row(self) -> dict[str, object]      # column-name -> value (enum -> .name)
@dataclass ExecutionRow / OrderEventRow / MessageRow: ...   # doc 01 §6, same pattern

# state_machine.py
@dataclass Result:
    state: OrderState                # post-message snapshot
    executions: list[ExecutionRow]   # 0..n (bust/correct/DK re-emit referenced exec)
    events: list[OrderEventRow]      # 0..n
    message: MessageRow
    error: str | None                # set (others None/empty) on unparseable/unhandleable

class OrderStateMachine:
    def __init__(self, now_fn: Callable[[], datetime] = utcnow): ...
    def process(self, raw: str) -> Result          # parse + dispatch, never raises
    def process_fields(self, f: dict[int, str]) -> Result
    # lookups mirroring cache semantics (used by tests; DH tables serve prod queries):
    def get_by_order_id/get_by_clordid/get_by_execid(...) -> OrderState | None
    def find_by_account/find_by_symbol(...) -> list[OrderState]
    def order_count(self) -> int
```

Behavior: exactly doc 01 §3/§5/§6/§7. `Result.state` is a **copy** (immutable snapshot
semantics). Unknown 35= → `error` set, no state change.

### 3.1 pytest suite (`deephaven-scripts/tests/`)

`test_parser.py` (delimiters, checksum vector, transact time), `test_state_machine_*.py`
— one test per doc 01 §5 rule and §7 edge case (12 edge cases enumerated), plus the
full worked lifecycle from the reference (D→A→0→partial→G→E→5→fill ⇒ FILLED,
AvgPx 185.522, chain C1,C2), and lookup tests. Target ≥40 tests.

## 4. Deephaven scripts `dh_app` (imports deephaven; runs only in server)

Location `deephaven-scripts/src/dh_app/`. Config via env:

| Variable | Default | Applies to |
|---|---|---|
| `FIX42_SOURCE` | `kafka` | which source feeds `fix_raw`: `kafka` or `amps`. Anything else is a startup error, not a silent fallback |
| `FIX42_KAFKA_BOOTSTRAP` | `kafka:9092` | kafka |
| `FIX42_TOPIC` | `fix42.messages` | kafka; also the AMPS topic when `FIX42_AMPS_TOPIC` is unset |
| `FIX42_AMPS_URI` | `tcp://amps:9007/amps/fix` | amps; comma/space separated for an HA pair |
| `FIX42_AMPS_TOPIC` | `FIX42_TOPIC` | amps |
| `FIX42_AMPS_BOOKMARK` | `epoch` | amps; `epoch` / `now` / `most_recent`, or a literal bookmark |
| `FIX42_AMPS_FILTER` | *(none)* | amps; server-side content filter |
| `FIX42_AMPS_CLIENT_NAME` | `dh-fix42-dashboard` | amps; the analogue of the Kafka group id |
| `FIX42_AMPS_MAX_PENDING` | `250000` | amps; buffer bound between update graph cycles |

```python
# schemas.py     — dict[str, DType] for the four publishers + errors (doc 01 §4/§6; single source of truth)
# ingest.py      — fix_source() picks the source; build_fix_raw() -> blink table (doc 03 §2.1)
# amps_ingest.py — AMPS bookmark_subscribe from EPOCH -> the same blink table (doc 03 §2.1)
# pipeline.py    — class Pipeline: wires publishers + listener (doc 03 §2.2/2.3);
#                  start(fix_raw) registers listener; holds all references;
#                  batches rows per cycle; errors -> ingest_errors publisher
# dag.py         — build_derived(...) -> dict of all doc 03 §2.4 tables
# query_api.py   — get_by_order_id/get_by_clordid/get_by_execid/find_by_account/
#                  find_by_symbol/order_detail — resolve via index tables then filter
#                  order_state_latest (return live tables)
# dashboard.py   — build_dashboard(tables) -> ui.dashboard per doc 03 §2.6
#                  (3 linked panels + summary; defensive on_row_press)
# app.py         — entrypoint: sys.path bootstrap, ingest → pipeline → dag →
#                  globals()[name] = table for every node + `fix42_dashboard`
```

`dh_app` contains **no business logic** — it adapts `fix42cache` rows to publisher
batches. All business logic stays in the pure package where it is unit-tested.

`:deephaven-app-java` re-implements this whole section (and §3) against the Deephaven **Java**
engine API, reading the same environment variables and exporting the same globals, so the two apps
are interchangeable behind `DH_APP`. The same split holds there:
`com.fix42.dashboard.fixcache` is the pure state machine and `com.fix42.dashboard.dh` the adapter.
Its `ParityAgainstPythonTest` asserts the two implementations agree column-for-column on every
emitted row. See [`deephaven-app-java/README.md`](../deephaven-app-java/README.md).
(`dh_app` correctness is covered by the integration test; no pytest for it — with one
exception: `ingest.py` and `amps_ingest.py` import `deephaven` and `AMPS` lazily, so
source selection, AMPS config and the AMPS→update-graph hand-off *are* unit-tested in
`tests/test_ingest_source.py`. `run_integration.sh` cannot cover them — it brings up
`docker-compose.yml`, which has no AMPS service.)

## 5. Demo stack (`docker/`, podman)

- `docker-compose.yml` (works with `podman compose`):
  - `kafka`: `apache/kafka:3.9.x` KRaft single node. Listeners: `PLAINTEXT` internal
    `kafka:9092`, `EXTERNAL` advertised `localhost:19092` (host generator), controller.
    Healthcheck: `kafka-topics.sh --create --if-not-exists --topic fix42.messages
    --partitions 3` then `--describe` — the healthcheck **must pre-create the
    topic** (deephaven's `kc.consume` resolves partitions once at startup; a
    topic that appears only on first produce leaves the consumer with zero
    partitions and it shuts down). Auto-create topics on (belt and braces).
  - `deephaven`: `ghcr.io/deephaven/server:<pinned — verify pullable>`; ports
    `${DH_PORT:-10000}:10000`; volumes: `../deephaven-scripts/src:/scripts:ro,z`,
    `./apps/_lib:/dh-app-lib:ro,z`, `./apps/${DH_APP:-fix42-dashboard}:/app.d:ro,z`;
    `START_OPTS` = app-mode dir + anonymous auth + `-Xmx4g` (doc 04 §7);
    depends_on kafka healthy. If `deephaven.ui` missing from base image, add
    `Dockerfile` (`pip install deephaven-plugin-ui deephaven-plugin-plotly-express`)
    and build in compose.
- **One folder per app under `docker/apps/`**, selected with `DH_APP`; only that folder
  is mounted at `/app.d`. `apps/<name>/<name>.app` per doc 04 §7 with `file_0=main.py`;
  `main.py` calls `load()` from the shared `apps/_lib/loader.py` (mounted at
  `/dh-app-lib`) to execute its entrypoint under `/scripts`. `DH_PORT` and
  `DH_CONTAINER` let a second app run beside the first — `docker/apps/README.md`.
- SELinux-friendly `:z` volume flags are harmless on macOS podman.

## 6. Integration test (`integration-test/`)

`run_integration.sh` + `test_e2e.py` (pytest, `pydeephaven`, `kafka-python` optional):

1. `podman compose up -d --wait` (or docker compose; auto-detect).
2. `./gradlew :fix-mock-generator:run --args="--seed 42 --orders 12 --rate 200"`.
3. Poll `order_state_latest` via pydeephaven until row count stable ≥ expected chains.
4. Assert (seed-42 expectations exported by the generator as
   `--dry-run --emit-expected expected.json` or a checked-in golden file):
   per-order final `OrdStatus`, `CumQty`, `LeavesQty`; a busted exec has
   `FillStatus=BUSTED` in `executions_latest`; `clordid_index` resolves an amended
   chain's first ClOrdID; query API globals exist.
5. Restart deephaven container → poll again → identical cache (replay idempotence).
6. `podman compose down -v` (flag to keep up: `KEEP_STACK=1`).

Marked/skipped cleanly when podman or the stack is unavailable (CI-friendly).

## 7. README runbook (claude-code/README.md)

Prereqs (JDK via gradle, python3, podman desktop) → `./gradlew build` (all unit tests)
→ `podman compose -f docker/docker-compose.yml up -d` → run generator (gradle run,
`--loop` for continuous demo) → open `http://localhost:10000/ide` → open
`fix42_dashboard` panel; click an order → executions + history panels react; query API
examples in console; run integration test; troubleshooting (ports, podman machine,
image pulls, app-mode logs `podman logs deephaven`).

## 8. Division of labor (implementation agents)

| Agent | Owns (exclusively) | Contract |
|---|---|---|
| A | root gradle files, `gradle/wrapper`, `fix-mock-generator/` | §1, §2 |
| B | `deephaven-scripts/` scaffolding (pyproject, build.gradle.kts, pytest cfg), `src/fix42cache/`, `tests/` | §1 (python task), §3 |
| C | `deephaven-scripts/src/dh_app/` only | §4 (+docs 03/04) |
| D | `docker/`, `integration-test/`, `README.md` | §5, §6, §7 |

No agent edits another's files; shared truth = docs. Naming/schemas are frozen by docs
01/03; deviations require updating docs in the same change.
