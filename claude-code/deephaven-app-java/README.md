# `:deephaven-app-java` — the FIX 4.2 dashboard app, in Java

This module is the Java rewrite of the `fix42-dashboard` app that
[`deephaven-scripts/`](../deephaven-scripts) implements in Python. Same contracts, same table
names, same columns, same values — built against the Deephaven **Java engine API** and running
inside the same `ghcr.io/deephaven/server:42.4` container.

`docs/06-state-machine-language-analysis.md` §3 names this as the project's escape hatch for when
the Python fold's measured ~23–24k msg/s ceiling stops being enough. This module is that hatch,
taken end to end rather than only for the fold.

```bash
./gradlew :deephaven-app-java:assemble                                    # build + stage the jar
DH_APP=fix42-dashboard-java podman compose -f docker/docker-compose.yml up -d
open http://localhost:10000/ide                                           # same panels as the python app
```

## What it is

Two packages, mirroring the Python split exactly:

| Java | Python | Contains |
|---|---|---|
| `com.fix42.dashboard.fixcache` | `fix42cache` | The FIX 4.2 parser and order state machine. **No Deephaven imports** — plain JDK, unit-tested on its own. |
| `com.fix42.dashboard.dh` | `dh_app` | The Deephaven wiring: schemas, ingest, the stateful listener node, the derived DAG, the query API. **No FIX business logic.** |

The seam between them is the frozen row schemas of `docs/01-fix42-messages-and-state-machine.md`
§4/§6, exactly as in Python — which is what made the port a rewrite of two independent halves
rather than one tangled one.

| File | Purpose |
|---|---|
| `fixcache/FixTags`, `FixEnums` | Tag numbers; the seven `str`-valued enums with lenient `fromFix`. |
| `fixcache/FixParser` | SOH/pipe parsing, `renderPipe`, tri-state `checksumOk`, `parseTransactTime`. |
| `fixcache/OrderState`, `ExecutionRow`, `OrderEventRow`, `MessageRow`, `Columns` | The published rows and their frozen column order. |
| `fixcache/OrderStateMachine`, `Result` | The stateful fold: identity resolution, the six handlers, the replay and stale guards. |
| `fixcache/PyNum`, `PyFloat`, `PyInt`, `PyStrptime`, `PyRepr`, `PyException` | CPython-semantics shims — see [Faithfulness](#faithfulness-what-had-to-be-reimplemented). |
| `dh/Schemas`, `Names` | Column-to-dtype definitions built *from* `Columns`; the frozen global names. |
| `dh/BlinkStream` | One `TablePublisher` plus its batch builder and null coercions. |
| `dh/Fix42Pipeline` | The one stateful DAG node: the `fix_raw` listener and the five publishers. |
| `dh/Fix42Dag` | The eleven derived tables. |
| `dh/Fix42QueryApi` | The six lookup functions, with identifier sanitization. |
| `dh/Ingest`, `KafkaIngest`, `AmpsConfig`, `RawBuffer`, `AmpsRawSource` | Source selection and the two sources. |
| `dh/Fix42JavaApp` | The entry point, and the `ApplicationState.Factory` for the pure-Java route. |

## How it runs, and why python is still in the loop

`docker/apps/fix42-dashboard-java/` is an ordinary app folder, selected with `DH_APP` like any
other (see [`docker/apps/README.md`](../docker/apps/README.md)). Its descriptor is
`type=script`, and `main.py` is a ~30-line shim that calls one static Java method and binds the
returned tables into the script session's globals.

That shim is not laziness — it buys two things Java alone cannot have on Deephaven 42.4:

1. **`pydeephaven` can see the tables.** `Session.open_table()` builds a *scope* ticket
   unconditionally. A `type=dynamic` Java application exports its tables as `ApplicationState`
   fields, whose ticket is `a/<appId>/f/<name>` — visible in the web IDE, invisible to
   `open_table`. Binding into python globals makes them scope variables, which is why
   `integration-test/test_e2e.py` asserts this app **unchanged**.
2. **The dashboard exists at all.** `deephaven.ui` ships in the image only as python
   (`deephaven_plugin_ui-0.40.2`); there are no `deephaven-plugin-ui` jars. See
   [What is not ported](#what-is-not-ported).

The pure-Java route is implemented and available — `Fix42JavaApp.create(ApplicationState.Listener)`
with a `type=dynamic` descriptor — for a deployment that wants no python at all and accepts losing
the dashboard and `open_table`.

## What is not ported

**The `deephaven.ui` three-panel dashboard.** It has no Java API in 42.4, so it cannot be written
in Java, and this module does not pretend otherwise. Because the shim already puts the Java tables
in python's hands, `dh_app/dashboard.py` is reused **verbatim, unmodified** against them: the FIX
engine is Java, the presentation layer stays python, and `fix42_dashboard` still appears in the
Panels menu with the same row-click linkage.

If you take the pure-Java route instead, the interactive click-through is simply gone. The tables
are all still there to open side by side; say that rather than implying parity.

## Faithfulness: what had to be reimplemented

A "same behaviour" claim is only worth what verifies it, so the port reproduces CPython's
semantics wherever the JDK's differ in a way that would change a published column:

| Shim | Why the JDK equivalent is wrong |
|---|---|
| `PyNum` | `String.format("%.6g", 185.5)` is `"185.500"`; python is `"185.5"`. And `str(int(v))` is exact and unbounded — `(long)` would saturate. Every `Detail` string flows through this. |
| `PyFloat` | `Double.parseDouble` accepts `"100d"`, `"5f"` and hex; python's `float()` rejects them and accepts `"1_0"`, `"inf"`, `"nan"`. A `38=100d` must be *absent*, not 100. |
| `PyInt` | `Long.parseLong` rejects python's digit-group underscores. |
| `PyStrptime` | `%m`/`%d`/`%H`/`%M`/`%S` accept **one or two** digits, so python reads `2024011-14:30:00` as 2024-01-01; `DateTimeFormatter` rejects it. `java.time` also accepts proleptic year 0, which `datetime` does not. |
| `PyException` | python's `int(nan)` raises `ValueError` *after* the chain has been mutated, and the machine reports it as `Result.error`. Reproduced, so the two agree even on pathological input. |
| `PyDigits.strip` | `String.strip()` strips `Character.isWhitespace`, which by definition excludes the non-breaking spaces (U+00A0, U+2007, U+202F) and U+0085 NEL — all of which python strips. A value padded with one would parse in python and read as *absent* here. Verified to match python across all 65,536 BMP code points. |
| `PyDigits.isDigit` | python's `float()`/`int()` accept any Unicode decimal digit, so `38=١٢٣` is a quantity of 123. Also matched across the whole BMP. |

`PyDigits` also exists for a reason that is not about semantics: the obvious regex for a python
digit run, `\d(?:_?\d)*`, is implemented by `java.util.regex` with one stack frame per repetition,
so a FIX field carrying a few thousand digits threw `StackOverflowError`. That is an `Error`, not a
`RuntimeException` — it sailed past the state machine's catch *and* the Deephaven listener's, which
would have killed the stream permanently where python merely raises `ValueError`. The scanners are
O(n) with a constant stack, the catches now include `StackOverflowError`, and the listener catches
`Throwable`.

### The two deliberate deviations

Both are cases where python **raises** rather than reading a value differently, both are pinned by
tests rather than only described, and neither changes any value the two implementations both read.

1. **A `34 MsgSeqNum` beyond signed 64 bits** becomes a null `SeqNum`, where python holds an
   unbounded int that the Deephaven `long` column cannot represent either way — python only
   discovers that later, when the batch is built, and fails the whole batch into `ingest_errors`.
   Every in-range value parses identically (`MessageRow.optLong`, `RowModelTest`).
2. **A tag python's `int()` raises on** — a digit run longer than an `int`, or a character where
   `str.isdigit()` is true but `int()` still raises (`Numeric_Type=Digit` but not `Decimal`, such as
   the superscripts). python's `parse_fix` calls `int(tag_text)` unguarded, so the whole message
   becomes unparseable; this parser skips that one field and reads the rest. `java.lang.Character`
   exposes no `Numeric_Type` accessor, so matching it means shipping a Unicode table for the 95 BMP
   code points involved — not a trade worth making on an ASCII wire protocol (`FixParser.asTag`,
   `FixParserTest`).

## Tests

`./gradlew :deephaven-app-java:test` — 267 JUnit cases.

- **Rules, lifecycle, edge cases, lookups** mirror `deephaven-scripts/tests/test_state_machine_*.py`
  case for case, so a contract change fails in both languages.
- **`ParityAgainstPythonTest`** is the strongest claim: it replays
  `src/test/resources/parity/corpus.txt` and asserts **every column of every emitted row** equals
  what the python implementation produced — state snapshots, execution rows, event rows (including
  the human-readable `Detail` strings), audit rows and error strings. The corpus is deliberately
  hostile: unparseable input, unknown message types and enum codes, values containing `=` and `|`,
  duplicate and out-of-range tags, `nan`/`inf`/underscored/suffixed numbers, and timestamps that
  probe exactly where `strptime` and `DateTimeFormatter` disagree.

  Regenerate the golden after any deliberate contract change:

  ```bash
  python3 deephaven-app-java/parity/dump_python_rows.py \
      deephaven-app-java/src/test/resources/parity/corpus.txt \
      deephaven-app-java/src/test/resources/parity/expected.jsonl.gz
  ```

- **`SchemasTest`** pins the Deephaven schemas against `fixcache.Columns`, so the two declarations
  of the same frozen contract cannot drift apart.

End to end, `DH_APP=fix42-dashboard-java ./integration-test/run_integration.sh` runs the existing
six-test suite — including the container-restart replay-idempotence test — with no changes to it.

> Running the e2e suite twice **without** tearing the stack down fails four chains
> (`ORD-0003/0005/0010/0012` stuck in `PENDING_*`). That is a pre-existing property of the harness,
> not of either app: the generator republishes the same seeded chain keys onto a topic that already
> holds them, the trailing `F`/`G` re-opens a pending request, and its resolving `8` is discarded by
> the ExecID replay guard. The **python** app fails identically on the same dirty topic. Start from
> `podman compose ... down -v`.

## Dependencies, and why almost all of them are `compileOnly`

The server image already carries the whole engine on its own classpath, and `bin/start`
**prepends** `EXTRA_CLASSPATH` to it — so shipping a second copy of any Deephaven jar would shadow
the engine. Every `io.deephaven` dependency is therefore `compileOnly`, and
`:deephaven-app-java:assemble` stages exactly one jar into `build/deploy/libs`, which
`docker-compose.yml` mounts at `/apps/libs`.

The AMPS client is `compileOnly` for a different reason: it is commercial software and is not in
the image, exactly as `amps-python-client` is not (doc 03 §2.1). `KafkaIngest` and `AmpsRawSource`
each own their vendor imports, so the Kafka path never loads the AMPS client — the arrangement
python gets from its lazy `import AMPS`. To use `FIX42_SOURCE=amps`, drop `amps-client-<version>.jar`
into the mounted directory:

```bash
# exactly one jar -- the gradle cache holds several versions and a -sources jar, and
# EXTRA_CLASSPATH=/apps/libs/* would put all of them in front of the server's classpath
cp ~/.gradle/caches/modules-2/files-2.1/com.crankuptheamps/amps-client/5.3.4.1/*/amps-client-5.3.4.1.jar \
   deephaven-app-java/build/deploy/libs/
```

Note that `stageDeployLibs` is a `Sync` task, so it deletes anything it did not put there -- copy
the AMPS jar in *after* `assemble`, or keep it somewhere else and point `DH_JAVA_LIBS` at that
directory instead.

Configuration is identical to the python app's — the same `FIX42_*` variables, documented in
`docs/05-implementation-and-testing.md` §4.
