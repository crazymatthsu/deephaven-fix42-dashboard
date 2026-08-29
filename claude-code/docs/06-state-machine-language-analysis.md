# State Machine Language Choice: Python vs Java

Should the FIX 4.2 order-state fold be written in Python (as built, `fix42cache`) or
Java (as in the sibling `fix42-oms-cache` library)? This records the trade-off
analysis and a **measured** throughput ceiling for the Python implementation, taken
inside the production container.

## 1. Context that frames the decision

The fold runs inside Deephaven's embedded Python, on the update-graph (UG) thread, as
a table listener that processes each Kafka row and republishes via `TablePublisher`
(doc 03 §2.2). Everything around it — ingestion wiring, DAG, query API, deephaven.ui
dashboard — is Python: that is Deephaven Community's scripting surface, and the project
TODO mandates Deephaven server-side Python scripting practices.

*(Written before the port. "Necessarily" would be too strong: the ingestion wiring, DAG and query
API all have Java engine equivalents, and `deephaven-app-java` uses them. Only the deephaven.ui
dashboard is genuinely Python-only — §3.)*

## 2. Comparison

| Dimension | Python (current) | Java |
|---|---|---|
| Deephaven integration | Native: listener, publishers, dashboard share one language and process | Jar on server classpath + custom image; then either Python/jpy glue anyway, or expert-level engine APIs (`TableUpdateListener`, stream adapters) |
| Throughput (measured / expected) | **~23–24k msg/s ceiling** (§4) | Hundreds of k–millions msg/s for an equivalent single-writer fold |
| Latency behavior | Listener holds the GIL on the UG thread; overload stretches the cycle for the whole graph | No GIL; predictable JVM tuning |
| Type safety | Test-time only (mitigated: 169 unit tests, mutation-checked) | Compile-time; sealed types + exhaustive switches suit FIX enums |
| Dev loop | Edit script, restart container | Build jar → rebuild image → restart |
| Core-logic testability | Pure stdlib package; pytest suite runs in ~0.1s, no JVM | Equally testable, heavier harness |
| Reuse | Written fresh to doc 01 | Could reuse the adversarially-reviewed `fix42-oms-cache` library nearly as-is |
| Ops surface | One runtime (the DH server process) | Two build artifacts, one runtime; version-skew between jar and scripts becomes possible |

### Why Python wins for this project

1. **The seam matters more than the fold.** A Java state machine still leaves Python
   owning the wiring and dashboard, adding an interop boundary through the most
   stateful component. (Empirically, the only end-to-end bug in this project was an
   integration seam — the Kafka topic-creation race — not fold logic.)
2. **The workload is branch-heavy dict manipulation**, not numeric crunching; demo
   and moderate production rates sit far below the measured ceiling.
3. **Iteration speed** — rule change → container restart → visible in dashboard.
4. **Correctness assurance is cheap**: the deephaven-free package made a fast,
   exhaustive unit suite possible (doc 05 §3.1).

### When Java becomes right

- Sustained rates approaching the ceiling (full-venue drop-copy days, burst replays).
- Tight tail-latency SLOs on cache updates.
- The cache must serve non-Deephaven JVM consumers directly.

One correction the port itself supplies to row 1 of the table above ("Deephaven integration"): the
Java path needs **no** custom image and **no** jpy glue. A jar on `EXTRA_CLASSPATH` and a
`.app` descriptor are the whole deployment. A ~30-line python shim is still wanted, but for
visibility rather than interop — see the module README on `open_table` tickets and `deephaven.ui`.

## 3. Escape hatch (deliberate design)

> **Status update — the hatch has been taken.** `deephaven-app-java` is a complete Java
> implementation of the whole app (option 2 below, not just the fold) against the Deephaven Java
> engine API. It runs in the same server, exports the same globals, and passes the same integration
> suite unmodified; `DH_APP=fix42-dashboard-java` selects it. The analysis below stands as the
> reasoning for why Python remains the *default* — nothing measured here has changed — and the
> throughput claim for the Java path is **not** re-measured: §4's numbers are Python's.
> See [`deephaven-app-java/README.md`](../deephaven-app-java/README.md).

`fix42cache` and `dh_app` communicate only through the frozen row schemas
(doc 01 §4/§6), so swapping the fold's engine is not a pipeline rewrite:

1. **Hybrid (practical first step):** the Python listener passes each cycle's raw
   messages *as one batch* across jpy to a Java `OmsCache.process()` and receives
   column arrays back — one boundary crossing per UG cycle, JVM-speed fold, all
   wiring/dashboard untouched.
2. **Full Java ingester extension** (Deephaven's own Kafka adapter is Java):
   highest throughput and effort; least-documented path.
3. **Python scale-out without Java:** shard the listener by Kafka partition
   (per-order ordering is per-partition already) into N machines publishing to the
   same publishers — linear-ish scaling at the cost of one fold instance per shard.

## 4. Measured ceiling (the benchmark)

**Method.** Corpus: 102,991 pipe-rendered FIX messages across 20,000 chains from the
Java generator (`--dry-run --orders 20000 --seed 7`, weighted scenario mix). Run
inside the running `fix42-deephaven` container (server image 42.4, Python 3.12.3,
aarch64 podman VM on an Apple-silicon host), single-threaded, against the mounted
`/scripts` package — i.e. the exact runtime that serves the dashboard. Fresh
`OrderStateMachine` per phase (so ExecID-dedupe paths aren't skewed); 5k-message
warm-up; results discarded per iteration exactly like the real listener. Two runs,
<5% variance. Script (with corpus + run instructions in its docstring):
[`deephaven-scripts/benchmarks/bench_fold.py`](../deephaven-scripts/benchmarks/bench_fold.py).

| Phase | Work measured | Result (2-run range) |
|---|---|---|
| `parse` | `parse_fix()` only | ~311–319k msg/s (3.1–3.2 µs/msg) |
| `fold` | `OrderStateMachine.process(raw)` | ~28.2–28.3k msg/s (35.4 µs/msg) |
| `fold+rows` | process + every `to_row()` dict the listener builds | **~23.0–24.1k msg/s (41.5–43.5 µs/msg)** |

Not included: Deephaven column construction, jpy crossing, and `publisher.add()` —
those are per-*batch* (once per UG cycle per stream), and observed cycle times in the
live stack are 1–3 ms, so they are second-order at these rates.

**Reading the numbers.**

- The python-side per-message ceiling is **~23–24k msg/s** on one core in the target
  environment. Parsing is ~8% of the cost; the fold logic dominates, so a faster
  parser buys little — the language of the *fold* is what matters.
- Deephaven's UG targets ~1s cycles. Saturating the thread consumes the entire
  cycle; a comfortable engineering budget (≤25% of cycle for ingestion, leaving
  headroom for the DAG, UI subscriptions, and bursts) puts the **sustained
  recommendation at ~6k msg/s**, with burst absorption to the low-20k range
  (Kafka buffers absorb what a cycle doesn't drain).
- Demo rates (generator default 50/s, stress 200/s) use <1% of the ceiling.

**Decision rule.** Below ~5k msg/s sustained: Python, no caveats. ~5–20k: Python
still works; watch UG cycle times (`PeriodicUpdateGraph` log lines) and consider the
partition-sharding option. Above ~20k sustained, tight tail-latency SLOs, or
non-Deephaven consumers: move the fold to Java via the hybrid in §3 — the row
contracts make it a drop-in swap.
