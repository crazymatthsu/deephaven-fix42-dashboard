"""Benchmark the pure-python FIX fold (see docs/06-state-machine-language-analysis.md).

Measures, over a generator-produced corpus, with a fresh OrderStateMachine per phase:
  parse      -- fix42cache.parser.parse_fix only
  fold       -- OrderStateMachine.process(raw)
  fold+rows  -- process(raw) + every .to_row() dict the Pipeline listener builds
Phase 3 approximates the python-side per-message ceiling of the listener; deephaven
column building / jpy / publisher.add are per-batch (per UG cycle) and excluded.

Produce a corpus (host):
  ./gradlew --quiet :fix-mock-generator:run \
      --args="--dry-run --orders 20000 --seed 7" 2>/dev/null | grep '^8=FIX' > /tmp/fix_corpus.txt

Run inside the live container (the environment that matters):
  podman cp /tmp/fix_corpus.txt fix42-deephaven:/tmp/fix_corpus.txt
  podman cp deephaven-scripts/benchmarks/bench_fold.py fix42-deephaven:/tmp/bench_fold.py
  podman exec fix42-deephaven /opt/deephaven/venv/bin/python /tmp/bench_fold.py

Or on the host from deephaven-scripts/:
  PYTHONPATH=src python3 benchmarks/bench_fold.py /tmp/fix_corpus.txt
"""

import sys
import time

sys.path.insert(0, "/scripts")  # container mount; harmless elsewhere

from fix42cache.parser import parse_fix
from fix42cache.state_machine import OrderStateMachine

CORPUS = sys.argv[1] if len(sys.argv) > 1 else "/tmp/fix_corpus.txt"
WARMUP = 5000


def load() -> list[str]:
    with open(CORPUS, "r", encoding="ascii") as f:
        return [line.strip() for line in f if line.strip()]


def timed(label: str, n: int, fn) -> float:
    t0 = time.perf_counter()
    fn()
    dt = time.perf_counter() - t0
    rate = n / dt
    print(f"{label:<10} {dt:8.3f}s  {rate:>10,.0f} msg/s  {1e6 * dt / n:8.2f} us/msg")
    return rate


def main() -> None:
    msgs = load()
    n = len(msgs)
    print(f"python {sys.version.split()[0]}  corpus {n:,} messages")

    warm = OrderStateMachine()
    for raw in msgs[:WARMUP]:
        warm.process(raw)

    def parse_all() -> None:
        for raw in msgs:
            parse_fix(raw)

    timed("parse", n, parse_all)

    m2 = OrderStateMachine()

    def fold_only() -> None:  # discard results, like the real listener
        for raw in msgs:
            m2.process(raw)

    timed("fold", n, fold_only)

    m3 = OrderStateMachine()

    def fold_rows() -> None:
        for raw in msgs:
            r = m3.process(raw)
            if r.error is not None:
                continue
            r.state.to_row()
            for e in r.executions:
                e.to_row()
            for e in r.events:
                e.to_row()
            r.message.to_row()

    rate3 = timed("fold+rows", n, fold_rows)
    print(f"orders folded: {m3.order_count():,}")
    print(f"UG-cycle framing: saturation ~{rate3:,.0f} msg/s; comfortable sustained "
          f"budget (<=25% of a 1s cycle) ~{rate3 * 0.25:,.0f} msg/s")


if __name__ == "__main__":
    main()
