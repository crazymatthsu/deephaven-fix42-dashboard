"""Render every row the python ``fix42cache`` emits for a corpus, in the parity encoding.

This produces ``src/test/resources/parity/expected.jsonl.gz`` (gzipped: the uncompressed golden
is half a megabyte of exhaustively-enumerated columns), which
``ParityAgainstPythonTest`` replays through the Java port and compares byte for byte. It is the
strongest statement this module makes: the two implementations are not merely both "correct
according to doc 01", they agree on every column of every row.

Run it from the repo root after changing either implementation::

    python3 deephaven-app-java/parity/dump_python_rows.py \
        deephaven-app-java/src/test/resources/parity/corpus.txt \
        deephaven-app-java/src/test/resources/parity/expected.jsonl.gz

The encoding is deliberately language-neutral so the two sides produce identical *text*:

* keys are sorted;
* an Instant is its epoch-nanosecond count (python renders 6 fraction digits, java the shortest
  form, so the ISO strings would differ while denoting the same instant);
* a float is its IEEE-754 bit pattern as a signed 64-bit integer -- exact, and free of
  ``repr`` differences such as python's ``1e+20`` versus java's ``1.0E20``;
* everything else is plain JSON.
"""

import gzip
import io
import json
import struct
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "deephaven-scripts" / "src"))

from fix42cache import OrderStateMachine  # noqa: E402

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
#: Must match FakeClock in the java test sources.
CLOCK_START = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
CLOCK_STEP = timedelta(milliseconds=1)


class FakeClock:
    def __init__(self):
        self.current = CLOCK_START

    def __call__(self):
        value = self.current
        self.current += CLOCK_STEP
        return value


def encode(value):
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, datetime):
        delta = value.astimezone(timezone.utc) - EPOCH
        return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000
    if isinstance(value, float):
        return struct.unpack(">q", struct.pack(">d", value))[0]
    if isinstance(value, int):
        return value
    raise TypeError(f"unhandled {type(value).__name__}: {value!r}")


def row(mapping):
    return {key: encode(val) for key, val in mapping.items()}


def main(corpus_path, out_path):
    machine = OrderStateMachine(now_fn=FakeClock())
    lines = 0
    # mtime=0 keeps the gzip header byte-stable, so regenerating an unchanged golden produces no
    # git diff.
    with open(corpus_path, encoding="utf-8") as source, io.TextIOWrapper(
            gzip.GzipFile(out_path, "wb", compresslevel=9, mtime=0), encoding="utf-8") as out:
        for raw in source:
            raw = raw.rstrip("\n")
            if not raw:
                continue
            result = machine.process(raw)
            out.write(json.dumps({
                "error": result.error,
                "events": [row(e.to_row()) for e in result.events],
                "executions": [row(e.to_row()) for e in result.executions],
                "message": row(result.message.to_row()) if result.message is not None else None,
                "state": row(result.state.to_row()) if result.state is not None else None,
            }, sort_keys=True, separators=(",", ":")) + "\n")
            lines += 1
        # A final line holding every chain snapshot, so the fold's end state is pinned too.
        out.write(json.dumps(
            {"finalChains": [row(s.to_row()) for s in machine.snapshot_all()]},
            sort_keys=True, separators=(",", ":")) + "\n")
    print(f"{lines} messages -> {out_path} ({machine.order_count()} chains)")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
