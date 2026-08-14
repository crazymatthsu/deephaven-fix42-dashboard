"""Deephaven server-side application for the FIX 4.2 order-state dashboard.

This package adapts the pure-python :mod:`fix42cache` state machine to Deephaven
streaming tables.  It contains **no business logic** -- every FIX rule lives in
``fix42cache`` where it is unit tested (see ``docs/05-implementation-and-testing.md``
section 4).

Modules
-------
``schemas``
    Frozen column names / Deephaven dtypes for the published streams.
``ingest``
    Kafka source table (``fix_raw`` blink table).
``pipeline``
    The single stateful DAG node: a table listener folding raw FIX through the
    state machine and republishing normalized rows via ``TablePublisher``s.
``dag``
    Declarative derived nodes (cache, indexes, summaries).
``query_api``
    Point-lookup / scan functions over the derived tables.
``dashboard``
    ``deephaven.ui`` dashboard with three linked panels.
``app``
    Entry point wiring everything together and exporting globals.

Only :mod:`dh_app.schemas` is importable without Deephaven installed; every other
module imports ``deephaven`` at module scope and therefore only runs inside the
Deephaven server.
"""

__all__ = [
    "app",
    "dag",
    "dashboard",
    "ingest",
    "pipeline",
    "query_api",
    "schemas",
]

__version__ = "0.1.0"
