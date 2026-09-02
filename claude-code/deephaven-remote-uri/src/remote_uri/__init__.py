"""Remote-URI leaves and collector -- doc 10.

The doc 09 multi-OMS order flow spread across several Deephaven servers: each
**leaf** folds a subset of the OMS hub tapes read from AMPS and exports a narrow
projection plus aggregates; the **collector** acquires those exports through
Deephaven's remote-table mechanisms (``deephaven.uri.resolve`` / Barrage
subscriptions for push, Barrage snapshots and remote console execution for pull),
re-links the cross-hub families, marks them against a simulated market-data table
and answers the (source OMS, account, symbol) exposure lookup.

Import layers (deliberate, doc 10 section 12):

* :mod:`remote_uri.config`, :mod:`remote_uri.uris`, :mod:`remote_uri.exposure`,
  :mod:`remote_uri.marketdata` and :mod:`remote_uri.search` are **pure stdlib**
  (plus the equally pure ``multi_oms.config`` / ``.linking`` / ``.query_api``) --
  they import on a bare host python and carry the unit-test surface.
* :mod:`remote_uri.ingest`, :mod:`remote_uri.remote`, :mod:`remote_uri.query_api`
  and :mod:`remote_uri.dashboard` keep their ``deephaven`` / ``AMPS`` imports
  *inside* functions, so they are importable without a Deephaven server.
* :mod:`remote_uri.leaf`, :mod:`remote_uri.collector` and
  :mod:`remote_uri.marketdata_table` import ``deephaven`` at module scope:
  server-only, integration-tested by the doc 10 section 12 e2e.

``app.py`` memoizes its wired runtime as an attribute of *this* package object so a
second execution (Application Mode + a console re-run) re-exports the same tables
instead of opening a second AMPS subscription or a second set of Barrage
subscriptions.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
