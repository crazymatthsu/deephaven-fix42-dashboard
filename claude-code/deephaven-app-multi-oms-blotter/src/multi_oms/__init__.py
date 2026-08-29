"""Multi-OMS drop-copy blotter -- doc 09.

Ingests FIX 4.2 drop-copy tapes from several OMS hubs, links orders across hubs
through configurable external-order-id tags and reconciles every parent against
the rollup of its *direct* children.

Import layers (deliberate, doc 09 section 10):

* :mod:`multi_oms.config` and :mod:`multi_oms.linking` are **pure stdlib** -- they
  import on a bare host python and carry the unit-test surface.
* :mod:`multi_oms.ingest`, :mod:`multi_oms.query_api` and :mod:`multi_oms.dashboard`
  keep their ``deephaven`` / ``deephaven.ui`` imports *inside* functions, so they
  are importable (and partly testable) without a Deephaven server too.
* :mod:`multi_oms.schemas`, :mod:`multi_oms.pipeline` and :mod:`multi_oms.dag`
  import ``deephaven`` at module scope: server-only, integration-tested.

``app.py`` memoizes its wired runtime as an attribute of *this* package object so a
second execution (Application Mode + a console re-run) re-exports the same tables
instead of opening a second subscription per hub.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
