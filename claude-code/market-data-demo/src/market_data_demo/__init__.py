"""Historical market-data demo for Deephaven -- doc 11.

Per-minute OHLC bars stored as parquet files under ``<root>/YYYY/MM/DD/<SYMBOL>.parquet``
on local disk or S3, loaded on demand into Deephaven and rendered as candlestick / OHLC /
line / area / normalized / volume charts for one or more symbols over a UI-chosen period.

Package map (pure python unless stated):

* :mod:`market_data_demo.layout`   -- the directory layout and date helpers
* :mod:`market_data_demo.mockgen`  -- deterministic mock bar generator (pyarrow)
* :mod:`market_data_demo.store`    -- ``LocalStore`` / ``S3Store`` file discovery
* :mod:`market_data_demo.config`   -- ``MD_*`` environment configuration
* :mod:`market_data_demo.reader`   -- parquet -> Deephaven tables (server only)
* :mod:`market_data_demo.derived`  -- resampling, daily summary, normalization (server only)
* :mod:`market_data_demo.charts`   -- ``deephaven.plot.express`` figures (server only)
* :mod:`market_data_demo.dashboard`-- the ``deephaven.ui`` dashboard (server only)
* :mod:`market_data_demo.query_api`-- console helpers (server only)
* :mod:`market_data_demo.app`      -- Application Mode entrypoint
* :mod:`market_data_demo.cli`      -- ``python -m market_data_demo generate|upload|list``
"""

__version__ = "0.1.0"
