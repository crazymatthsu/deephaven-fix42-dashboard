"""App entrypoint: the historical market-data dashboard.

Runs `market-data-demo/src/market_data_demo/app.py` -- store (local parquet tree or S3)
-> inventory -> on-demand bar loader -> resampling / daily summary -> plotly-express
figures -> `deephaven.ui` dashboard.

  /md-scripts   = market-data-demo/src   (the market_data_demo package)
  /market-data  = market-data-demo/data  (MD_SOURCE=local: the YYYY/MM/DD/<SYMBOL>.parquet tree)

`globals()` is passed on purpose: the entrypoint's tables must land in *this* module's
namespace to become server-side globals. See /dh-app-lib/loader.py.
"""

import sys

sys.path.insert(0, "/dh-app-lib")
if "/md-scripts" not in sys.path:
    sys.path.insert(0, "/md-scripts")

from loader import load  # noqa: E402  (the path inserts above have to run first)

load(
    "/md-scripts/market_data_demo/app.py",
    globals(),
    app_name="market-data-demo",
    scripts_dir="/md-scripts",
)
