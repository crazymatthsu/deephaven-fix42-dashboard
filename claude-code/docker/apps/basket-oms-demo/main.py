"""App entrypoint: the basket OMS demo.

Runs `basket-oms-demo/src/basket_oms_demo/app.py` -- config -> order core -> input
tables -> seeded mock baskets -> mock venue thread -> console API -> `deephaven.ui`
dashboard.

  /oms-scripts = basket-oms-demo/src  (the basket_oms_demo package)

`globals()` is passed on purpose: the entrypoint's tables must land in *this* module's
namespace to become server-side globals. See /dh-app-lib/loader.py.
"""

import sys

sys.path.insert(0, "/dh-app-lib")
if "/oms-scripts" not in sys.path:
    sys.path.insert(0, "/oms-scripts")

from loader import load  # noqa: E402  (the path inserts above have to run first)

load(
    "/oms-scripts/basket_oms_demo/app.py",
    globals(),
    app_name="basket-oms-demo",
    scripts_dir="/oms-scripts",
)
