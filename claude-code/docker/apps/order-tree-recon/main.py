"""App entrypoint: the Raptor -> algo -> router order tree (doc 14).

Runs `order-tree-recon/src/order_tree_recon/app.py` -- mock native tables -> per-level
adapters -> linking / root walk / per-edge recon -> query API -> `deephaven.ui` dashboard.

  /otr-scripts = order-tree-recon/src  (the order_tree_recon package)

`globals()` is passed on purpose: the entrypoint's tables must land in *this* module's
namespace to become server-side globals. See /dh-app-lib/loader.py.
"""

import sys

sys.path.insert(0, "/dh-app-lib")
if "/otr-scripts" not in sys.path:
    sys.path.insert(0, "/otr-scripts")

from loader import load  # noqa: E402  (the path inserts above have to run first)

load(
    "/otr-scripts/order_tree_recon/app.py",
    globals(),
    app_name="order-tree-recon",
    scripts_dir="/otr-scripts",
)
