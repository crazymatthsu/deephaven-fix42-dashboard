"""Put the sibling ``multi_oms`` sources on ``sys.path`` for the unit suite.

``remote_uri`` reuses three *pure stdlib* modules from the multi-OMS blotter --
``multi_oms.config`` (topology parsing and validation), ``multi_oms.linking``
(name sanitising) and ``multi_oms.query_api`` (``sanitize_id``) -- exactly as doc 10
section 4 says it should: one JSON document describes the fleet whether it runs in
one server or in N.

In the container those modules arrive on ``PYTHONPATH`` from the ``/moms-scripts``
mount. On a bare host they arrive from here, rather than by pip-installing a sibling
module into this one's virtualenv: nothing is written outside this module's tree, and
the import path is the same shape as the deployed one.
"""

from __future__ import annotations

import os
import sys

_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_MODULE_DIR = os.path.dirname(_TESTS_DIR)
_MULTI_OMS_SRC = os.path.abspath(
    os.path.join(_MODULE_DIR, os.pardir, "deephaven-app-multi-oms-blotter", "src")
)

if os.path.isdir(_MULTI_OMS_SRC) and _MULTI_OMS_SRC not in sys.path:
    sys.path.insert(0, _MULTI_OMS_SRC)
