"""basket_oms_demo -- Deephaven as an equities OMS front end, on mock orders.

Layers (doc 13 §7):

* ``core``      -- baskets, orders, transitions, roll-ups (pure python, no deephaven)
* ``mockdata``  -- the seeded demo baskets and the symbol universe (pure python)
* ``venue``     -- the mock venue: acks, fills, rejects, quotes (pure python)
* ``config``    -- ``OMS_*`` environment variables (pure python)
* ``tables``    -- the Deephaven input tables and derived views (server only)
* ``simulator`` -- the venue thread (server only)
* ``dashboard`` -- the ``deephaven.ui`` dashboard (server only)
* ``app``       -- the Application-Mode entrypoint wiring it all together (server only)
"""

__version__ = "0.1.0"
