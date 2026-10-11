"""order_tree_recon -- the Raptor -> algo -> router order tree and its exposure recon (doc 14).

Layers:

* ``config``     -- level topology, break taxonomy, ``OTR_*`` settings (pure python)
* ``reference``  -- the python oracle of every DAG formula (pure python)
* ``mockdata``   -- native Raptor / algo / router mock tables + scenarios (pure python)
* ``query_api``  -- filter builders (pure) and the console API (server only at call time)
* ``states``       -- the generic latest-order-state fold over named fields (pure python)
* ``fixstate``     -- FIX order states: parser, tag mapping, SOW mirror and fold twins (pure python)
* ``fixstate_mock``  -- the mock book as FIX order-state messages, ``fix-samples/`` (pure python)
* ``fixstate_check`` -- dry-run a mapping on files or a live AMPS SOW query (pure python + AMPS)
* ``sources``    -- native tables and the per-level adapters -- the file you rewrite (server only)
* ``states_dag``   -- the latest-state fold in Deephaven (server only)
* ``fixstate_dag`` -- FIX order-state events -> SOW mirror -> level tables + id history (server only)
* ``amps_sow``     -- ``sow_and_subscribe`` on AMPS SOW topics -> live event tables (server + AMPS)
* ``dag``        -- linking, root walk, per-edge recon, ladder, exposure, tree (server only)
* ``dashboard``  -- the ``deephaven.ui`` dashboard (server only)
* ``app``        -- the Application-Mode entrypoint (server only)
"""

__version__ = "0.1.0"
