"""order_tree_recon -- the Raptor -> algo -> router order tree and its exposure recon (doc 14).

Layers:

* ``config``     -- level topology, break taxonomy, ``OTR_*`` settings (pure python)
* ``reference``  -- the python oracle of every DAG formula (pure python)
* ``mockdata``   -- native Raptor / algo / router mock tables + scenarios (pure python)
* ``query_api``  -- filter builders (pure) and the console API (server only at call time)
* ``sources``    -- native tables and the per-level adapters -- the file you rewrite (server only)
* ``dag``        -- linking, root walk, per-edge recon, ladder, exposure, tree (server only)
* ``dashboard``  -- the ``deephaven.ui`` dashboard (server only)
* ``app``        -- the Application-Mode entrypoint (server only)
"""

__version__ = "0.1.0"
