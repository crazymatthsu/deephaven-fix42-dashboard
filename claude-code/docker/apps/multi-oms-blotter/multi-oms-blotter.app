# Deephaven Application Mode descriptor (doc 04 s7 / doc 09 s9).
#
# Selected with DH_APP=multi-oms-blotter:
#
#     DH_APP=multi-oms-blotter podman compose -f docker/docker-compose.yml up -d
#
# docker-compose.yml mounts ./apps/${DH_APP} at /app.d and sets
# -Ddeephaven.application.dir=/app.d, so Deephaven picks up every .app file in
# *this* folder and no other.
#
# Everything main.py leaves in globals() -- orders_recon, chain_recon, oms_breaks,
# orders_tree, the per-hub raw blinks (oms_raw_oms_a, ...), the query API and
# multi_oms_blotter -- shows up in the web IDE's Panels menu and via pydeephaven's
# session.open_table(). `orders_tree` is a hierarchical table: it is reachable from
# the IDE and from run_script, but not from open_table (not a plain-table ticket).
#
# The multi_oms package is mounted at /moms-scripts, separately from /scripts:
# fix42cache is reused UNCHANGED from the single-hub app (doc 09 s4).

type=script
scriptType=python
enabled=true
id=fix42.multi.oms.blotter
name=Multi-OMS Drop-Copy Blotter
file_0=main.py
