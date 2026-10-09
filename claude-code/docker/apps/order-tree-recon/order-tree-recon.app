# Deephaven Application Mode descriptor (doc 04 s7 / doc 14 section 9).
#
# Run from its own compose file (not docker-compose.yml):
#
#     podman compose -f docker/docker-compose.order-tree.yml up -d
#
# docker-compose.order-tree.yml mounts ./apps/order-tree-recon at /app.d and sets
# -Ddeephaven.application.dir=/app.d, so Deephaven picks up every .app file in *this*
# folder and no other.
#
# Everything main.py leaves in globals() -- raptor_orders, algo_orders, router_orders,
# the otr_* tables, order_tree / find_tree / exposure and order_tree_dashboard -- shows
# up in the web IDE's Panels menu and via pydeephaven's session.open_table().

type=script
scriptType=python
enabled=true
id=fix42.order.tree.recon
name=Order Tree Reconciliation
file_0=main.py
