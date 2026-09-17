# Deephaven Application Mode descriptor (doc 04 s7 / doc 13 §6).
#
# Run from its own compose file (not docker-compose.yml):
#
#     podman compose -f docker/docker-compose.basket-oms.yml up -d
#
# docker-compose.basket-oms.yml mounts ./apps/basket-oms-demo at /app.d and sets
# -Ddeephaven.application.dir=/app.d, so Deephaven picks up every .app file in *this*
# folder and no other.
#
# Everything main.py leaves in globals() -- oms_baskets, oms_orders, oms_events,
# oms_executions, oms_quotes, the oms_*_view tables, the oms_* functions and
# basket_oms_dashboard -- shows up in the web IDE's Panels menu and via pydeephaven's
# session.open_table().

type=script
scriptType=python
enabled=true
id=fix42.basket.oms.demo
name=Basket OMS Demo
file_0=main.py
