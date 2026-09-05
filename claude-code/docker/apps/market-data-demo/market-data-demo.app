# Deephaven Application Mode descriptor (doc 04 s7 / doc 11 s11).
#
# Run from its own compose file (not docker-compose.yml):
#
#     podman compose -f docker/docker-compose.market-data.yml up -d --build
#
# docker-compose.market-data.yml mounts ./apps/market-data-demo at /app.d and sets
# -Ddeephaven.application.dir=/app.d, so Deephaven picks up every .app file in *this*
# folder and no other.
#
# Everything main.py leaves in globals() -- md_bars, md_daily_summary,
# md_inventory_symbols, md_inventory_days, the md_* query functions and
# market_data_dashboard -- shows up in the web IDE's Panels menu and via pydeephaven's
# session.open_table().
#
# The market_data_demo package is mounted at /md-scripts; the parquet tree at /market-data.

type=script
scriptType=python
enabled=true
id=fix42.market.data.demo
name=Market Data Demo
file_0=main.py
