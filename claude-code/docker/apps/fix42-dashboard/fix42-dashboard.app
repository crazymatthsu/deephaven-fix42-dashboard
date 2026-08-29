# Deephaven Application Mode descriptor (doc 04 s7).
#
# Selected with DH_APP=fix42-dashboard (the default). docker-compose.yml mounts
# ./apps/${DH_APP} at /app.d and sets -Ddeephaven.application.dir=/app.d, so
# Deephaven picks up every .app file in *this* folder and no other.
#
# Everything main.py leaves in globals() (order_state_latest, executions,
# order_events, fix42_dashboard, ...) shows up in the web IDE's Panels menu and
# via pydeephaven's session.open_table().
#
# file_N paths were verified on ghcr.io/deephaven/server:42.4 to accept BOTH an
# absolute container path and a path relative to this application dir.

type=script
scriptType=python
enabled=true
id=fix42.dashboard
name=FIX42 Order State Dashboard
file_0=main.py
