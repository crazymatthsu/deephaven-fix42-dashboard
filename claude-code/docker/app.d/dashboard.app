# Deephaven Application Mode descriptor (doc 04 s7).
#
# Picked up because docker-compose.yml sets -Ddeephaven.application.dir=/app.d
# and this folder is mounted there. Everything the script leaves in globals()
# (order_state_latest, executions, order_events, fix42_dashboard, ...) shows up
# in the web IDE's Panels menu and via pydeephaven's session.open_table().
#
# file_N paths were verified on ghcr.io/deephaven/server:42.4 to accept BOTH an
# absolute container path and a path relative to this application dir. We use
# the relative loader.py, which then executes /scripts/dh_app/app.py with a
# proper __file__ and sys.path -- see loader.py's docstring for why.

type=script
scriptType=python
enabled=true
id=fix42.dashboard
name=FIX42 Order State Dashboard
file_0=loader.py
