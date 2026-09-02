# Deephaven Application Mode descriptor (doc 04 s7 / doc 10 s11).
#
# Selected by docker/docker-compose.remote-uri.yml, which mounts
# ./apps/remote-uri-collector at /app.d and sets -Ddeephaven.application.dir=/app.d,
# so Deephaven picks up every .app file in *this* folder and no other.
#
# The collector holds no FIX fold of its own: it resolves each leaf's four exports
# (REMOTEURI_LEAVES, doc 10 s4.3), merges them, re-links the cross-hub families with
# doc 09's builders over the FULL topology, marks them against a simulated
# market-data table and publishes orders_marked / exposure_by_level /
# exposure_by_source / exposure_by_leaf / fleet, the doc 10 s9 query API
# (find_exposure, exposure_for, remote_executions, snapshot_leaf, reconnect, ...)
# and remote_uri_dashboard.
#
# Same three mounts as the leaf app; the collector additionally needs
# amps-python-client only because it shares the image, not because it talks to AMPS.

type=script
scriptType=python
enabled=true
id=fix42.remote.uri.collector
name=Remote-URI collector
file_0=main.py
