# Deephaven Application Mode descriptor (doc 04 s7 / doc 10 s11).
#
# Selected by docker/docker-compose.remote-uri.yml, which mounts
# ./apps/remote-uri-leaf at /app.d and sets -Ddeephaven.application.dir=/app.d, so
# Deephaven picks up every .app file in *this* folder and no other. Both leaves
# (rx-dh1, rx-dh2) run this same app; REMOTEURI_LEAF_NAME / REMOTEURI_LEAF_HUBS
# decide which server folds which hub tapes (doc 10 s4.2).
#
# Everything main.py leaves in globals() -- the per-hub raw AMPS blinks
# (oms_raw_oms_a, ...), the five oms_*_blink streams, oms_orders_latest,
# oms_executions, oms_events, id_index and the four exports the collector resolves
# (rx_orders, rx_id_index, rx_exposure, rx_leaf_stats) plus leaf_config -- shows up
# in the web IDE's Panels menu, via pydeephaven's session.open_table(), and through
# the scope URIs/tickets the collector uses (dh+plain://<host>:10000/scope/rx_orders,
# ticket b"s/rx_orders").
#
# Three source directories are mounted, not one:
#
#   /remote-scripts = deephaven-remote-uri/src              (the remote_uri package)
#   /moms-scripts   = deephaven-app-multi-oms-blotter/src   (multi_oms, UNCHANGED)
#   /scripts        = deephaven-scripts/src                 (fix42cache + dh_app, UNCHANGED)

type=script
scriptType=python
enabled=true
id=fix42.remote.uri.leaf
name=Remote-URI leaf
file_0=main.py
