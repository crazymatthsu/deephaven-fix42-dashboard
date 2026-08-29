# Deephaven Application Mode descriptor (doc 04 s7) -- the Java engine build of the app.
#
# Selected with DH_APP=fix42-dashboard-java. docker-compose.yml mounts ./apps/${DH_APP} at /app.d
# and sets -Ddeephaven.application.dir=/app.d, so Deephaven picks up every .app file in *this*
# folder and no other.
#
# type=script (rather than type=dynamic) on purpose, even though the whole app is Java. Deephaven
# offers both, and they differ on exactly one thing that matters here:
#
#   type=dynamic  -> tables land in ApplicationState fields, ticket a/<appId>/f/<name>
#   type=script   -> tables land in the python script session's query scope, ticket s/<name>
#
# pydeephaven's Session.open_table() builds a scope ticket unconditionally, and
# integration-test/test_e2e.py uses it for all 11 required globals -- so only the script route
# lets the existing end-to-end suite assert this app. It is also what makes the deephaven.ui
# dashboard reusable, since that plugin is python-only (see main.py).
#
# main.py contains no business logic. Every table is built by com.fix42.dashboard.dh.Fix42JavaApp
# against the Deephaven *Java* engine API, from the jar mounted at /apps/libs.
# The pure-Java route is still available: see Fix42JavaApp.create(Listener).

type=script
scriptType=python
enabled=true
id=fix42.dashboard.java
name=FIX42 Order State Dashboard (Java engine)
file_0=main.py
