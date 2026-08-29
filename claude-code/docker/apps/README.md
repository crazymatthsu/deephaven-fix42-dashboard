# `docker/apps/` — one folder per Deephaven app

Each folder here is an independently launchable **Deephaven Application Mode** app. The
compose stack mounts exactly one of them at `/app.d` and runs it:

```bash
podman compose -f docker/docker-compose.yml up -d                            # fix42-dashboard
DH_APP=example-minimal podman compose -f docker/docker-compose.yml up -d     # something else
```

`DH_APP` names a folder in this directory and defaults to `fix42-dashboard`, so the
existing quickstart is unchanged. Only the selected folder is mounted, so Deephaven
sees that app's `.app` descriptors and no others — apps cannot leak into each other.

| App | What it publishes |
|---|---|
| `fix42-dashboard` | The full FIX 4.2 pipeline: `order_state_latest`, `executions`, `order_events`, `fix_messages`, the query API and `fix42_dashboard`. Needs Kafka (or AMPS — see `FIX42_SOURCE`). |
| `fix42-dashboard-java` | The same pipeline, built by `:deephaven-app-java` against the Deephaven **Java** engine API. Same table names, columns and values. Run `./gradlew :deephaven-app-java:assemble` first — the jar is mounted at `/apps/libs` and named on `EXTRA_CLASSPATH`. |
| `example-minimal` | One table, `example_heartbeat`. The copy-me template: it imports nothing from the repo, so bringing it up proves the `DH_APP` switch on its own. |

`fix42-dashboard-java` is also the worked example of an app whose `main.py` is a *shim* rather than
an entrypoint: it calls one static Java method and binds the returned tables into `globals()`. That
is what makes them reachable through `pydeephaven`'s `open_table` (which resolves scope tickets
only) and what lets the python-only `deephaven.ui` dashboard run over Java-built tables. See
[`deephaven-app-java/README.md`](../../deephaven-app-java/README.md).

## Adding an app

1. `cp -r example-minimal my-service`
2. Rename `example-minimal.app` → `my-service.app` and give it a unique `id=`.
   Deephaven loads **every** `.app` file in the mounted folder, and a duplicate `id`
   across two descriptors in the same folder is a conflict.
3. Point `main.py` at your entrypoint. If it lives under `deephaven-scripts/src/`, use
   the shared loader the way `fix42-dashboard/main.py` does:

   ```python
   import sys
   sys.path.insert(0, "/dh-app-lib")
   from loader import load
   load("/scripts/dh_app/app.py", globals(), app_name="my-service")
   ```

   Passing `globals()` is load-bearing: the entrypoint's tables have to land in your
   module's namespace to become server-side globals. `runpy` or `exec(code, {})` would
   build them in a throwaway namespace and nothing would appear in the IDE.
4. `DH_APP=my-service podman compose -f docker/docker-compose.yml up -d`

## `_lib/` — the shared loader

`_lib/loader.py` is mounted separately at `/dh-app-lib`, so every app shares one copy
instead of carrying its own. It exists because app-mode scripts run with `__file__`
**unset**; `load()` sets it before executing the entrypoint, puts `/scripts` on
`sys.path`, and turns a missing or raising entrypoint into one actionable log line with
the server left up so the IDE console still works.

The leading underscore keeps it from ever looking like a selectable app.

## Running two apps at once

Give the second one its own project name, port and container name, and skip the shared
dependencies it does not need:

```bash
DH_APP=example-minimal DH_PORT=10001 DH_CONTAINER=dh-example \
  podman compose -f docker/docker-compose.yml -p dh-example up -d --no-deps deephaven
```

`--no-deps` is what keeps it from starting a second Kafka. Drop it if the second app
actually needs the broker — but then point it at the running one rather than a new
instance, since two brokers on one machine will fight over `19092`.

Tear that one down on its own with `podman rm -f dh-example`.
