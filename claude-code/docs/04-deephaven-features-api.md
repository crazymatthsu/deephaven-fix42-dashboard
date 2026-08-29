# Deephaven Features & API Survey

The TODO asks for an analysis of Deephaven features/APIs relevant to this dashboard.
This is the working inventory: what we use, the exact entry points, and the gotchas.
(Implementers: verify signatures against the **pinned server version** in
`docker/docker-compose.yml`; the deephaven.ui API in particular evolves quickly.)

## 1. Core engine

- **Update Graph (UG):** all live tables form a DAG; a periodic cycle (default 1s
  target) propagates row additions/removals/modifications incrementally and atomically
  per cycle. Implication: dashboard latency ≈ one cycle after Kafka delivery.
- **Execution/Query scope:** scripts run server-side (embedded Python). Any table
  created inside a callback thread needs the captured execution context:
  `from deephaven.execution_context import get_exec_ctx; ctx = get_exec_ctx()` then
  `with ctx: ...` (required in our Kafka listener).
- **Query strings:** expressions like `where("OrderKey == `X`")` compile to Java;
  backticks quote string literals; python variables interpolate via f-strings; python
  functions are callable inside expressions (we avoid per-row python calls on hot
  paths — parsing happens once, in the listener).

## 2. Kafka integration — `deephaven.stream.kafka.consumer`

```python
from deephaven.stream.kafka import consumer as kc
t = kc.consume(kafka_config: dict, topic: str,
               partitions=None,                      # all
               offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
               key_spec=kc.simple_spec("ChainKey", dht.string),
               value_spec=kc.simple_spec("RawFix", dht.string),
               table_type=kc.TableType.blink())      # .append(), .ring(n)
```

- Adds `KafkaPartition`, `KafkaOffset`, `KafkaTimestamp` columns automatically.
- `simple_spec` maps the raw key/value to one column (our value is the raw FIX string).
  `json_spec`/`avro_spec`/`object_processor_spec` exist for structured payloads —
  not needed since we parse FIX ourselves.
- Consumer runs on its own thread; rows enter the UG on cycle boundaries.
- `kafka_config` passes through standard client properties
  (`bootstrap.servers`, `group.id`, …).

## 3. Programmatic streams — `table_publisher`

```python
from deephaven.stream.table_publisher import table_publisher
from deephaven import dtypes as dht
blink_tbl, pub = table_publisher(
    "OrderState", {"OrderKey": dht.string, "CumQty": dht.double, ...},
    on_shutdown_callback=None)
pub.add(new_table([...cols...]))    # append a batch (a static table with same schema)
pub.publish_failure(exc)            # poison the stream on fatal error
```

- Produces a **blink** table; combine with `last_by` for keyed latest-state caches.
- `new_table` + typed column factories (`string_col`, `double_col`, `long_col`,
  `bool_col`, `datetime_col` — the Instant factory; some versions name it
  `instant_col`, import defensively) build the batch; build one batch per update,
  not per row.

## 4. Listeners — `deephaven.table_listener`

```python
from deephaven.table_listener import listen
def on_update(update, is_replay):
    added = update.added()            # dict[str, numpy.ndarray] for the added rows
    ...
handle = listen(fix_raw, on_update, description="fix-state-machine")
```

- Callback runs on the UG thread — keep it O(added rows), no blocking I/O.
- For blink sources, `update.added()` per cycle is exactly the new rows — ideal for our
  fold. `is_replay` true on initial-snapshot replay (empty for blink).
- Must hold a strong reference to `handle` (module-level global) or the listener is GC'd.

## 5. Table ops used (the declarative DAG)

- `last_by(by)` — latest row per key; **blink-aggregation semantics** retain per-key
  state across cycles (the cache trick; doc 02 §1.1).
- `blink_to_append_only(t)` (`from deephaven.stream import blink_to_append_only`) —
  materialize full history for panels.
- `count_by`, `agg_by` (`from deephaven import agg`), `select_distinct`, `where`,
  `view`/`update_view` (lazy columns), `sort`/`sort_descending`, `natural_join`
  (only if panels ever need state columns joined onto history rows), `snapshot` /
  `snapshot_when` (frozen views; not needed in the live dashboard).
- Formatting: `format_columns` for status color-coding in plain-table fallback;
  deephaven.ui `ui.table` supports databars/formatting props in newer versions
  (use conservatively).

## 6. Dashboarding — `deephaven.ui` (+ plotly express)

`deephaven.ui` is Deephaven's React-style python component framework; ships in the
standard server Docker image (0.34+; verify with `pip show deephaven-plugin-ui` inside
the container, else `pip install deephaven-plugin-ui`).

Primitives we use:

```python
import deephaven.ui as ui

@ui.component
def orders_dashboard(state_tbl, execs_tbl, events_tbl, summary_tbl):
    selected, set_selected = ui.use_state("")           # OrderKey
    def on_press(row):                                   # row: dict col -> cell dict
        set_selected(str(row["OrderKey"]["value"]))
    execs = ui.use_memo(
        lambda: execs_tbl.where(f"OrderKey == `{selected}`") if selected else execs_tbl.head(0),
        [selected])
    ...
    return ui.column(
        ui.row(ui.panel(ui.table(state_tbl, on_row_press=on_press), title="Orders"), height=55),
        ui.row(ui.panel(ui.table(execs), title="Executions"),
               ui.panel(ui.table(events), title="Order History"), height=45))

dash = ui.dashboard(orders_dashboard(...))
```

- `ui.table(t, on_row_press=fn)`: `fn(row_data)` receives the pressed row as a dict of
  `{col: {"value": ..., "text": ...}}` — extract defensively (API shifted across
  versions; older signature was `fn(index, row_data)` — implementer must verify
  against the pinned version and adapt).
- `ui.use_state`, `ui.use_memo` — React-style hooks; memoize `where` results.
- Layout: `ui.dashboard`, `ui.row`, `ui.column`, `ui.panel`, `ui.stack`.
- Widgets: `ui.picker`/`ui.text_field` for Account/Symbol/status filters;
  `ui.badge`/`ui.text` for headline stats.
- Charts: `deephaven-plugin-plotly-express` (`import deephaven.plot.express as dx`)
  for a status bar chart — optional garnish, keep the dashboard functional without it.
- A named `ui.dashboard` assigned to a global appears in the web IDE's Panels menu;
  plain tables assigned to globals appear as panels too (fallback UX if ui plugin
  missing).

## 7. Deployment — Docker + Application Mode

- Image: `ghcr.io/deephaven/server:<pinned>` (python). Port 10000 (web IDE + gRPC).
  `START_OPTS=-Xmx4g -Ddeephaven.console.type=python`.
- **Application Mode** auto-runs one app's scripts at startup:
  `-Ddeephaven.application.dir=/app.d`, where `/app.d` is whichever folder under
  `docker/apps/` the `DH_APP` variable names (default `fix42-dashboard`). Deephaven
  loads **every** `.app` file in that one directory, so mounting a single app folder is
  what keeps apps from leaking into each other:

  ```
  type=script
  scriptType=python
  enabled=true
  id=fix42.dashboard
  name=FIX42 Order State Dashboard
  file_0=main.py
  ```

  `file_N` accepts an absolute container path **or** a path relative to the application
  dir (both verified on 42.4). Adding an app is adding a folder — `docker/apps/README.md`.

  Globals created by `app.py` (tables + the `ui.dashboard`) surface in the UI's
  Panels list. `app.py` adds `deephaven-scripts/src` to `sys.path` and wires
  ingest → DAG → dashboard.
- Auth: demo runs `AnonymousAuthenticationHandler`
  (`-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler`).
- Kafka reachable at the compose service name (`kafka:9092`) from inside the network;
  host tools (the Java generator) use the advertised `localhost:19092` listener.

## 8. Client access — `pydeephaven` (integration tests)

```python
from pydeephaven import Session
s = Session(host="localhost", port=10000)        # anonymous
tbl = s.open_table("order_state_latest")          # global from app mode
arrow = tbl.to_arrow(); df = arrow.to_pandas()    # assert on contents
s.run_script("print('scriptable too')")
```

- Speaks gRPC/Barrage on the same port 10000. Version should match the server minor.
- Integration test polls `order_state_latest` until row count settles, then asserts
  exact per-order end states (doc 05 §6).

## 9. Feature gotchas checklist (for implementers)

1. Hold references: listener handles AND publisher blink tables must stay in globals.
2. Wrap listener-side table construction in the captured exec context.
3. Build publisher batches with matching column order/types; `datetime_col` takes
   `datetime`/`Instant` values — convert epoch-nanos via `deephaven.time` helpers.
4. Blink `last_by` retains state — but a server restart clears it; recovery = Kafka
   seek-to-beginning (by design).
5. `ui.table` `on_row_press` payload shape must be verified on the pinned version.
6. Don't call blocking I/O or heavy python per-row in query strings; do work in the
   listener, once.
7. `where(f"... == `{sel}`")`: guard `sel` against backticks/injection (ids are
   generator-controlled alphanumerics; still sanitize).
8. Memory: append audit tables grow forever — acceptable demo tradeoff, documented.
