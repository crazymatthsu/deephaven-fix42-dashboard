"""App entrypoint: the FIX 4.2 order-state dashboard, built by the JAVA engine app.

This file is a *shim*, not an implementation. Ingest, the state-machine fold, the TablePublishers
and the whole derived DAG are built by `com.fix42.dashboard.dh.Fix42JavaApp` against the Deephaven
Java engine API, from the jar mounted at /apps/libs. Nothing here touches `dh_app` except the
dashboard, which cannot be written in Java at all.

Why python is in the loop:

1. `pydeephaven`'s `Session.open_table()` / `Session.tables` only resolve *query scope* tickets, so
   server-side globals are the only thing `integration-test/test_e2e.py` can see. Binding the Java
   tables into this module's `globals()` makes them scope variables, and the existing e2e suite and
   the web IDE Panels menu both work against this app unchanged.
2. `deephaven.ui` has no Java API in 42.4. The dashboard is pure presentation over the tables, so
   it is reused verbatim from `dh_app.dashboard` rather than reimplemented or dropped.

Binding into `globals()` also hands the tables to the script session's QueryScope, which manages
them for liveness -- a second, independent safety net alongside the LivenessScope that
`Fix42JavaApp.start()` holds.

`docker/apps/_lib/loader.py` is deliberately not used here: it exists to `exec()` a python
entrypoint out of /scripts, and this app has none.
"""

import traceback

import jpy
from deephaven.table import Table

#: Set to the failure text when the java app could not be started; None on success.
fix42_startup_error = None

try:
    _App = jpy.get_type("com.fix42.dashboard.dh.Fix42JavaApp")
    # Idempotent: a second call returns the memoized runtime rather than re-subscribing the source.
    _runtime = _App.start()
except Exception as exc:  # noqa: BLE001 - see below; app mode must not kill the server
    # This mirrors what `_lib/loader.py` does for script-based apps, and for the same reason: an
    # exception escaping an app-mode script is an uncaught exception on the server's main thread,
    # and Deephaven shuts the whole process down. The container then restarts into the same
    # failure, so there is no IDE to diagnose it from -- for what is most often simply a jar that
    # was never built.
    fix42_startup_error = "%s: %s" % (type(exc).__name__, exc)
    _runtime = None
    print(
        "[fix42-dashboard-java] ERROR: could not start the java app: %s\n"
        "[fix42-dashboard-java] The jar is mounted at /apps/libs and named on EXTRA_CLASSPATH.\n"
        "[fix42-dashboard-java] If the class was not found, build it first:\n"
        "[fix42-dashboard-java]     ./gradlew :deephaven-app-java:assemble\n"
        "[fix42-dashboard-java] then recreate the container (the mount is read at start).\n"
        "[fix42-dashboard-java] The server is still up: the IDE works, but this app published "
        "nothing." % (fix42_startup_error,),
        flush=True,
    )
    traceback.print_exc()


def _wrap(j_table):
    """Wrap a java Table handle so python (and deephaven.ui) can use it."""
    return Table(j_table) if j_table is not None else None


def _ident(value):
    """Render a lookup key the way python's ``sanitize_id`` accepts it.

    The java methods take a String, but ``dh_app.query_api.sanitize_id`` takes Any and stringifies,
    so ``get_by_order_id(1234)`` works against the python app. Coercing here keeps the two call
    signatures interchangeable instead of failing at the jpy boundary.
    """
    return "" if value is None else str(value)


if _runtime is not None:
    # --- tables: fix_raw + the 5 blink streams + the 11 derived nodes ------------------------
    _tables = _runtime.tables()
    for _key in _tables.keySet().toArray():
        globals()[str(_key)] = _wrap(_tables.get(_key))

    fix42_pipeline = _runtime.pipeline()
    _api = _runtime.queryApi()
else:
    fix42_pipeline = None
    _api = None


def get_by_order_id(order_id):
    """The live cache row(s) for a venue OrderID (tag 37)."""
    return _wrap(_api.getByOrderId(_ident(order_id))) if _api is not None else None


def get_by_clordid(clordid):
    """The live cache row for any ClOrdID in an amend chain."""
    return _wrap(_api.getByClOrdId(_ident(clordid))) if _api is not None else None


def get_by_execid(execid):
    """The live cache row owning an ExecID (tag 17)."""
    return _wrap(_api.getByExecId(_ident(execid))) if _api is not None else None


def find_by_account(account):
    """All live cache rows for an account (tag 1)."""
    return _wrap(_api.findByAccount(_ident(account))) if _api is not None else None


def find_by_symbol(symbol):
    """All live cache rows for a symbol (tag 55)."""
    return _wrap(_api.findBySymbol(_ident(symbol))) if _api is not None else None


def order_detail(order_key):
    """{'state': Table, 'executions': Table, 'events': Table} -- same shape as the python app."""
    if _api is None:
        return {}
    detail = _api.orderDetail(_ident(order_key))
    return {str(key): _wrap(detail.get(key)) for key in detail.keySet().toArray()}


# --- dashboard: python-only, reusing dh_app/dashboard.py over the JAVA tables -----------------
fix42_dashboard = None
try:
    if _runtime is None:
        raise RuntimeError("the java app did not start: %s" % (fix42_startup_error,))
    from dh_app.dashboard import build_dashboard  # PYTHONPATH=/scripts

    fix42_dashboard = build_dashboard(
        {
            name: globals()[name]
            for name in (
                "order_state_latest",
                "executions",
                "order_events",
                "status_summary",
                "open_orders",
                "account_list",
                "symbol_summary",
            )
        }
    )
    # build_dashboard RETURNS None when deephaven.ui is missing; it does not raise, so the banner
    # has to test the value rather than rely on reaching this line.
    _dashboard_status = (
        "fix42_dashboard (python deephaven.ui over the java tables)"
        if fix42_dashboard is not None
        else "unavailable (deephaven.ui not installed) -- use the table panels"
    )
except Exception as exc:  # noqa: BLE001 - the dashboard is garnish; the tables are the contract
    fix42_dashboard = None
    _dashboard_status = "unavailable (%r) -- use the table panels" % (exc,)

if _runtime is not None:
    print(_runtime.bannerText(_dashboard_status), flush=True)
