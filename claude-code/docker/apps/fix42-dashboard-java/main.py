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

import jpy
from deephaven.table import Table

_App = jpy.get_type("com.fix42.dashboard.dh.Fix42JavaApp")

# Idempotent: a second call returns the memoized runtime rather than re-subscribing the source.
_runtime = _App.start()


def _wrap(j_table):
    """Wrap a java Table handle so python (and deephaven.ui) can use it."""
    return Table(j_table) if j_table is not None else None


# --- tables: fix_raw + the 5 blink streams + the 11 derived nodes ----------------------------
_tables = _runtime.tables()
for _key in _tables.keySet().toArray():
    globals()[str(_key)] = _wrap(_tables.get(_key))

fix42_pipeline = _runtime.pipeline()

# --- query api: the same six names the python app exports ------------------------------------
_api = _runtime.queryApi()


def get_by_order_id(order_id):
    """The live cache row(s) for a venue OrderID (tag 37)."""
    return _wrap(_api.getByOrderId(order_id))


def get_by_clordid(clordid):
    """The live cache row for any ClOrdID in an amend chain."""
    return _wrap(_api.getByClOrdId(clordid))


def get_by_execid(execid):
    """The live cache row owning an ExecID (tag 17)."""
    return _wrap(_api.getByExecId(execid))


def find_by_account(account):
    """All live cache rows for an account (tag 1)."""
    return _wrap(_api.findByAccount(account))


def find_by_symbol(symbol):
    """All live cache rows for a symbol (tag 55)."""
    return _wrap(_api.findBySymbol(symbol))


def order_detail(order_key):
    """{'state': Table, 'executions': Table, 'events': Table} -- same shape as the python app."""
    detail = _api.orderDetail(order_key)
    return {str(key): _wrap(detail.get(key)) for key in detail.keySet().toArray()}


# --- dashboard: python-only, reusing dh_app/dashboard.py over the JAVA tables -----------------
try:
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
    _dashboard_status = "fix42_dashboard (python deephaven.ui over the java tables)"
except Exception as exc:  # noqa: BLE001 - the dashboard is garnish; the tables are the contract
    fix42_dashboard = None
    _dashboard_status = "unavailable (%r) -- use the table panels" % (exc,)

print(_runtime.bannerText(_dashboard_status), flush=True)
