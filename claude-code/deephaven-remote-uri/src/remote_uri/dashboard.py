"""``remote_uri_dashboard`` -- doc 10 section 8.

The search UI over the fleet::

    +-------------------------------------------------------------------------------+
    | Source OMS v  Account v  Symbol v  [Find] [Clear]           fleet (stats)      |
    +--------------------------------------+----------------------------------------+
    | Families upstream -> downstream      | Totals by level                        |
    | (orders_marked, click a hop)         | + the root-level totals                |
    +--------------------------------------+------------------------+---------------+
    | Executions of the selected hop       | Market data (latest)   | Per-leaf sums |
    | = REMOTE CALL to the owning leaf     |                        |               |
    +--------------------------------------+------------------------+---------------+

The one genuinely new interaction versus doc 09's blotter: pressing a row does not
filter a local table, it runs a **remote query on another server**. That cannot
happen on the render path, so the press stores ``(GlobalKey, nonce)``, a
``use_effect`` runs the call on a worker thread, and a "loading..." text shows
meanwhile. The result is a *static* snapshot, so it needs no liveness scope.

Version tolerance is doc 09 section 6's, copied rather than imported (doc 05 section
8's module-ownership rule): ``deephaven.ui`` is imported lazily and the builder
returns ``None`` when the plugin is missing, ``_safe()`` wraps every optional widget
and ``on_row_press`` accepts every known payload shape.
"""

from __future__ import annotations

import threading
import time
import traceback
from typing import Any, Callable, List, Mapping, Optional, Sequence, Tuple

from multi_oms.query_api import sanitize_id

from remote_uri import search
from remote_uri.config import CollectorSettings

__all__ = [
    "FAMILY_COLUMNS",
    "EXECUTION_COLUMNS",
    "build_dashboard",
]

#: Left-to-right column order of the families panel. ``GlobalKey``/``RootKey`` trail
#: the display columns because the row-press handler reads them out of the payload.
FAMILY_COLUMNS: Tuple[str, ...] = (
    "Leaf",
    "Oms",
    "HubDepth",
    "Depth",
    "ClOrdID",
    "OrderID",
    "ExtOrdID",
    "LinkState",
    "Account",
    "Symbol",
    "Side",
    "OrdStatus",
    "OrderQty",
    "CumQty",
    "LeavesQty",
    "AvgPx",
    "MarkPx",
    "ExecNotional",
    "OpenNotional",
    "TotalNotional",
    "SignedExposure",
    "BreakKind",
    "GlobalKey",
    "RootKey",
)

#: The remote-call panel's columns (a leaf's ``oms_executions`` projection).
EXECUTION_COLUMNS: Tuple[str, ...] = (
    "Oms",
    "ExecID",
    "ExecType",
    "OrdStatus",
    "LastShares",
    "LastPx",
    "CumQty",
    "LeavesQty",
    "AvgPx",
    "IsFill",
    "TransactTime",
)

#: Keys a row payload may be delivered under when the callback uses keyword args.
_ROW_KWARGS = ("row", "row_data", "data", "item")
#: Keys a cell payload may carry the display/typed value under.
_CELL_KEYS = ("value", "text", "raw_value")


# --------------------------------------------------------------------------------------
# Defensive helpers (copied from multi_oms.dashboard -- doc 05 section 8)
# --------------------------------------------------------------------------------------


def _extract_row(args: Sequence[Any], kwargs: Mapping[str, Any]) -> Optional[Any]:
    """Find the row payload in an ``on_row_press`` invocation."""
    for name in _ROW_KWARGS:
        candidate = kwargs.get(name)
        if candidate is not None:
            return candidate
    positional = list(args)
    for candidate in reversed(positional):
        if isinstance(candidate, Mapping):
            return candidate
    for candidate in reversed(positional):
        if candidate is not None and not isinstance(candidate, (bool, int, float, str, bytes)):
            return candidate
    return None


def _lookup(container: Any, key: str) -> Any:
    """Best-effort ``container[key]`` / ``container.key`` lookup."""
    if container is None:
        return None
    if isinstance(container, Mapping):
        return container.get(key)
    try:
        return container[key]
    except Exception:  # noqa: BLE001 - not subscriptable / no such key
        return getattr(container, key, None)


def _cell_value(row: Any, column: str) -> str:
    """Read one column out of a row payload, whatever the cell shape."""
    cell = _lookup(row, column)
    if cell is None:
        return ""
    if isinstance(cell, (str, bytes)):
        return cell.decode() if isinstance(cell, bytes) else cell
    for key in _CELL_KEYS:
        nested = _lookup(cell, key)
        if nested is not None:
            return str(nested)
    return str(cell)


def _safe(factory: Callable[[], Any]) -> Optional[Any]:
    """Build an optional UI element, returning ``None`` if this version rejects it."""
    try:
        return factory()
    except Exception:  # noqa: BLE001 - optional garnish only
        return None


def _first(*factories: Callable[[], Any]) -> Optional[Any]:
    """Return the first factory's result that does not raise (``None`` if all do)."""
    for factory in factories:
        built = _safe(factory)
        if built is not None:
            return built
    return None


def _selection_handler(setter: Callable[[Tuple[str, int]], None]) -> Callable[..., None]:
    """Wrap ``setter`` in a signature-agnostic ``on_row_press`` callback.

    Stores ``(GlobalKey, nonce)``. The nonce is what makes pressing the *same* row
    twice re-run the remote call: without it the ``use_effect`` deps would not change
    and a stale result would stay on screen.
    """

    def handler(*args: Any, **kwargs: Any) -> None:
        try:
            row = _extract_row(args, kwargs)
            key = _cell_value(row, "GlobalKey")
        except Exception:  # noqa: BLE001 - a UI callback must never raise
            return
        if key:
            setter((key, time.monotonic_ns()))

    return handler


def _view(table: Any, columns: Sequence[str]) -> Any:
    """Apply a display column order, falling back to the full table."""
    viewed = _safe(lambda: table.view(list(columns)))
    return table if viewed is None else viewed


# --------------------------------------------------------------------------------------
# The dashboard
# --------------------------------------------------------------------------------------


def build_dashboard(
    settings: CollectorSettings,
    tables: Mapping[str, Any],
    api: Mapping[str, Callable[..., Any]],
) -> Optional[Any]:
    """Build the fleet search dashboard.

    Args:
        settings: The validated collector configuration (the leaf list drives the
            panel titles that name which server a remote call went to).
        tables: The dict returned by :func:`remote_uri.collector.build_collector`.
        api: The dict returned by :func:`remote_uri.query_api.make_query_api`.

    Returns:
        A ``ui.dashboard`` element, or ``None`` when ``deephaven.ui`` is not
        installed -- every table is exported as a global regardless, so the fleet is
        still usable from the Panels menu and from ``pydeephaven``.
    """
    try:
        import deephaven.ui as ui
    except ImportError:  # pragma: no cover - server without the ui plugin
        print("[remote-uri] deephaven.ui not available; falling back to plain table panels")
        return None

    # The three filtered views come from the query API, so the dashboard and a
    # console call cannot drift apart; the four panels below are shown whole.
    exposure_by_leaf = tables["exposure_by_leaf"]
    market_data_latest = tables["market_data_latest"]
    fleet = tables["fleet"]
    source_oms_list = tables["source_oms_list"]
    account_list = tables["account_list"]
    symbol_list = tables["symbol_list"]

    find_exposure = api["find_exposure"]
    family_totals = api["family_totals"]
    exposure_for = api["exposure_for"]
    remote_executions = api["remote_executions"]
    leaf_of = api["leaf_of"]

    @ui.component
    def remote_uri_component() -> Any:
        """Pickers + families + totals + the remote-call executions panel."""
        # -- the pending (typed) filter and the applied one. [Find] promotes the
        # first to the second: a lookup across a fleet is a deliberate act, not
        # something to re-run on every keystroke.
        pending_oms, set_pending_oms = ui.use_state(None)
        pending_account, set_pending_account = ui.use_state(None)
        pending_symbol, set_pending_symbol = ui.use_state(None)
        applied, set_applied = ui.use_state(("", "", ""))
        request, set_request = ui.use_state(("", 0))
        result, set_result = ui.use_state(None)

        source_oms, account, symbol = applied
        selected_key, nonce = request

        families = ui.use_memo(
            lambda: _view(find_exposure(source_oms, account, symbol), FAMILY_COLUMNS),
            [applied],
        )
        levels = ui.use_memo(lambda: family_totals(source_oms, account, symbol), [applied])
        totals = ui.use_memo(lambda: exposure_for(source_oms, account, symbol), [applied])

        # -- the remote call ---------------------------------------------------
        # Runs off the render path on a worker thread: a Barrage round trip to
        # another server must never block the UI's update cycle. The result is a
        # static snapshot, so it needs no liveness scope.
        def run_remote_query() -> None:
            """Kick off (or clear) the remote executions query for the pressed hop."""
            if not selected_key:
                set_result(None)
                return

            def worker() -> None:
                try:
                    table = remote_executions(selected_key)
                    set_result((nonce, table, ""))
                except Exception as exc:  # noqa: BLE001 - a leaf being down is normal
                    traceback.print_exc()
                    set_result((nonce, None, f"{type(exc).__name__}: {exc}"))

            threading.Thread(
                target=worker, name=f"remote-uri-query-{nonce}", daemon=True
            ).start()

        ui.use_effect(run_remote_query, [request])

        loading = bool(selected_key) and (result is None or result[0] != nonce)
        executions = result[1] if (result is not None and result[0] == nonce) else None
        remote_error = result[2] if (result is not None and result[0] == nonce) else ""
        owner = leaf_of(selected_key.split("|", 1)[0]) if selected_key else ""

        def apply_filters() -> None:
            """[Find]: promote the pickers to the applied filter."""
            set_applied(
                (
                    sanitize_id(pending_oms),
                    sanitize_id(pending_account),
                    sanitize_id(pending_symbol),
                )
            )
            set_request(("", 0))
            set_result(None)

        def clear_filters() -> None:
            """[Clear]: back to "everything"."""
            set_pending_oms(None)
            set_pending_account(None)
            set_pending_symbol(None)
            set_applied(("", "", ""))
            set_request(("", 0))
            set_result(None)

        # -- controls -----------------------------------------------------------
        pickers = [
            _first(
                lambda: ui.picker(
                    source_oms_list,
                    label="Source OMS",
                    selected_key=pending_oms,
                    on_change=set_pending_oms,
                ),
                lambda: ui.text_field(
                    label="Source OMS", value=pending_oms or "", on_change=set_pending_oms
                ),
            ),
            _first(
                lambda: ui.picker(
                    account_list,
                    label="Account",
                    selected_key=pending_account,
                    on_change=set_pending_account,
                ),
                lambda: ui.text_field(
                    label="Account", value=pending_account or "", on_change=set_pending_account
                ),
            ),
            _first(
                lambda: ui.picker(
                    symbol_list,
                    label="Symbol",
                    selected_key=pending_symbol,
                    on_change=set_pending_symbol,
                ),
                lambda: ui.text_field(
                    label="Symbol", value=pending_symbol or "", on_change=set_pending_symbol
                ),
            ),
        ]
        buttons = [
            _safe(lambda: ui.button("Find", on_press=apply_filters)),
            _safe(lambda: ui.button("Clear", on_press=clear_filters)),
        ]
        controls: List[Any] = [element for element in pickers if element is not None]
        controls.extend(element for element in buttons if element is not None)
        controls.append(
            ui.text(f"showing: {search.describe_filters(source_oms, account, symbol)}")
        )
        controls.append(
            ui.text(f"leaves: {', '.join(settings.leaf_names)}")
        )

        # -- the remote-call panel ----------------------------------------------
        if not selected_key:
            executions_panel: Any = ui.text(
                "Click a hop in the families panel to fetch its executions "
                "from the leaf that owns it (a remote query)."
            )
            executions_title = "Executions (remote call)"
        elif loading:
            executions_panel = ui.text(f"loading {selected_key} from leaf {owner or '?'}...")
            executions_title = f"Executions of {selected_key} (remote call)"
        elif executions is None:
            executions_panel = ui.text(
                f"the remote query for {selected_key} failed: {remote_error}\n"
                "The leaf may be down -- run reconnect() in the console once it is back."
            )
            executions_title = f"Executions of {selected_key} -- FAILED"
        else:
            executions_panel = ui.table(_view(executions, EXECUTION_COLUMNS))
            executions_title = f"Executions of {selected_key} (remote call to {owner})"

        return ui.column(
            ui.row(
                ui.panel(
                    ui.flex(*controls, direction="row", gap="size-150", wrap=True, align_items="end"),
                    title="Fleet exposure -- source OMS / account / symbol",
                ),
                ui.panel(ui.table(totals), title="Totals (root level)"),
                height=20,
            ),
            ui.row(
                ui.panel(
                    _first(
                        # GlobalKey trails the display columns, so it is off-screen in a
                        # narrow panel; on_row_press only carries viewport columns unless
                        # always_fetch_columns names it (deephaven.ui 0.40 semantics).
                        lambda: ui.table(
                            families,
                            on_row_press=_selection_handler(set_request),
                            always_fetch_columns=["GlobalKey"],
                        ),
                        lambda: ui.table(families, on_row_press=_selection_handler(set_request)),
                    ),
                    title="Families upstream -> downstream (click a hop)",
                ),
                ui.panel(ui.table(levels), title="Totals by level"),
                height=44,
            ),
            ui.row(
                ui.panel(executions_panel, title=executions_title),
                ui.panel(ui.table(market_data_latest), title="Market data (latest)"),
                ui.panel(ui.table(fleet), title="Fleet"),
                ui.panel(ui.table(exposure_by_leaf), title="Per-hub totals by leaf"),
                height=36,
            ),
        )

    dashboard = _safe(lambda: ui.dashboard(remote_uri_component()))
    if dashboard is None:
        print(
            "[remote-uri] deephaven.ui rejected the dashboard layout; "
            "every table is still exported as a global"
        )
    return dashboard
