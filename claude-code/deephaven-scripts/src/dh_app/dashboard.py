"""``deephaven.ui`` dashboard -- doc 03 section 2.6 / doc 04 section 6.

Three linked panels driven purely by UI state: pressing a row of the master orders
grid sets the selected ``OrderKey``, which re-filters the executions and order
history panels.  The DAG never changes shape at runtime.

Version tolerance: ``deephaven.ui`` evolves quickly, so this module

* imports ``deephaven.ui`` lazily and returns ``None`` when the plugin is missing
  (``app.py`` then degrades to plain table panels);
* accepts every known ``on_row_press`` payload shape (see :func:`_row_press_handler`);
* treats live scalar counts and pickers as optional garnish, falling back cleanly.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping, Optional, Sequence

from deephaven.table import Table

from dh_app import schemas
from dh_app.query_api import sanitize_id

__all__ = ["build_dashboard"]

#: Keys a row payload may be delivered under when the callback uses keyword args.
_ROW_KWARGS = ("row", "row_data", "data", "item")
#: Keys a cell payload may carry the display/typed value under.
_CELL_KEYS = ("value", "text", "raw_value")


def _extract_row(args: Sequence[Any], kwargs: Mapping[str, Any]) -> Optional[Any]:
    """Find the row payload in an ``on_row_press`` invocation.

    Handles the documented ``fn(row_data)`` form, the older ``fn(index, row_data)``
    form, keyword delivery (``fn(row=...)``) and event-object payloads.

    Args:
        args: Positional arguments the callback received.
        kwargs: Keyword arguments the callback received.

    Returns:
        The row payload, or ``None`` when nothing usable was passed.
    """
    for name in _ROW_KWARGS:
        candidate = kwargs.get(name)
        if candidate is not None:
            return candidate
    positional = list(args)
    for candidate in reversed(positional):
        if isinstance(candidate, Mapping):
            return candidate
    # No mapping anywhere: fall back to the last non-scalar argument and let
    # _cell_value try duck-typed item/attribute access on it.
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
    """Read one column out of a row payload, whatever the cell shape.

    Supports ``{col: {"value": v}}`` (current ``deephaven.ui``), the flat
    ``{col: v}`` shape, and objects exposing item or attribute access.

    Args:
        row: The row payload, or ``None``.
        column: Column name to read.

    Returns:
        The cell value as a string, or ``""`` when absent.
    """
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


def _row_press_handler(column: str, setter: Callable[[str], None]) -> Callable[..., None]:
    """Wrap ``setter`` in a signature-agnostic ``on_row_press`` callback.

    Args:
        column: Column whose value should be handed to ``setter`` (``OrderKey``).
        setter: The state setter to call with the extracted value.

    Returns:
        A callable accepting any argument shape ``deephaven.ui`` may use.
    """

    def handler(*args: Any, **kwargs: Any) -> None:
        try:
            value = _cell_value(_extract_row(args, kwargs), column)
        except Exception:  # noqa: BLE001 - a UI callback must never raise
            value = ""
        if value:
            setter(value)

    return handler


def _filtered(table: Table, order_key: str, sort_column: str) -> Table:
    """Filter a history table to one order chain, newest first.

    Args:
        table: ``executions`` or ``order_events`` (append-only).
        order_key: The selected ``OrderKey`` ("" selects nothing).
        sort_column: Column to sort descending (``IngestTs``).

    Returns:
        A live filtered/sorted view, or an empty view when nothing is selected.
    """
    key = sanitize_id(order_key)
    if not key:
        return table.head(0)
    return table.where(f"OrderKey == `{key}`").sort_descending([sort_column])


def _apply_grid_filters(table: Table, account: Optional[str], symbol: Optional[str]) -> Table:
    """Apply the optional Account/Symbol picker filters to the orders grid."""
    filters = []
    account_value = sanitize_id(account)
    symbol_value = sanitize_id(symbol)
    if account_value:
        filters.append(f"Account == `{account_value}`")
    if symbol_value:
        filters.append(f"Symbol == `{symbol_value}`")
    return table.where(filters) if filters else table


def _safe(factory: Callable[[], Any]) -> Optional[Any]:
    """Build an optional UI element, returning ``None`` if this version rejects it."""
    try:
        return factory()
    except Exception:  # noqa: BLE001 - optional garnish only
        return None


def build_dashboard(tables: Mapping[str, Table]) -> Optional[Any]:
    """Build the three-panel linked dashboard.

    Args:
        tables: The dict returned by :func:`dh_app.dag.build_derived`.

    Returns:
        A ``ui.dashboard`` element, or ``None`` when ``deephaven.ui`` is not
        installed (the caller then falls back to plain table panels).
    """
    try:
        import deephaven.ui as ui
    except ImportError:  # pragma: no cover - server without the ui plugin
        print("[fix42] deephaven.ui not available; falling back to plain table panels")
        return None

    order_state_latest = tables["order_state_latest"]
    executions = tables["executions"]
    order_events = tables["order_events"]
    status_summary = tables["status_summary"]
    open_orders = tables["open_orders"]
    account_list = tables["account_list"]
    symbol_list = tables["symbol_summary"].view(["Symbol"])

    # Sensible left-to-right column order for the master grid (doc 03 section 2.6).
    orders_grid = order_state_latest.view(list(schemas.order_grid_columns()))
    total_count = order_state_latest.count_by("Count")
    open_count = open_orders.count_by("Count")

    # ui.use_cell_data keeps the headline numbers live; older plugin builds lack it.
    use_cell_data = getattr(ui, "use_cell_data", None)

    @ui.component
    def fix42_orders_dashboard() -> Any:
        """Master orders grid + executions + order history, linked by OrderKey."""
        selected, set_selected = ui.use_state("")
        account, set_account = ui.use_state(None)
        symbol, set_symbol = ui.use_state(None)

        if use_cell_data is not None:
            total_orders = use_cell_data(total_count)
            live_orders = use_cell_data(open_count)
        else:  # pragma: no cover - static fallback, refreshes on re-render only
            total_orders = order_state_latest.size
            live_orders = open_orders.size

        grid = ui.use_memo(
            lambda: _apply_grid_filters(orders_grid, account, symbol), [account, symbol]
        )
        order_executions = ui.use_memo(
            lambda: _filtered(executions, selected, "IngestTs"), [selected]
        )
        order_history = ui.use_memo(
            lambda: _filtered(order_events, selected, "IngestTs"), [selected]
        )

        def clear_filters() -> None:
            set_account(None)
            set_symbol(None)

        controls = [
            ui.text(f"Orders: {total_orders}"),
            ui.text(f"Open: {live_orders}"),
            ui.text(f"Selected: {selected or '-'}"),
        ]
        account_picker = _safe(
            lambda: ui.picker(
                account_list,
                label="Account",
                selected_key=account,
                on_change=set_account,
            )
        )
        symbol_picker = _safe(
            lambda: ui.picker(
                symbol_list,
                label="Symbol",
                selected_key=symbol,
                on_change=set_symbol,
            )
        )
        controls.extend(p for p in (account_picker, symbol_picker) if p is not None)
        controls.append(ui.button("Clear filters", on_press=clear_filters))

        return ui.column(
            ui.row(
                ui.panel(
                    ui.flex(*controls, direction="row", gap="size-150", align_items="center"),
                    title="FIX 4.2 Orders",
                ),
                ui.panel(ui.table(status_summary), title="Status Summary"),
                height=20,
            ),
            ui.row(
                ui.panel(
                    ui.table(grid, on_row_press=_row_press_handler("OrderKey", set_selected)),
                    title="Orders (click a row)",
                ),
                height=45,
            ),
            ui.row(
                ui.panel(ui.table(order_executions), title="Executions"),
                ui.panel(ui.table(order_history), title="Order History"),
                height=35,
            ),
        )

    return ui.dashboard(fix42_orders_dashboard())
