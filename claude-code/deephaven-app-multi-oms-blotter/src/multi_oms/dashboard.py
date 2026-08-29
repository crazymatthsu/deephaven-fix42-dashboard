"""``deephaven.ui`` dashboard -- doc 09 section 6.

Four linked panels driven purely by UI state::

    +---------------------------------------------------------------------------+
    | Account v  Symbol v  Side v  [x]OMS-A [x]OMS-B-parent [x]...  [ ]breaks only|
    | search: [ClOrdID/OrderID/ext id]      Prev   page 1/12   Next    (200/pg)  |
    +----------------------------------------------+----------------------------+
    | Blotter (flat, paged)                        | Chain panel (RootKey == sel)|
    +----------------------------------------------+----------------------------+
    | Executions of selected hop                   | Order events of selected    |
    +----------------------------------------------+----------------------------+

Pressing any row stores ``(GlobalKey, RootKey)``: the chain panel filters on
``RootKey`` and the bottom panels on ``GlobalKey``, so selecting an upstream *or* a
downstream hop lights the whole family (issue #10's "works both ways"), while the
history panels stay strictly per-hop (quantities are never summed across hubs).

Version tolerance, exactly like ``dh_app.dashboard``: ``deephaven.ui`` is imported
lazily and the builder returns ``None`` when the plugin is missing (``app.py`` then
degrades to plain table panels); ``on_row_press`` accepts every known payload shape;
pickers, checkboxes, live counts and server-side coloring are optional garnish built
inside :func:`_safe`.

``deephaven`` is deliberately not imported at module scope: the filter-building and
paging helpers below are pure and unit-tested on a bare host python.
"""

from __future__ import annotations

from typing import Any, Callable, List, Mapping, Optional, Sequence, Tuple

from multi_oms import config
from multi_oms.config import Topology
from multi_oms.query_api import sanitize_id

__all__ = [
    "BLOTTER_COLUMNS",
    "CHAIN_COLUMNS",
    "RED_BREAK_KINDS",
    "SEARCH_COLUMNS",
    "SIDES",
    "hub_filter",
    "search_filter",
    "blotter_filters",
    "build_dashboard",
]

#: Left-to-right column order of the flat blotter (doc 09 section 6 sketch).
#: ``GlobalKey``/``RootKey``/``Depth`` trail the display columns because the row-press
#: handler reads them out of the pressed row.
BLOTTER_COLUMNS: Tuple[str, ...] = (
    "Oms",
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
    "Notional",
    "ChildCount",
    "DeltaCumQty",
    "DeltaLeavesQty",
    "DeltaNotional",
    "BreakKind",
    "OnBrokenEdge",
    "Depth",
    "GlobalKey",
    "RootKey",
)

#: The chain panel shows the same columns; it differs only in filter and sort.
CHAIN_COLUMNS: Tuple[str, ...] = BLOTTER_COLUMNS

#: Columns the free-text search box matches with ``contains``.
SEARCH_COLUMNS: Tuple[str, ...] = ("ClOrdID", "OrderID", "ExtOrdID", "GlobalKey")

#: ``BreakKind`` values painted red; ``UNROUTED`` is amber, ``NONE`` is quiet.
RED_BREAK_KINDS: Tuple[str, ...] = ("QTY_BREAK", "NOTIONAL_BREAK", "DANGLING", "NO_LINK")

#: Side picker options (doc 01 section 2's tag-54 enum names).
SIDES: Tuple[str, ...] = ("BUY", "SELL", "SELL_SHORT")

#: Keys a row payload may be delivered under when the callback uses keyword args.
_ROW_KWARGS = ("row", "row_data", "data", "item")
#: Keys a cell payload may carry the display/typed value under.
_CELL_KEYS = ("value", "text", "raw_value")


# --------------------------------------------------------------------------------------
# Defensive on_row_press extraction (copied from dh_app.dashboard -- doc 05 section 8)
# --------------------------------------------------------------------------------------


def _extract_row(args: Sequence[Any], kwargs: Mapping[str, Any]) -> Optional[Any]:
    """Find the row payload in an ``on_row_press`` invocation.

    Handles the documented ``fn(row_data)`` form, the older ``fn(index, row_data)``
    form, keyword delivery (``fn(row=...)``) and event-object payloads.
    """
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


def _selection_handler(setter: Callable[[Tuple[str, str]], None]) -> Callable[..., None]:
    """Wrap ``setter`` in a signature-agnostic ``on_row_press`` callback.

    Reads **both** ``GlobalKey`` and ``RootKey`` from the pressed row: the hop drives
    the history panels and the chain drives the family panel, and taking them from
    the same payload avoids a second round trip to resolve one from the other.
    """

    def handler(*args: Any, **kwargs: Any) -> None:
        try:
            row = _extract_row(args, kwargs)
            selection = (_cell_value(row, "GlobalKey"), _cell_value(row, "RootKey"))
        except Exception:  # noqa: BLE001 - a UI callback must never raise
            return
        if selection[0] or selection[1]:
            setter(selection)

    return handler


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


# --------------------------------------------------------------------------------------
# Pure filter construction (unit-tested without deephaven)
# --------------------------------------------------------------------------------------


def hub_filter(hub_names: Sequence[str], hubs_on: Mapping[str, bool]) -> Optional[str]:
    """Build the source-system filter clause.

    Args:
        hub_names: Every configured hub name.
        hubs_on: ``{hub name: checked}``; a missing hub counts as checked.

    Returns:
        ``""`` when every hub is selected (no filter needed), ``None`` when *none*
        is selected (the caller must render an empty table -- an all-false ``where``
        would be a needlessly compiled filter), or the ``||``-joined clause.
    """
    selected = [name for name in hub_names if hubs_on.get(name, True)]
    if not selected:
        return None
    if len(selected) == len(hub_names):
        return ""
    return "(" + " || ".join(f"Oms == `{sanitize_id(name)}`" for name in selected) + ")"


def search_filter(text: Any) -> str:
    """Build the free-text clause over :data:`SEARCH_COLUMNS` (``""`` when blank)."""
    needle = sanitize_id(text)
    if not needle:
        return ""
    return " || ".join(f"{column}.contains(`{needle}`)" for column in SEARCH_COLUMNS)


def blotter_filters(
    hub_names: Sequence[str],
    account: Any = None,
    symbol: Any = None,
    side: Any = None,
    hubs_on: Optional[Mapping[str, bool]] = None,
    breaks_only: bool = False,
    search: Any = None,
) -> Optional[List[str]]:
    """Build the blotter's ``where`` clauses, all optional and combinable.

    Args:
        hub_names: Every configured hub name.
        account: Selected account, or ``None``/``""``.
        symbol: Selected symbol, or ``None``/``""``.
        side: Selected side, or ``None``/``""``.
        hubs_on: ``{hub name: checked}``; ``None`` means every hub is on.
        breaks_only: Restrict to rows that are broken or sit on a broken edge --
            i.e. red rows, amber rows, *and* the healthy child of a broken parent,
            so both ends of a discrepancy stay visible.
        search: Free text matched against :data:`SEARCH_COLUMNS`.

    Returns:
        The clause list (``[]`` = unfiltered), or ``None`` when no source system is
        selected and the blotter must show nothing at all.
    """
    filters: List[str] = []
    account_value = sanitize_id(account)
    symbol_value = sanitize_id(symbol)
    side_value = sanitize_id(side)
    if account_value:
        filters.append(f"Account == `{account_value}`")
    if symbol_value:
        filters.append(f"Symbol == `{symbol_value}`")
    if side_value:
        filters.append(f"Side == `{side_value}`")

    hubs = hub_filter(hub_names, hubs_on if hubs_on is not None else {})
    if hubs is None:
        return None
    if hubs:
        filters.append(hubs)

    if breaks_only:
        filters.append("BreakKind != `NONE` || OnBrokenEdge")

    text = search_filter(search)
    if text:
        filters.append(text)
    return filters


# --------------------------------------------------------------------------------------
# Table shaping
# --------------------------------------------------------------------------------------


def _view(table: Any, columns: Sequence[str]) -> Any:
    """Apply a display column order, falling back to the full table."""
    viewed = _safe(lambda: table.view(list(columns)))
    return table if viewed is None else viewed


def _colorize(table: Any) -> Any:
    """Paint break rows server-side (stable Table API, doc 09 section 6).

    Three attempts, most expressive first: named colors + a ``BreakKind`` cell
    format, then plain hex row colors, then no coloring at all. Coloring is
    presentation -- it must never cost the panel.
    """
    red_rows = " || ".join(f"BreakKind == `{kind}`" for kind in RED_BREAK_KINDS)
    amber_rows = "BreakKind == `UNROUTED`"

    def named() -> Any:
        return (
            table.format_row_where(red_rows, "RED")
            .format_row_where(amber_rows, "ORANGE")
            .format_columns(
                [f"BreakKind = ({red_rows}) ? RED : ({amber_rows}) ? ORANGE : NO_FORMATTING"]
            )
        )

    def named_rows_only() -> Any:
        return table.format_row_where(red_rows, "RED").format_row_where(amber_rows, "ORANGE")

    def hex_rows() -> Any:
        return table.format_row_where(red_rows, "`#B3261E`").format_row_where(
            amber_rows, "`#B26A00`"
        )

    painted = _first(named, named_rows_only, hex_rows)
    return table if painted is None else painted


def _apply_blotter_filters(table: Any, filters: Optional[Sequence[str]]) -> Any:
    """Apply :func:`blotter_filters`' output (``None`` -> an empty view)."""
    if filters is None:
        return table.head(0)
    if not filters:
        return table
    return table.where(list(filters))


def _chain_view(orders_recon: Any, root_key: str) -> Any:
    """The selected family, every hop, sorted upstream -> downstream."""
    key = sanitize_id(root_key)
    if not key:
        return orders_recon.head(0)
    return orders_recon.where(f"RootKey == `{key}`").sort(["Depth", "Oms", "OrderKey"])


def _hop_view(table: Any, global_key: str, sort_column: str = "IngestTs") -> Any:
    """One hop's history, newest first (never rolled up across hubs)."""
    key = sanitize_id(global_key)
    if not key:
        return table.head(0)
    return table.where(f"GlobalKey == `{key}`").sort_descending([sort_column])


# --------------------------------------------------------------------------------------
# The dashboard
# --------------------------------------------------------------------------------------


def build_dashboard(
    topology: Topology,
    tables: Mapping[str, Any],
    size: Optional[int] = None,
) -> Optional[Any]:
    """Build the linked multi-OMS blotter dashboard.

    Args:
        topology: The validated hub graph -- one checkbox per configured hub.
        tables: The dict returned by :func:`multi_oms.dag.build_derived`.
        size: Blotter page size; defaults to :func:`config.page_size`.

    Returns:
        A ``ui.dashboard`` element, or ``None`` when ``deephaven.ui`` is not
        installed (the caller then falls back to plain table panels; every table is
        exported as a global regardless).
    """
    try:
        import deephaven.ui as ui
    except ImportError:  # pragma: no cover - server without the ui plugin
        print("[multi-oms] deephaven.ui not available; falling back to plain table panels")
        return None

    page_size = config.page_size() if size is None else max(1, int(size))
    hub_names: Tuple[str, ...] = topology.names

    orders_recon = tables["orders_recon"]
    oms_executions = tables["oms_executions"]
    oms_events = tables["oms_events"]
    account_list = tables["account_list"]
    symbol_list = tables["symbol_list"]
    side_list = tables["side_list"]
    break_summary = tables["break_summary"]

    # ui.use_cell_data keeps the "rows X-Y of N" caption live; older plugin builds
    # lack it, and the getattr is resolved once so the hook order never changes.
    use_cell_data = getattr(ui, "use_cell_data", None)

    @ui.component
    def multi_oms_component() -> Any:
        """Filters + paged blotter + chain panel + per-hop executions/events."""
        account, set_account = ui.use_state(None)
        symbol, set_symbol = ui.use_state(None)
        side, set_side = ui.use_state(None)
        search, set_search = ui.use_state("")
        breaks_only, set_breaks_only = ui.use_state(False)
        hubs_on, set_hubs_on = ui.use_state({name: True for name in hub_names})
        page, set_page = ui.use_state(0)
        selection, set_selection = ui.use_state(("", ""))

        selected_key, selected_root = selection

        # Any filter change resets paging: page 7 of the old result set is
        # meaningless against the new one.
        def resetting(setter: Callable[[Any], None]) -> Callable[[Any], None]:
            def apply(value: Any) -> None:
                setter(value)
                set_page(0)

            return apply

        def toggle_hub(name: str) -> Callable[[Any], None]:
            def apply(checked: Any) -> None:
                set_hubs_on({**hubs_on, name: bool(checked)})
                set_page(0)

            return apply

        # One scalar, hashable key for every memo below. Deliberately not the dict
        # or the filter list themselves: `use_memo` deps are compared (and on some
        # builds hashed) element-wise, and a dict/list dep would be fragile there.
        filter_key = (
            sanitize_id(account),
            sanitize_id(symbol),
            sanitize_id(side),
            tuple(name for name in hub_names if hubs_on.get(name, True)),
            bool(breaks_only),
            sanitize_id(search),
        )
        # Pure python and cheap -- rebuilt every render rather than memoized.
        filters = blotter_filters(
            hub_names,
            account=account,
            symbol=symbol,
            side=side,
            hubs_on=hubs_on,
            breaks_only=breaks_only,
            search=search,
        )
        filtered = ui.use_memo(
            lambda: _apply_blotter_filters(orders_recon, filters).sort(
                ["RootKey", "Depth", "Oms", "OrderKey"]
            ),
            [filter_key],
        )
        counter = ui.use_memo(lambda: filtered.count_by("Count"), [filter_key])

        if use_cell_data is not None:
            total: Optional[int] = use_cell_data(counter)
        else:  # pragma: no cover - static fallback, refreshes on re-render only
            total = None

        bounds = config.page_bounds(page, page_size, total)
        window = ui.use_memo(
            lambda: _colorize(
                _view(filtered.slice(bounds["start"], bounds["end"]), BLOTTER_COLUMNS)
            ),
            [filter_key, bounds["start"], bounds["end"]],
        )
        chain = ui.use_memo(
            lambda: _colorize(_view(_chain_view(orders_recon, selected_root), CHAIN_COLUMNS)),
            [selected_root],
        )
        hop_executions = ui.use_memo(lambda: _hop_view(oms_executions, selected_key), [selected_key])
        hop_events = ui.use_memo(lambda: _hop_view(oms_events, selected_key), [selected_key])

        def clear_filters() -> None:
            set_account(None)
            set_symbol(None)
            set_side(None)
            set_search("")
            set_breaks_only(False)
            set_hubs_on({name: True for name in hub_names})
            set_page(0)

        # -- controls ------------------------------------------------------------
        pickers = [
            _safe(
                lambda: ui.picker(
                    account_list, label="Account", selected_key=account, on_change=resetting(set_account)
                )
            ),
            _safe(
                lambda: ui.picker(
                    symbol_list, label="Symbol", selected_key=symbol, on_change=resetting(set_symbol)
                )
            ),
            _first(
                lambda: ui.picker(
                    side_list, label="Side", selected_key=side, on_change=resetting(set_side)
                ),
                lambda: ui.picker(
                    *SIDES, label="Side", selected_key=side, on_change=resetting(set_side)
                ),
            ),
        ]
        hub_boxes = [
            _safe(
                lambda name=name: ui.checkbox(
                    name, is_selected=bool(hubs_on.get(name, True)), on_change=toggle_hub(name)
                )
            )
            for name in hub_names
        ]
        breaks_box = _safe(
            lambda: ui.checkbox(
                "breaks only", is_selected=breaks_only, on_change=resetting(set_breaks_only)
            )
        )
        search_box = _first(
            lambda: ui.text_field(
                label="search", value=search, on_change=resetting(set_search), width="size-3000"
            ),
            lambda: ui.text_field(label="search", value=search, on_change=resetting(set_search)),
        )

        caption = (
            f"rows {bounds['first_row']}-{bounds['last_row']} of {bounds['total']}"
            if bounds["total"] >= 0
            else f"page {bounds['page'] + 1} ({page_size}/pg)"
        )
        at_start = bounds["page"] <= 0
        at_end = bounds["total"] >= 0 and bounds["page"] >= bounds["pages"] - 1
        prev_button = _first(
            lambda: ui.button(
                "Prev", on_press=lambda: set_page(max(0, bounds["page"] - 1)), is_disabled=at_start
            ),
            lambda: ui.button("Prev", on_press=lambda: set_page(max(0, bounds["page"] - 1))),
        )
        next_button = _first(
            lambda: ui.button(
                "Next", on_press=lambda: set_page(bounds["page"] + 1), is_disabled=at_end
            ),
            lambda: ui.button("Next", on_press=lambda: set_page(bounds["page"] + 1)),
        )

        controls: List[Any] = [element for element in pickers if element is not None]
        controls.extend(element for element in hub_boxes if element is not None)
        if breaks_box is not None:
            controls.append(breaks_box)
        if search_box is not None:
            controls.append(search_box)

        paging: List[Any] = [element for element in (prev_button,) if element is not None]
        paging.append(ui.text(f"page {bounds['page'] + 1}/{bounds['pages']}"))
        if next_button is not None:
            paging.append(next_button)
        paging.append(ui.text(caption))
        paging.append(ui.text(f"selected: {selected_key or '-'}"))
        paging.append(ui.button("Clear filters", on_press=clear_filters))

        return ui.column(
            ui.row(
                ui.panel(
                    ui.flex(
                        ui.flex(*controls, direction="row", gap="size-150", wrap=True, align_items="end"),
                        ui.flex(*paging, direction="row", gap="size-150", align_items="center"),
                        direction="column",
                        gap="size-100",
                    ),
                    title="Multi-OMS Blotter -- filters",
                ),
                ui.panel(ui.table(break_summary), title="Breaks by system"),
                height=22,
            ),
            ui.row(
                ui.panel(
                    ui.table(window, on_row_press=_selection_handler(set_selection)),
                    title="Blotter (click a row)",
                ),
                ui.panel(
                    ui.table(chain, on_row_press=_selection_handler(set_selection)),
                    title="Chain of selected row",
                ),
                height=46,
            ),
            ui.row(
                ui.panel(ui.table(hop_executions), title="Executions (selected hop)"),
                ui.panel(ui.table(hop_events), title="Order events (selected hop)"),
                height=32,
            ),
        )

    return ui.dashboard(multi_oms_component())
