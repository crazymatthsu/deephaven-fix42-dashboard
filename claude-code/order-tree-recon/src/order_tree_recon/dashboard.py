"""``deephaven.ui`` dashboard -- doc 14 section 6.

::

    +--------------------------------------------------------------------------------+
    | Moniker [ac_____v]  Symbol [____v]  Side [any v]  [x] unlinked  [ ] breaks only |
    | any order id [__________]   (type a prefix; press a bucket row to pin it)       |
    +-----------------------------------------+--------------------------------------+
    | Exposure by moniker / symbol / side     | Level ladder (same filter)           |
    |  client cum/leaves vs street cum/leaves |  per level: orders, cum, leaves,     |
    |  held, over-routed, unbooked, breaks    |  held / over-routed / unbooked       |
    +-----------------------------------------+--------------------------------------+
    | Order tree (native, expand/collapse)    | Breaks in view                       |
    |  CLIENT > ALGO_PARENT > ALGO_CHILD >    |  red nodes + the children of a       |
    |  OR_PARENT > OR_CHILD, gaps per edge    |  broken edge                         |
    +-----------------------------------------+--------------------------------------+

Every panel is the same filter applied to a different table, so typing a moniker narrows
the buckets, the ladder, the tree and the breaks together. There is no hand-rolled
paging: the grids are viewport-virtualized, and the tree only ships the rows of nodes
the user has expanded -- a tree over the whole book costs the browser one row per root.

Version tolerance follows ``multi_oms.dashboard``: ``deephaven.ui`` is imported lazily,
optional garnish is built inside :func:`_safe`, and ``on_row_press`` accepts every known
payload shape.
"""

from __future__ import annotations

from typing import Any, Callable, List, Mapping, Optional, Sequence, Tuple

from order_tree_recon import config
from order_tree_recon.query_api import bucket_filters, node_filters, sanitize_id

__all__ = ["SIDES", "EXPOSURE_COLUMNS", "LADDER_COLUMNS", "BREAK_COLUMNS", "row_bucket", "build_dashboard"]

SIDES: Tuple[str, ...] = ("BUY", "SELL", "SELL_SHORT")

EXPOSURE_COLUMNS: Tuple[str, ...] = (
    "RootMoniker",
    "RootSymbol",
    "RootSide",
    "Breaks",
    "ClientCumQty",
    "StreetCumQty",
    "NetUnbookedQty",
    "ClientLeavesQty",
    "StreetLeavesQty",
    "HeldQty",
    "OverRoutedQty",
    "StreetOpenVsClient",
    "Trees",
    "UnlinkedTrees",
    "Orders",
)
LADDER_COLUMNS: Tuple[str, ...] = (
    "RootMoniker",
    "RootSymbol",
    "RootSide",
    "Level",
    "Orders",
    "OrderQty",
    "CumQty",
    "LeavesQty",
    "CumVsClient",
    "LeavesVsClient",
    "HeldQty",
    "OverRoutedQty",
    "UnbookedQty",
    "PhantomQty",
    "Breaks",
)
BREAK_COLUMNS: Tuple[str, ...] = (
    "BreakKind",
    "Level",
    "OrderId",
    "Status",
    "OrderQty",
    "CumQty",
    "LeavesQty",
    "FillGap",
    "RouteGap",
    "ChildCumQty",
    "ChildLeavesQty",
    "LinkId",
    "RootMoniker",
    "RootSymbol",
    "RootSide",
    "RootOrderId",
    "OnBrokenEdge",
)

_ROW_KWARGS = ("row", "row_data", "data", "item")
_CELL_KEYS = ("value", "text", "raw_value")


# --------------------------------------------------------------------------------------
# Defensive helpers (same shapes as multi_oms.dashboard)
# --------------------------------------------------------------------------------------


def _safe(factory: Callable[[], Any]) -> Optional[Any]:
    try:
        return factory()
    except Exception:  # noqa: BLE001 - optional garnish only
        return None


def _first(*factories: Callable[[], Any]) -> Optional[Any]:
    for factory in factories:
        built = _safe(factory)
        if built is not None:
            return built
    return None


def _extract_row(args: Sequence[Any], kwargs: Mapping[str, Any]) -> Optional[Any]:
    for name in _ROW_KWARGS:
        if kwargs.get(name) is not None:
            return kwargs[name]
    for candidate in reversed(list(args)):
        if isinstance(candidate, Mapping):
            return candidate
    return None


def _cell(row: Any, column: str) -> str:
    cell = row.get(column) if isinstance(row, Mapping) else None
    if cell is None:
        return ""
    if isinstance(cell, Mapping):
        for key in _CELL_KEYS:
            if cell.get(key) is not None:
                return str(cell[key])
        return ""
    return str(cell)


def row_bucket(args: Sequence[Any], kwargs: Mapping[str, Any]) -> Optional[Tuple[str, str, str]]:
    """``(RootMoniker, RootSymbol, RootSide)`` from an ``on_row_press`` payload, or ``None``."""
    row = _extract_row(args, kwargs)
    if row is None:
        return None
    bucket = (_cell(row, "RootMoniker"), _cell(row, "RootSymbol"), _cell(row, "RootSide"))
    return bucket if any(bucket) else None


def _view(table: Any, columns: Sequence[str]) -> Any:
    viewed = _safe(lambda: table.view(list(columns)))
    return table if viewed is None else viewed


def _where(table: Any, clauses: List[str]) -> Any:
    return table.where(clauses) if clauses else table


#: Share quantities are integral: show them that way.
_QTY_FORMAT_COLUMNS: Tuple[str, ...] = (
    "OrderQty", "CumQty", "LeavesQty", "ChildCumQty", "ChildLeavesQty", "FillGap", "RouteGap",
    "ClientCumQty", "StreetCumQty", "NetUnbookedQty", "ClientLeavesQty", "StreetLeavesQty", "HeldQty",
    "OverRoutedQty", "StreetOpenVsClient", "CumVsClient", "LeavesVsClient", "UnbookedQty", "PhantomQty",
)


def _shares(table: Any, columns: Sequence[str]) -> Any:
    """``#,##0`` on every share-quantity column present (presentation only)."""
    present = [c for c in _QTY_FORMAT_COLUMNS if c in columns]
    formatted = _safe(lambda: table.format_columns([f"{c} = Decimal(`#,##0`)" for c in present]))
    return table if formatted is None else formatted


def _paint_nodes(table: Any) -> Any:
    """Red for a red ``BreakKind``, amber for ``HELD``; the child side of a broken edge
    gets its ``BreakKind`` cell tinted. Presentation only -- never costs the panel."""
    red = " || ".join(f"BreakKind == `{k}`" for k in config.RED_BREAK_KINDS)
    amber = " || ".join(f"BreakKind == `{k}`" for k in config.AMBER_BREAK_KINDS)

    def named() -> Any:
        return table.format_columns(
            [
                f"BreakKind = ({red}) ? RED : ({amber}) ? ORANGE : OnBrokenEdge ? PINK : NO_FORMATTING",
                f"FillGap = abs(FillGap) > 0 ? RED : NO_FORMATTING",
                f"RouteGap = RouteGap < 0 ? RED : RouteGap > 0 ? ORANGE : NO_FORMATTING",
            ]
        )

    painted = _first(named, lambda: table.format_columns([f"BreakKind = ({red}) ? RED : NO_FORMATTING"]))
    return table if painted is None else painted


def _paint_buckets(table: Any, columns: Sequence[str]) -> Any:
    """Tint the exposure / ladder cells that are a problem (only columns present)."""
    rules = {
        "Breaks": "Breaks > 0 ? RED : NO_FORMATTING",
        "NetUnbookedQty": "NetUnbookedQty != 0 ? RED : NO_FORMATTING",
        "OverRoutedQty": "OverRoutedQty > 0 ? RED : NO_FORMATTING",
        "UnbookedQty": "UnbookedQty > 0 ? RED : NO_FORMATTING",
        "PhantomQty": "PhantomQty > 0 ? RED : NO_FORMATTING",
        "StreetOpenVsClient": "StreetOpenVsClient > 0 ? RED : NO_FORMATTING",
        "HeldQty": "HeldQty > 0 ? ORANGE : NO_FORMATTING",
        "UnlinkedTrees": "UnlinkedTrees > 0 ? RED : NO_FORMATTING",
    }
    formulas = [f"{column} = {rule}" for column, rule in rules.items() if column in columns]
    painted = _safe(lambda: table.format_columns(formulas))
    return table if painted is None else painted


# --------------------------------------------------------------------------------------
# The dashboard
# --------------------------------------------------------------------------------------


def build_dashboard(tables: Mapping[str, Any]) -> Optional[Any]:
    """Build ``order_tree_dashboard`` over :func:`order_tree_recon.dag.build_order_tree`'s tables.

    Returns ``None`` when ``deephaven.ui`` is not installed (every table is still a
    global, so the IDE's Panels menu works without it).
    """
    try:
        import deephaven.ui as ui
    except ImportError:  # pragma: no cover - server without the ui plugin
        print("[order-tree] deephaven.ui not available; use the otr_* tables directly", flush=True)
        return None

    from order_tree_recon.dag import TREE_COLUMNS

    recon = tables["otr_recon"]
    id_index = tables["otr_id_index"]
    exposure = tables["otr_exposure"]
    ladder = tables["otr_level_ladder"]
    breaks = tables["otr_breaks"]
    moniker_list = tables["otr_moniker_list"]
    symbol_list = tables["otr_symbol_list"]

    @ui.component
    def order_tree_component() -> Any:
        moniker, set_moniker = ui.use_state("")
        symbol, set_symbol = ui.use_state("")
        side, set_side = ui.use_state(None)
        include_unlinked, set_include_unlinked = ui.use_state(True)
        breaks_only, set_breaks_only = ui.use_state(False)
        search, set_search = ui.use_state("")
        # The text inputs are *uncontrolled* (default value + key): a controlled
        # input_value round-trips through the server, and a fast second keystroke is
        # overwritten by the stale render. Bumping `epoch` remounts them when a row
        # press or Clear has to set their text.
        epoch, set_epoch = ui.use_state(0)

        key = (
            sanitize_id(moniker).upper(),
            sanitize_id(symbol).upper(),
            sanitize_id(side),
            bool(include_unlinked),
            bool(breaks_only),
            sanitize_id(search),
        )
        args = dict(moniker=moniker, symbol=symbol, side=side,
                    include_unlinked=include_unlinked, prefix=True)

        def nodes_in_view() -> Any:
            subset = _where(recon, node_filters(breaks_only=breaks_only, **args))
            needle = sanitize_id(search)
            if needle:
                hits = recon.where_in(id_index.where(f"Id == `{needle}`"), "NodeKey")
                subset = subset.where_in(hits.select_distinct(["RootKey"]), "RootKey")
            return subset

        subset = ui.use_memo(nodes_in_view, [key])
        tree = ui.use_memo(
            lambda: _first(
                lambda: _shares(_paint_nodes(subset.view(list(TREE_COLUMNS))), TREE_COLUMNS).tree(
                    "NodeKey", "ParentKey", promote_orphans=True
                ),
                lambda: subset.view(list(TREE_COLUMNS)).tree("NodeKey", "ParentKey", promote_orphans=True),
            ),
            [key],
        )

        def buckets_in_view(table: Any, clauses: List[str]) -> Any:
            narrowed = _where(table, clauses)
            if sanitize_id(search):  # an id search pins the panels to that tree's bucket(s)
                by = ["RootMoniker", "RootSymbol", "RootSide"]
                narrowed = narrowed.where_in(subset.select_distinct(by), by)
            return narrowed

        exposure_view = ui.use_memo(
            lambda: _shares(
                _paint_buckets(
                    _view(
                        buckets_in_view(exposure, bucket_filters(breaks_only=breaks_only, **args)),
                        EXPOSURE_COLUMNS,
                    ),
                    EXPOSURE_COLUMNS,
                ),
                EXPOSURE_COLUMNS,
            ),
            [key],
        )
        ladder_view = ui.use_memo(
            lambda: _shares(
                _paint_buckets(_view(buckets_in_view(ladder, bucket_filters(**args)), LADDER_COLUMNS), LADDER_COLUMNS),
                LADDER_COLUMNS,
            ),
            [key],
        )
        breaks_view = ui.use_memo(
            lambda: _shares(
                _paint_nodes(_view(breaks.where_in(subset.select_distinct(["RootKey"]), "RootKey"), BREAK_COLUMNS)),
                BREAK_COLUMNS,
            ),
            [key],
        )

        def pin_bucket(*a: Any, **kw: Any) -> None:
            try:
                bucket = row_bucket(a, kw)
            except Exception:  # noqa: BLE001 - a UI callback must never raise
                return
            if bucket:
                set_moniker(bucket[0])
                set_symbol(bucket[1])
                set_side(bucket[2] or None)
                set_epoch(epoch + 1)

        def clear() -> None:
            set_moniker("")
            set_symbol("")
            set_side(None)
            set_search("")
            set_breaks_only(False)
            set_include_unlinked(True)
            set_epoch(epoch + 1)

        controls = [
            _first(
                lambda: ui.combo_box(
                    moniker_list, label="Moniker", allows_custom_value=True, menu_trigger="focus",
                    default_input_value=moniker, on_input_change=set_moniker, width="size-2400",
                    key=f"moniker-{epoch}",
                ),
                lambda: ui.text_field(label="Moniker", default_value=moniker, on_change=set_moniker,
                                      key=f"moniker-{epoch}"),
            ),
            _first(
                lambda: ui.combo_box(
                    symbol_list, label="Symbol", allows_custom_value=True, menu_trigger="focus",
                    default_input_value=symbol, on_input_change=set_symbol, width="size-2000",
                    key=f"symbol-{epoch}",
                ),
                lambda: ui.text_field(label="Symbol", default_value=symbol, on_change=set_symbol,
                                      key=f"symbol-{epoch}"),
            ),
            _first(
                lambda: ui.picker(*SIDES, label="Side", selected_key=side, on_change=set_side, width="size-1600"),
                lambda: ui.picker(*SIDES, label="Side", selected_key=side, on_change=set_side),
            ),
            _first(
                lambda: ui.text_field(label="Any order id (any system)", default_value=search,
                                      on_change=set_search, width="size-3000", key=f"search-{epoch}"),
                lambda: ui.text_field(label="Any order id", default_value=search, on_change=set_search),
            ),
            _safe(lambda: ui.checkbox("include unlinked", is_selected=include_unlinked,
                                      on_change=set_include_unlinked)),
            _safe(lambda: ui.checkbox("breaks only", is_selected=breaks_only, on_change=set_breaks_only)),
            _safe(lambda: ui.button("Clear", on_press=clear)),
        ]
        hint = ui.text(
            "Type a prefix (moniker / symbol) or press an exposure row to pin its bucket. "
            "Gaps are per edge: FillGap = own CumQty - children's, RouteGap = own LeavesQty - children's."
        )

        return ui.column(
            ui.row(
                ui.panel(
                    ui.flex(
                        ui.flex(*[c for c in controls if c is not None], direction="row", gap="size-150",
                                wrap=True, align_items="end"),
                        hint,
                        direction="column",
                        gap="size-100",
                    ),
                    title="Find orders",
                ),
                height=15,
            ),
            ui.row(
                ui.panel(
                    _first(
                        lambda: ui.table(exposure_view, on_row_press=pin_bucket,
                                         always_fetch_columns=["RootMoniker", "RootSymbol", "RootSide"]),
                        lambda: ui.table(exposure_view, on_row_press=pin_bucket),
                    ),
                    title="Exposure by moniker / symbol / side (press a row)",
                ),
                ui.panel(ui.table(ladder_view), title="Level ladder"),
                height=34,
            ),
            ui.row(
                ui.panel(ui.table(tree) if tree is not None else ui.text("tree unavailable"), title="Order tree"),
                ui.panel(ui.table(breaks_view), title="Breaks in view"),
                height=50,
            ),
        )

    return ui.dashboard(order_tree_component())
