"""The ``deephaven.ui`` dashboard (doc 13 §5).

::

    +--------------------------------------------------------------------------------+
    | Baskets  [New basket] [Route basket v] [Cancel basket] [Add orders]  sim  trader |
    |   BasketId Name Strategy Status Orders Live Filled% Qty Filled Notional ...      |
    +------------------------------------------------+-------------------------------+
    | Orders -- <basket>  (right-click a row)         | Executions | Events | Quotes  |
    |   OrderId Symbol Side Qty Filled Leaves Type Px | Order tree | Fill progress    |
    +------------------------------------------------+-------------------------------+

State is small: the selected basket id, the selected order row, and ``pending`` -- the
action a dialog is open for (``("modify", order_id)``, ``("split", order_id)``,
``("cancel", order_id)``, ``("new_basket", None)``, ``("add_orders", basket_id)``,
``("cancel_basket", basket_id)``). Everything else ticks on its own: the panels are
``where`` filters over the bridge's live tables, memoised on the selection.

The context menu's callback receives only the **cell** it was opened on (analysis §2),
so the menu resolver looks the order up from the tracked selection (``on_selection_change``)
and falls back to the cell value when the click landed on ``OrderId``.

Version tolerance, exactly like the other apps: ``deephaven.ui`` is imported lazily and
:func:`build_dashboard` returns ``None`` when it is missing; every optional garnish is
built inside :func:`_safe` / :func:`_first`, so a widget this plugin version rejects
degrades to a plainer one instead of costing the dashboard. The pure helpers at the top
are unit-tested on the host.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from basket_oms_demo.core import (
    ORD_TYPES,
    SIDES,
    STRATEGIES,
    TIFS,
    OmsError,
    Order,
    OrderLine,
    allowed_actions,
    parse_basket_lines,
    split_quantities,
)

__all__ = [
    "cell_value",
    "row_to_dict",
    "row_key",
    "actions_for",
    "coerce_int",
    "coerce_float",
    "parse_qty_list",
    "split_preview",
    "build_dashboard",
]

_CELL_KEYS = ("value", "text", "raw_value")


# --------------------------------------------------------------------------------------
# Pure helpers (host-tested)
# --------------------------------------------------------------------------------------


def cell_value(cell: Any) -> Any:
    """The scalar inside a ``deephaven.ui`` row payload cell (``{"value", "text", "type"}``) or the bare value."""
    if isinstance(cell, Mapping):
        for key in _CELL_KEYS:
            if key in cell and cell[key] is not None:
                return cell[key]
        return None
    return cell


def row_to_dict(row: Any) -> Dict[str, Any]:
    """A row payload (column -> cell) as a plain ``column -> value`` dict."""
    if row is None:
        return {}
    if isinstance(row, Mapping):
        return {str(column): cell_value(cell) for column, cell in row.items()}
    result: Dict[str, Any] = {}
    for name in dir(row):
        if not name.startswith("_"):
            try:
                result[name] = cell_value(getattr(row, name))
            except Exception:  # noqa: BLE001
                continue
    return result


def row_key(row: Any, column: str) -> Optional[str]:
    """``str`` of one column of a row payload, or ``None``."""
    value = row_to_dict(row).get(column)
    if value is None or value == "":
        return None
    return str(value)


def actions_for(status: str, qty: int = 2) -> List[str]:
    """The allowed context-menu actions for an order in ``status`` (menu order)."""
    stub = Order("?", "?", "?", "BUY", int(qty), "MKT", None, "DAY", "?", None, None, status=status)  # type: ignore[arg-type]
    allowed = allowed_actions(stub)
    return [name for name in ("modify", "route", "split", "cancel") if name in allowed]


def coerce_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None or value == "":
        return default
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return default


def coerce_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_qty_list(text: str) -> List[int]:
    """``"600, 400"`` / ``"600 400"`` -> ``[600, 400]``; raises :class:`OmsError`."""
    tokens = [tok for tok in str(text or "").replace(",", " ").replace(";", " ").split() if tok]
    sizes: List[int] = []
    for tok in tokens:
        try:
            sizes.append(int(tok))
        except ValueError as exc:
            raise OmsError(f"{tok!r} is not a whole number") from exc
    return sizes


def split_preview(qty: int, slices: Optional[int], qtys_text: str = "") -> Tuple[List[int], Optional[str]]:
    """The child sizes a Split dialog would create, or an error message."""
    try:
        if qtys_text and qtys_text.strip():
            sizes = parse_qty_list(qtys_text)
            if len(sizes) < 2:
                return [], "give at least two quantities"
            if any(s <= 0 for s in sizes):
                return [], "every slice must be > 0"
            if sum(sizes) != qty:
                return [], f"slices sum to {sum(sizes):,}, the order is {qty:,}"
            return sizes, None
        if slices is None:
            return [], "give a slice count or a list of quantities"
        return split_quantities(int(qty), int(slices)), None
    except OmsError as exc:
        return [], str(exc)


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


def _quote(value: str) -> str:
    return str(value).replace("`", "")


# --------------------------------------------------------------------------------------
# The dashboard
# --------------------------------------------------------------------------------------


def build_dashboard(runtime: Any) -> Optional[Any]:
    """Build ``basket_oms_dashboard`` for a wired :class:`~basket_oms_demo.app.Runtime`.

    Returns:
        A ``ui.dashboard`` element, or ``None`` without ``deephaven.ui``.
    """
    try:
        from deephaven import ui
    except Exception:  # noqa: BLE001 - plugin missing: tables + API still work
        return None
    try:
        import deephaven.plot.express as dx
    except Exception:  # noqa: BLE001
        dx = None

    core = runtime.core
    cfg = runtime.cfg
    bridge = runtime.bridge
    venues: List[str] = list(core.venues)
    baskets_view = bridge.baskets_view
    orders_view = bridge.orders_view
    executions = bridge.executions
    events = bridge.events
    quotes = bridge.quotes
    orders_tree = bridge.orders_tree

    def initial_basket() -> Optional[str]:
        ids = sorted(core.baskets)
        return ids[0] if ids else None

    # -- formatting ---------------------------------------------------------------------

    def order_formats() -> List[Any]:
        rules: List[Any] = []
        for factory in (
            lambda: ui.TableFormat(cols="Side", if_="Side == `BUY`", color="positive"),
            lambda: ui.TableFormat(cols="Side", if_="Side != `BUY`", color="negative"),
            lambda: ui.TableFormat(cols="Status", if_="Status == `FILLED`", color="positive"),
            lambda: ui.TableFormat(cols="Status", if_="Status == `CANCELLED` || Status == `REJECTED`", color="negative"),
            lambda: ui.TableFormat(cols="Status", if_="Status == `ROUTED` || Status == `WORKING` || Status == `PARTIAL`", color="notice"),
            lambda: ui.TableFormat(cols="Status", if_="Status == `NEW`", color="info"),
            lambda: ui.TableFormat(cols=["Qty", "Filled", "Leaves", "LastQty"], value="#,##0"),
            lambda: ui.TableFormat(cols=["LimitPx", "AvgPx", "LastPx", "Bid", "Ask", "Last", "RefPx"], value="0.00"),
            lambda: ui.TableFormat(cols=["Notional", "FilledNotional"], value="#,##0"),
            lambda: ui.TableFormat(cols="PctFilled", value="0.0"),
            lambda: ui.TableFormat(cols="PctFilled", mode=ui.TableDatabar(value_column="PctFilled", min=0, max=100, color="positive", value_placement="overlap")),
            lambda: ui.TableFormat(cols=["Created", "Updated"], value="HH:mm:ss"),
        ):
            rule = _safe(factory)
            if rule is not None:
                rules.append(rule)
        return rules

    def time_formats() -> List[Any]:
        rule = _safe(lambda: ui.TableFormat(cols="Time", value="HH:mm:ss.SSS"))
        return [rule] if rule is not None else []

    def basket_formats() -> List[Any]:
        rules: List[Any] = []
        for factory in (
            lambda: ui.TableFormat(cols="Status", if_="Status == `DONE`", color="positive"),
            lambda: ui.TableFormat(cols="Status", if_="Status == `CANCELLED`", color="negative"),
            lambda: ui.TableFormat(cols="Status", if_="Status == `WORKING`", color="notice"),
            lambda: ui.TableFormat(cols="Status", if_="Status == `STAGED`", color="info"),
            lambda: ui.TableFormat(cols=["Orders", "Live", "Done", "Qty", "Filled", "Leaves"], value="#,##0"),
            lambda: ui.TableFormat(cols=["Notional", "FilledNotional"], value="#,##0"),
            lambda: ui.TableFormat(cols="PctFilled", value="0.0"),
            lambda: ui.TableFormat(cols="PctFilled", mode=ui.TableDatabar(value_column="PctFilled", min=0, max=100, color="positive", value_placement="overlap")),
            lambda: ui.TableFormat(cols=["Created", "Updated"], value="HH:mm:ss"),
        ):
            rule = _safe(factory)
            if rule is not None:
                rules.append(rule)
        return rules

    ORDER_HIDDEN = ["ParentId", "Trader", "Created", "RefPx", "FilledNotional", "LastQty"]
    ORDER_NAMES = {"OrdType": "Type", "LimitPx": "Limit", "PctFilled": "Filled %", "AvgPx": "Avg Px", "LastPx": "Last Px", "OrderId": "Order", "BasketId": "Basket"}
    BASKET_HIDDEN = ["Created", "FilledNotional", "Note"]
    BASKET_NAMES = {"PctFilled": "Filled %", "BasketId": "Basket"}

    def toast(message: str, variant: str = "positive", timeout: int = 4000) -> None:
        _safe(lambda: ui.toast(message, variant=variant, timeout=timeout))

    def run(action: Callable[[], Any], success: Callable[[Any], str]) -> bool:
        """Run a core mutation from a callback; toast the outcome; True on success."""
        try:
            result = action()
        except OmsError as exc:
            toast(str(exc), variant="negative", timeout=7000)
            return False
        except Exception as exc:  # noqa: BLE001 - never let a callback kill the render
            toast(f"{type(exc).__name__}: {exc}", variant="negative", timeout=7000)
            return False
        toast(success(result))
        return True

    # -- dialogs ------------------------------------------------------------------------

    def modal(dialog: Any, on_close: Callable[[], None]) -> Any:
        """A modal dialog that is open right now (opened by a menu action, not a button)."""

        def on_open_change(is_open: bool) -> None:
            if not is_open:
                on_close()

        return _first(
            lambda: ui.dialog_trigger(
                ui.action_button("", is_hidden=True),
                dialog,
                type="modal",
                is_open=True,
                on_open_change=on_open_change,
            ),
            lambda: ui.dialog_trigger(ui.action_button("", is_hidden=True), dialog, is_open=True, on_open_change=on_open_change),
        )

    def field_error(message: str) -> Any:
        return ui.text(message, color="negative") if message else ui.text("")

    @ui.component
    def modify_dialog(order_id: str, trader: str, on_done: Callable[[], None]):
        """Modify = a ``ui.form``: the submit event carries the field values, so a Save
        (or Enter) right after typing cannot race the debounced ``on_change`` round trip
        (found in the browser verification: number_field commits on blur, text_field
        changes are debounced ~250 ms on the client)."""
        order = core.order(order_id)
        error, set_error = ui.use_state("")

        def save(data: Mapping[str, Any]) -> None:
            new_qty = coerce_int(data.get("qty"))
            if new_qty is None:
                set_error("quantity must be a whole number")
                return
            new_px = None
            if order.ord_type == "LMT":
                new_px = coerce_float(data.get("limit_px"))
                if new_px is None:
                    set_error("limit price must be a number")
                    return
            try:
                changed = core.modify(order_id, qty=new_qty, limit_px=new_px, tif=str(data.get("tif") or order.tif), user=trader)
            except OmsError as exc:
                set_error(str(exc))
                return
            toast(f"{changed.order_id} modified: {changed.note}")
            on_done()

        fields = [
            ui.text(f"{order.side} {order.symbol} · {order.status} · filled {order.filled:,} of {order.qty:,}"),
            ui.text_field(label="Quantity", name="qty", default_value=str(order.qty), description=f"at least {order.filled:,} (already filled)" if order.filled else None, auto_focus=True),
        ]
        if order.ord_type == "LMT":
            fields.append(ui.text_field(label="Limit price", name="limit_px", default_value=f"{order.limit_px:.2f}" if order.limit_px is not None else ""))
        else:
            fields.append(ui.text("Market order (no limit price)"))
        fields.append(ui.picker(*[ui.item(t, key=t) for t in TIFS], label="Time in force", name="tif", default_selected_key=order.tif))
        form = ui.form(
            *fields,
            field_error(error),
            ui.button_group(
                ui.button("Cancel", variant="secondary", type="button", on_press=lambda _e=None: on_done()),
                ui.button("Save", variant="accent", type="submit"),
                align="end",
            ),
            on_submit=save,
        )
        return ui.dialog(ui.heading(f"Modify {order.order_id}"), ui.content(form), size="M")

    @ui.component
    def split_dialog(order_id: str, trader: str, on_done: Callable[[], None]):
        order = core.order(order_id)
        slices, set_slices = ui.use_state("3")
        qtys_text, set_qtys_text = ui.use_state("")
        error, set_error = ui.use_state("")
        sizes, preview_error = split_preview(order.qty, coerce_int(slices), qtys_text)

        def save(data: Mapping[str, Any]) -> None:
            qtys = str(data.get("qtys") or qtys_text or "")
            count = coerce_int(data.get("slices", slices))
            try:
                if qtys.strip():
                    children = core.split(order_id, qtys=parse_qty_list(qtys), user=trader)
                else:
                    if count is None:
                        raise OmsError("the slice count must be a whole number")
                    children = core.split(order_id, slices=count, user=trader)
            except OmsError as exc:
                set_error(str(exc))
                return
            toast(f"{order.order_id} split into {len(children)}: " + ", ".join(c.order_id for c in children))
            on_done()

        preview = ui.text(
            "Children: " + ", ".join(f"{s:,}" for s in sizes) if sizes else (preview_error or ""),
            color="negative" if preview_error else None,
        )
        form = ui.form(
            ui.text(f"{order.side} {order.qty:,} {order.symbol} {order.ord_type}" + (f" @ {order.limit_px:.2f}" if order.limit_px else "")),
            ui.text_field(label="Number of slices", name="slices", value=slices, on_change=set_slices, width="size-1600", auto_focus=True),
            ui.text_field(label="or explicit quantities (comma separated, must sum to the order)", name="qtys", value=qtys_text, on_change=set_qtys_text),
            preview,
            field_error(error),
            ui.button_group(
                ui.button("Cancel", variant="secondary", type="button", on_press=lambda _e=None: on_done()),
                ui.button("Split", variant="accent", type="submit"),
                align="end",
            ),
            on_submit=save,
        )
        return ui.dialog(ui.heading(f"Split {order.order_id}"), ui.content(form), size="M")

    @ui.component
    def confirm_dialog(title: str, body: str, confirm_label: str, action: Callable[[], Any], success: Callable[[Any], str], on_done: Callable[[], None]):
        def go() -> None:
            run(action, success)
            on_done()

        return ui.dialog(
            ui.heading(title),
            ui.content(ui.text(body)),
            ui.button_group(
                ui.button("Keep", variant="secondary", on_press=lambda _e=None: on_done()),
                ui.button(confirm_label, variant="negative", on_press=lambda _e=None: go()),
            ),
            size="S",
        )

    @ui.component
    def basket_ticket(basket_id: Optional[str], trader: str, on_done: Callable[[], None], inline: bool = False):
        """The New basket / Add orders ticket (analysis §3, option A; ``inline`` = option B).

        Two sibling forms (forms cannot nest): the **line builder** (submit = *Add line*,
        remounted through its ``key`` after each add so its fields reset) and the
        **basket form** (name, strategy, route-on-create, the paste box, submit =
        *Create basket*). Submitting through forms carries the typed values with the
        event; the staged list and the live parse preview are server state.
        """
        existing = core.baskets.get(basket_id) if basket_id else None
        text, set_text = ui.use_state("")
        staged, set_staged = ui.use_state(())
        error, set_error = ui.use_state("")
        builder_key, set_builder_key = ui.use_state(0)

        parsed, errors = parse_basket_lines(text)
        lines: List[OrderLine] = list(staged) + parsed

        def add_line(data: Mapping[str, Any]) -> None:
            try:
                symbol = str(data.get("symbol") or "").strip().upper()
                ord_type = str(data.get("ord_type") or "MKT").upper()
                line = OrderLine(
                    symbol,
                    str(data.get("side") or "BUY").upper(),
                    coerce_int(data.get("qty")) or 0,
                    ord_type,
                    coerce_float(data.get("limit_px")) if ord_type == "LMT" else None,
                    str(data.get("tif") or "DAY").upper(),
                )
                if not line.symbol:
                    raise OmsError("symbol is required")
                if line.qty <= 0:
                    raise OmsError("quantity must be a whole number > 0")
                if line.ord_type == "LMT" and not line.limit_px:
                    raise OmsError("a LMT line needs a limit price")
            except OmsError as exc:
                set_error(str(exc))
                return
            set_error("")
            set_staged(tuple(staged) + (line,))
            set_builder_key(builder_key + 1)

        def remove_line(index: int) -> None:
            set_staged(tuple(l for i, l in enumerate(staged) if i != index))

        def create(data: Mapping[str, Any]) -> None:
            pasted, paste_errors = parse_basket_lines(str(data.get("lines") or text or ""))
            all_lines: List[OrderLine] = list(staged) + pasted
            if paste_errors:
                set_error(f"line {paste_errors[0][0]}: {paste_errors[0][2]}")
                return
            if not all_lines:
                set_error("add at least one order line")
                return
            route_to = str(data.get("route_to") or "")
            try:
                if existing is not None:
                    added = core.add_orders(existing.basket_id, all_lines, trader=trader)
                    target_id, label = existing.basket_id, f"{len(added)} orders added to {existing.basket_id}"
                else:
                    basket = core.create_basket(str(data.get("name") or ""), all_lines, strategy=str(data.get("strategy") or "DMA"), trader=trader)
                    target_id, label = basket.basket_id, f"{basket.basket_id} {basket.name!r} created with {len(all_lines)} orders"
                if route_to:
                    routed = core.route_basket(target_id, route_to, user=trader)
                    label += f"; {len(routed)} routed to {route_to}"
            except OmsError as exc:
                set_error(str(exc))
                return
            toast(label)
            on_done()

        builder = ui.form(
            ui.flex(
                ui.text_field(label="Symbol", name="symbol", width="size-1200", auto_focus=existing is not None),
                ui.picker(*[ui.item(s, key=s) for s in SIDES], label="Side", name="side", default_selected_key="BUY", width="size-1200"),
                ui.text_field(label="Qty", name="qty", default_value="1000", width="size-1200"),
                ui.picker(*[ui.item(t, key=t) for t in ORD_TYPES], label="Type", name="ord_type", default_selected_key="MKT", width="size-1000"),
                ui.text_field(label="Limit (LMT only)", name="limit_px", width="size-1200"),
                ui.picker(*[ui.item(t, key=t) for t in TIFS], label="TIF", name="tif", default_selected_key="DAY", width="size-1000"),
                ui.button("Add line", variant="secondary", type="submit", align_self="end"),
                direction="row",
                gap="size-100",
                wrap=True,
                align_items="end",
            ),
            on_submit=add_line,
            key=f"builder-{builder_key}",
        )
        staged_rows = [
            ui.flex(
                ui.text(f"{i + 1}. {line.text()}"),
                ui.action_button("remove", is_quiet=True, on_press=lambda _e=None, i=i: remove_line(i)),
                direction="row",
                gap="size-100",
                align_items="center",
            )
            for i, line in enumerate(staged)
        ]
        parsed_rows = [ui.text(f"{len(staged) + i + 1}. {line.text()}") for i, line in enumerate(parsed)]
        error_rows = [ui.text(f"line {n}: {msg}  ({t})", color="negative") for n, t, msg in errors]
        header = ui.flex(
            ui.text_field(label="Basket name", name="name", default_value=existing.name if existing else "", is_read_only=existing is not None, is_required=existing is None, auto_focus=existing is None, width="size-3000"),
            ui.picker(*[ui.item(s, key=s) for s in STRATEGIES], label="Strategy", name="strategy", default_selected_key=existing.strategy if existing else "DMA", is_disabled=existing is not None),
            ui.picker(*[ui.item(v, key=v) for v in venues], label="Route on create", name="route_to", placeholder="stay STAGED"),
            direction="row",
            gap="size-200",
            wrap=True,
            align_items="end",
        )
        paste = ui.text_area(
            label="Paste lines: SYMBOL SIDE QTY [MKT | LMT price] [DAY | IOC | GTC]  (one per line, # comments)",
            name="lines",
            value=text,
            on_change=set_text,
            width="100%",
            height="size-1200",
        )
        basket_form = ui.form(
            header,
            paste,
            ui.text(f"{len(lines)} line(s) staged" + (f", {len(errors)} error(s)" if errors else "")),
            ui.flex(*staged_rows, *parsed_rows, *error_rows, direction="column", gap="size-50"),
            field_error(error),
            ui.button_group(
                ui.button("Cancel" if not inline else "Clear", variant="secondary", type="button", on_press=lambda _e=None: on_done()),
                ui.button("Add orders" if existing is not None else "Create basket", variant="accent", type="submit", is_disabled=not lines or bool(errors)),
                align="end",
            ),
            on_submit=create,
        )
        body = ui.flex(
            ui.text("Build a line and add it, or paste lines below", color="gray-700"),
            builder,
            ui.divider(size="S"),
            basket_form,
            direction="column",
            gap="size-150",
        )
        title = "New basket" if existing is None else f"Add orders to {existing.basket_id} -- {existing.name}"
        if inline:
            return ui.flex(ui.heading(title, level=4), body, direction="column", gap="size-150")
        return ui.dialog(ui.heading(title), ui.content(body), size="L")

    # -- the dashboard component ------------------------------------------------------------

    @ui.component
    def oms_dashboard():
        selected_basket, set_selected_basket = ui.use_state(initial_basket())
        selected_order, set_selected_order = ui.use_state(None)
        pending, set_pending = ui.use_state(None)
        trader, set_trader = ui.use_state(cfg.trader)
        sim_on, set_sim_on = ui.use_state(bool(runtime.simulator.running))
        # The context-menu resolver runs on the server when the menu opens, from the
        # closure of the render that built the table. A right-click selects the cell and
        # fires on_selection_change, but that arrives as a *new* render -- so the resolver
        # reads the latest selection from refs, never from its (possibly stale) closure.
        order_ref = ui.use_ref(None)
        basket_ref = ui.use_ref(initial_basket())
        # Bumped after every trader action so the toolbar text (basket name, unrouted
        # count) is recomputed; the tables tick on their own and never need it.
        version, set_version = ui.use_state(0)

        def close_dialog() -> None:
            set_pending(None)
            set_version(version + 1)

        # -- selection ---------------------------------------------------------------
        def on_basket_press(row: Any, *_rest: Any) -> None:
            key = row_key(row, "BasketId")
            if key:
                basket_ref.current = key
            if key and key != selected_basket:
                set_selected_basket(key)
                set_selected_order(None)
                order_ref.current = None

        def on_basket_selection(rows: Any) -> None:
            if rows:
                on_basket_press(rows[0])

        def on_order_selection(rows: Any) -> None:
            picked = row_to_dict(rows[0]) if rows else None
            order_ref.current = picked
            set_selected_order(picked)

        def on_order_press(row: Any, *_rest: Any) -> None:
            picked = row_to_dict(row)
            order_ref.current = picked
            set_selected_order(picked)

        selected_order_id = (selected_order or {}).get("OrderId")

        # -- live filters (memoised on the selection only) -----------------------------
        def basket_filter(table: Any) -> Any:
            if not selected_basket:
                return table
            return table.where(f"BasketId == `{_quote(selected_basket)}`")

        basket_orders = ui.use_memo(lambda: basket_filter(orders_view), [selected_basket])
        basket_executions = ui.use_memo(
            lambda: executions.where(f"OrderId == `{_quote(selected_order_id)}`") if selected_order_id else basket_filter(executions),
            [selected_basket, selected_order_id],
        )
        basket_events = ui.use_memo(
            lambda: events.where(f"OrderId == `{_quote(selected_order_id)}`") if selected_order_id else basket_filter(events),
            [selected_basket, selected_order_id],
        )
        basket_tree = ui.use_memo(
            lambda: bridge.orders_readonly.where(f"BasketId == `{_quote(selected_basket)}`").tree(id_col="OrderId", parent_col="ParentId", promote_orphans=True)
            if selected_basket
            else orders_tree,
            [selected_basket],
        )
        progress = ui.use_memo(
            lambda: _safe(
                lambda: dx.bar(
                    basket_orders.where("Status != `SPLIT`").view(["Symbol", "Filled", "Leaves"]),
                    x="Symbol",
                    y=["Filled", "Leaves"],
                    title="Filled vs leaves by symbol",
                )
            )
            if dx is not None
            else None,
            [selected_basket],
        )

        # -- actions -------------------------------------------------------------------
        def resolve_order(params: Any = None) -> Optional[Order]:
            column = (params or {}).get("column_name") if isinstance(params, Mapping) else None
            value = (params or {}).get("value") if isinstance(params, Mapping) else None
            candidates = []
            if column == "OrderId" and value:
                candidates.append(str(value))
            current = order_ref.current or {}
            if current.get("OrderId"):
                candidates.append(str(current["OrderId"]))
            if selected_order_id:
                candidates.append(str(selected_order_id))
            for candidate in candidates:
                order = core.orders.get(candidate)
                if order is not None:
                    return order
            return None

        def open_for(kind: str) -> Callable[[Any], None]:
            """A menu action that resolves the order when *clicked* (the selection has landed by then)."""

            def go(_params: Any = None) -> None:
                order = resolve_order()
                if order is None:
                    toast("Select an order row first", variant="negative")
                elif kind not in allowed_actions(order):
                    toast(f"{order.order_id} is {order.status}: cannot {kind}", variant="negative")
                else:
                    set_pending((kind, order.order_id))

            return go

        def route_selected(venue: str) -> Callable[[Any], None]:
            def go(_params: Any = None) -> None:
                order = resolve_order()
                if order is None:
                    toast("Select an order row first", variant="negative")
                else:
                    run(lambda: core.route(order.order_id, venue, user=trader), lambda o: f"{o.order_id} routed to {o.venue}")
                    set_version(version + 1)

            return go

        def route_action(order_id: str, venue: str) -> Callable[[Any], None]:
            def go(_params: Any = None) -> None:
                run(lambda: core.route(order_id, venue, user=trader), lambda o: f"{o.order_id} routed to {o.venue}")
                set_version(version + 1)

            return go

        def order_menu(params: Any) -> Any:
            order = resolve_order(params)
            if order is None:
                # The selection event has not reached the server yet: generic items that
                # resolve the order when they are clicked.
                return [
                    {"title": "Modify selected order…", "order": 10, "action": open_for("modify")},
                    {"title": "Route selected order to", "order": 20, "actions": [{"title": v, "order": i, "action": route_selected(v)} for i, v in enumerate(venues)]},
                    {"title": "Split selected order…", "order": 30, "action": open_for("split")},
                    {"title": "Cancel selected order", "order": 40, "action": open_for("cancel")},
                ]
            allowed = allowed_actions(order)
            summary = f"{order.status} · filled {order.filled:,} / {order.qty:,}"
            items: List[Dict[str, Any]] = []
            if "modify" in allowed:
                items.append({"title": f"Modify {order.order_id}…", "order": 10, "description": summary, "action": lambda _p, oid=order.order_id: set_pending(("modify", oid))})
            if "route" in allowed:
                items.append({"title": f"Route {order.order_id} to", "order": 20, "description": "pick a venue", "actions": [{"title": v, "order": i, "action": route_action(order.order_id, v)} for i, v in enumerate(venues)]})
            if "split" in allowed:
                items.append({"title": f"Split {order.order_id}…", "order": 30, "description": f"{order.qty:,} shares into slices", "action": lambda _p, oid=order.order_id: set_pending(("split", oid))})
            if "cancel" in allowed:
                items.append({"title": f"Cancel {order.order_id}", "order": 40, "description": summary, "action": lambda _p, oid=order.order_id: set_pending(("cancel", oid))})
            if not items:
                return [{"title": f"{order.order_id} is {order.status} -- no actions", "action": lambda _p: None}]
            return items

        def resolve_basket(params: Any) -> Optional[str]:
            column = (params or {}).get("column_name") if isinstance(params, Mapping) else None
            value = (params or {}).get("value") if isinstance(params, Mapping) else None
            if column == "BasketId" and value and str(value) in core.baskets:
                return str(value)
            if basket_ref.current in core.baskets:
                return basket_ref.current
            return selected_basket if selected_basket in core.baskets else None

        def basket_menu(params: Any) -> Any:
            basket_id = resolve_basket(params)
            if basket_id is None:
                return [{"title": "Select a basket row first, then right-click", "action": lambda _p: None}]
            basket = core.baskets[basket_id]
            unrouted = sum(1 for o in core.orders_in(basket_id) if o.status == "NEW")
            live = sum(1 for o in core.orders_in(basket_id) if "cancel" in allowed_actions(o))
            items: List[Dict[str, Any]] = [
                {"title": f"Add orders to {basket_id}…", "order": 10, "description": basket.name, "action": lambda _p, b=basket_id: set_pending(("add_orders", b))},
            ]
            if unrouted:
                items.append(
                    {
                        "title": f"Route {unrouted} unrouted order(s) to",
                        "order": 20,
                        "actions": [
                            {"title": v, "order": i, "action": (lambda _p, b=basket_id, v=v: (run(lambda: core.route_basket(b, v, user=trader), lambda os: f"{len(os)} orders of {b} routed to {v}"), set_version(version + 1)))}
                            for i, v in enumerate(venues)
                        ],
                    }
                )
            if live:
                items.append({"title": f"Cancel basket ({live} live order(s))", "order": 30, "action": lambda _p, b=basket_id: set_pending(("cancel_basket", b))})
            return items

        def toggle_sim(value: bool) -> None:
            if value:
                runtime.simulator.start()
            else:
                runtime.simulator.stop()
            set_sim_on(bool(runtime.simulator.running))
            toast("Mock venue " + ("running" if runtime.simulator.running else "stopped"), variant="neutral")

        # -- toolbar -------------------------------------------------------------------
        basket_name = core.baskets[selected_basket].name if selected_basket in core.baskets else "(no basket selected)"
        unrouted_here = sum(1 for o in core.orders_in(selected_basket) if o.status == "NEW") if selected_basket else 0

        route_menu = _first(
            lambda: ui.menu_trigger(
                ui.action_button("Route basket"),
                ui.menu(
                    *[ui.item(v, key=v) for v in venues],
                    on_action=lambda key: (run(lambda: core.route_basket(selected_basket, str(key), user=trader), lambda os: f"{len(os)} orders of {selected_basket} routed to {key}"), set_version(version + 1)),
                ),
            ),
            lambda: ui.action_button("Route basket to " + venues[0], on_press=lambda _e=None: run(lambda: core.route_basket(selected_basket, venues[0], user=trader), lambda os: f"{len(os)} orders routed")),
        )
        toolbar = ui.flex(
            ui.action_button("New basket", on_press=lambda _e=None: set_pending(("new_basket", None))),
            ui.action_button("Add orders", is_disabled=not selected_basket, on_press=lambda _e=None: set_pending(("add_orders", selected_basket))),
            route_menu,
            ui.action_button("Cancel basket", is_disabled=not selected_basket, on_press=lambda _e=None: set_pending(("cancel_basket", selected_basket))),
            ui.switch("Mock venue", is_selected=sim_on, on_change=toggle_sim),
            ui.text_field(label="Trader", value=trader, on_change=set_trader, width="size-1600", is_quiet=True),
            ui.text(f"Selected: {selected_basket or '-'} {basket_name}" + (f" · {unrouted_here} unrouted" if unrouted_here else "")),
            direction="row",
            gap="size-150",
            align_items="end",
            wrap=True,
        )

        # -- tables --------------------------------------------------------------------
        baskets_table = _first(
            lambda: ui.table(
                baskets_view,
                on_row_press=on_basket_press,
                on_selection_change=on_basket_selection,
                always_fetch_columns=True,
                context_menu=[basket_menu],
                format_=basket_formats(),
                hidden_columns=BASKET_HIDDEN,
                column_display_names=BASKET_NAMES,
                density="compact",
            ),
            lambda: ui.table(baskets_view, on_row_press=on_basket_press, always_fetch_columns=True, context_menu=[basket_menu]),
            lambda: ui.table(baskets_view, on_row_press=on_basket_press),
            lambda: ui.table(baskets_view),
        )
        orders_table = _first(
            lambda: ui.table(
                basket_orders,
                on_row_press=on_order_press,
                on_selection_change=on_order_selection,
                always_fetch_columns=True,
                context_menu=[order_menu],
                format_=order_formats(),
                hidden_columns=ORDER_HIDDEN,
                column_display_names=ORDER_NAMES,
                density="compact",
            ),
            lambda: ui.table(basket_orders, on_row_press=on_order_press, always_fetch_columns=True, context_menu=[order_menu]),
            lambda: ui.table(basket_orders, on_row_press=on_order_press),
            lambda: ui.table(basket_orders),
        )
        executions_table = _first(
            lambda: ui.table(basket_executions, reverse=True, density="compact", format_=time_formats() + [ui.TableFormat(cols="LastPx", value="0.00"), ui.TableFormat(cols="LastQty", value="#,##0")]),
            lambda: ui.table(basket_executions, reverse=True),
            lambda: ui.table(basket_executions),
        )
        events_table = _first(
            lambda: ui.table(basket_events, reverse=True, density="compact", format_=time_formats()),
            lambda: ui.table(basket_events, reverse=True),
            lambda: ui.table(basket_events),
        )
        quotes_table = _first(lambda: ui.table(quotes, format_=[ui.TableFormat(cols=["Bid", "Ask", "Last", "RefPx"], value="0.00"), ui.TableFormat(cols="ChgPct", value="0.00")], density="compact"), lambda: ui.table(quotes))
        tree_table = _first(lambda: ui.table(basket_tree, density="compact"), lambda: ui.table(basket_tree))

        # -- dialogs -------------------------------------------------------------------
        dialog: Any = None
        inline_ticket: Any = None
        if pending is not None:
            kind, payload = pending
            if kind == "modify" and payload in core.orders:
                dialog = modal(modify_dialog(payload, trader, close_dialog), close_dialog)
            elif kind == "split" and payload in core.orders:
                dialog = modal(split_dialog(payload, trader, close_dialog), close_dialog)
            elif kind == "cancel" and payload in core.orders:
                order = core.orders[payload]
                dialog = modal(
                    confirm_dialog(
                        f"Cancel {order.order_id}?",
                        f"{order.summary()} -- {order.status}, filled {order.filled:,} of {order.qty:,}. The remaining {order.leaves:,} will be cancelled.",
                        "Cancel order",
                        lambda oid=payload: core.cancel(oid, user=trader),
                        lambda o: f"{o.order_id} cancelled",
                        close_dialog,
                    ),
                    close_dialog,
                )
            elif kind == "cancel_basket" and payload in core.baskets:
                basket = core.baskets[payload]
                live = [o for o in core.orders_in(payload) if "cancel" in allowed_actions(o)]
                dialog = modal(
                    confirm_dialog(
                        f"Cancel basket {basket.basket_id}?",
                        f"{basket.name}: {len(live)} live order(s) will be cancelled.",
                        "Cancel basket",
                        lambda b=payload: core.cancel_basket(b, user=trader),
                        lambda os: f"{len(os)} orders of {payload} cancelled",
                        close_dialog,
                    ),
                    close_dialog,
                )
            elif kind in ("new_basket", "add_orders"):
                target = payload if kind == "add_orders" and payload in core.baskets else None
                if cfg.ticket_panel:
                    inline_ticket = basket_ticket(target, trader, close_dialog, inline=True)
                else:
                    dialog = modal(basket_ticket(target, trader, close_dialog), close_dialog)

        # -- layout --------------------------------------------------------------------
        baskets_body = ui.flex(toolbar, baskets_table, *( [dialog] if dialog is not None else [] ), direction="column", gap="size-100", height="100%")
        right_panels = [
            ui.panel(executions_table, title="Executions" + (f" -- {selected_order_id}" if selected_order_id else "")),
            ui.panel(events_table, title="Events" + (f" -- {selected_order_id}" if selected_order_id else "")),
            ui.panel(quotes_table, title="Quotes"),
            ui.panel(tree_table, title="Order tree"),
        ]
        if progress is not None:
            right_panels.append(ui.panel(progress, title="Fill progress"))
        if inline_ticket is not None:
            right_panels.insert(0, ui.panel(inline_ticket, title="Ticket"))
        orders_title = f"Orders -- {selected_basket or 'all'} {basket_name}  (right-click a row: modify / route / split / cancel)"
        return ui.column(
            ui.row(ui.panel(baskets_body, title="Baskets"), height=38),
            ui.row(
                ui.column(ui.panel(orders_table, title=orders_title), width=64),
                _first(lambda: ui.stack(*right_panels, width=36, active_item_index=0), lambda: ui.stack(*right_panels, width=36)),
                height=62,
            ),
        )

    return ui.dashboard(oms_dashboard())
