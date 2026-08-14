"""FIX 4.2 blotter dashboard.

Selecting an OrderKey / ClOrdID filters the executions panel and the
new/amend/cancel history panel. Tables are also left bound so the Code
Studio Linker can join them by hand.
"""

from deephaven import ui


_STATUS_FORMAT = [
    ui.TableFormat(if_="OrdStatus = `2`", background_color="positive", color="white"),
    ui.TableFormat(if_="OrdStatus = `1`", background_color="accent", color="white"),
    ui.TableFormat(if_="OrdStatus = `4` || OrdStatus = `8` || OrdStatus = `C`", background_color="negative", color="white"),
    ui.TableFormat(if_="OrdStatus = `A` || OrdStatus = `6` || OrdStatus = `E`", background_color="notice"),
]


@ui.component
def create_dashboard():
    selected, set_selected = ui.use_state("")
    account, set_account = ui.use_state("")
    symbol, set_symbol = ui.use_state("")

    def apply_blotter_filters(table):
        out = table
        if account:
            safe = account.replace("`", "")
            out = out.where(f"Account = `{safe}`")
        if symbol:
            safe = symbol.replace("`", "")
            out = out.where(f"Symbol = `{safe}`")
        return out

    blotter = ui.use_memo(
        lambda: apply_blotter_filters(orders_latest),
        [orders_latest, account, symbol],
    )

    order_key = ui.use_memo(lambda: resolve_order_key(selected), [selected])

    def selected_execs():
        if not order_key:
            return executions.head(0)
        safe = order_key.replace("`", "")
        return executions.where(f"OrderKey = `{safe}`")

    def selected_history():
        if not order_key:
            return order_events.head(0)
        safe = order_key.replace("`", "")
        return order_events.where(f"OrderKey = `{safe}`")

    def selected_order():
        if not order_key:
            return orders_latest.head(0)
        safe = order_key.replace("`", "")
        return orders_latest.where(f"OrderKey = `{safe}`")

    execs = ui.use_memo(selected_execs, [executions, order_key])
    hist = ui.use_memo(selected_history, [order_events, order_key])
    detail = ui.use_memo(selected_order, [orders_latest, order_key])

    def on_select(payload=None, **kwargs):
        row = payload or kwargs
        if row is None:
            return
        if isinstance(row, dict):
            token = row.get("OrderKey") or row.get("ClOrdID") or ""
            if token:
                set_selected(str(token))

    return ui.column(
        ui.panel(
            ui.flex(
                ui.text_field(label="Account", value=account, on_change=set_account),
                ui.text_field(label="Symbol", value=symbol, on_change=set_symbol),
                ui.text_field(
                    label="OrderKey / ClOrdID / OrderID / ExecID",
                    value=selected,
                    on_change=set_selected,
                ),
                ui.text(f"Resolved OrderKey: {order_key or '(none)'}"),
                direction="column",
                gap="size-100",
            ),
            title="Filters",
        ),
        ui.row(
            ui.panel(
                ui.table(
                    blotter,
                    format_=_STATUS_FORMAT,
                    on_row_double_press=on_select,
                ),
                title="Latest order state",
            ),
            ui.panel(ui.table(detail, format_=_STATUS_FORMAT), title="Selected order"),
        ),
        ui.row(
            ui.panel(ui.table(execs), title="Executions"),
            ui.panel(ui.table(hist), title="New / amend / cancel / reject history"),
        ),
    )


fix42_dashboard = ui.dashboard(create_dashboard())
