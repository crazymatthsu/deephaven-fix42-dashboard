"""``fix42cache`` -- pure-python FIX 4.2 parser and order state machine.

Stdlib only, no Deephaven imports: the Deephaven layer (``dh_app``) feeds raw
FIX strings to :class:`~fix42cache.state_machine.OrderStateMachine` and publishes
the returned rows.  The behavioural contract lives in
``docs/01-fix42-messages-and-state-machine.md``.

Typical use::

    from fix42cache import OrderStateMachine

    machine = OrderStateMachine()
    result = machine.process(raw_fix_string)
    if result.error is None:
        state_row = result.state.to_row()
        exec_rows = [row.to_row() for row in result.executions]
"""

from .fixtags import (
    TERMINAL_STATUSES,
    CxlRejResponseTo,
    ExecTransType,
    ExecType,
    OrdStatus,
    OrdType,
    Side,
    Tag,
    TimeInForce,
    is_terminal,
)
from .model import (
    EXECUTION_COLUMNS,
    MESSAGE_COLUMNS,
    ORDER_EVENT_COLUMNS,
    ORDER_STATE_COLUMNS,
    EventType,
    ExecutionRow,
    FillStatus,
    MessageRow,
    OrderEventRow,
    OrderState,
    PendingAction,
)
from .parser import (
    PIPE,
    SOH,
    checksum_ok,
    parse_fix,
    parse_transact_time,
    render_fields,
    render_pipe,
)
from .state_machine import HANDLED_MSG_TYPES, OrderStateMachine, Result, utcnow

__version__ = "0.1.0"

__all__ = [
    # fixtags
    "Tag",
    "OrdStatus",
    "ExecType",
    "ExecTransType",
    "Side",
    "OrdType",
    "TimeInForce",
    "CxlRejResponseTo",
    "TERMINAL_STATUSES",
    "is_terminal",
    # parser
    "SOH",
    "PIPE",
    "parse_fix",
    "render_pipe",
    "render_fields",
    "checksum_ok",
    "parse_transact_time",
    # model
    "OrderState",
    "ExecutionRow",
    "OrderEventRow",
    "MessageRow",
    "EventType",
    "FillStatus",
    "PendingAction",
    "ORDER_STATE_COLUMNS",
    "EXECUTION_COLUMNS",
    "ORDER_EVENT_COLUMNS",
    "MESSAGE_COLUMNS",
    # state machine
    "OrderStateMachine",
    "Result",
    "HANDLED_MSG_TYPES",
    "utcnow",
    "__version__",
]
