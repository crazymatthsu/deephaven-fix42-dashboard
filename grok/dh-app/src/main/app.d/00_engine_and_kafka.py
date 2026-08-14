"""Ingest Kafka FIX 4.2, run the Java state machine, publish the live DAG.

Deephaven server-side practice: bind ticking tables at module scope so
Application Mode and the Code Studio share one DAG. The Java engine is
the apply/link step; last_by is only the latest-state projection.
"""

from deephaven import dtypes as dht
from deephaven import DynamicTableWriter
from deephaven.stream.kafka import consumer as kc
from deephaven.table_listener import listen
from deephaven.stream import blink_to_append_only

import jpy
import os

OmsCaches = jpy.get_type("com.deephaven.fix42.oms.OmsCaches")
oms_cache = OmsCaches.createDefault()

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.environ.get("FIX_TOPIC", "fix42.dropcopy")

_ORDER_COLS = {
    "OrderKey": dht.string,
    "PreviousOrderKey": dht.string,
    "Tombstone": dht.bool_,
    "ClOrdID": dht.string,
    "OrigClOrdID": dht.string,
    "ClOrdIDHistory": dht.string,
    "OrderID": dht.string,
    "Account": dht.string,
    "Symbol": dht.string,
    "Side": dht.string,
    "OrdType": dht.string,
    "TimeInForce": dht.string,
    "OrdStatus": dht.string,
    "ExecType": dht.string,
    "ExecTransType": dht.string,
    "LastExecID": dht.string,
    "OrderQty": dht.double,
    "CumQty": dht.double,
    "LeavesQty": dht.double,
    "LastQty": dht.double,
    "LastPx": dht.double,
    "AvgPx": dht.double,
    "Price": dht.double,
    "ParentOrderID": dht.string,
    "ParentClOrdID": dht.string,
    "PendingCancel": dht.bool_,
    "PendingReplace": dht.bool_,
    "DkTrade": dht.bool_,
    "Text": dht.string,
    "CxlRejReason": dht.string,
    "CxlRejResponseTo": dht.string,
    "DkReason": dht.string,
    "OrdRejReason": dht.string,
    "TransactTime": dht.string,
    "LastMsgType": dht.string,
    "Version": dht.int32,
    "Created": dht.bool_,
    "Applied": dht.bool_,
}

_EVENT_COLS = {
    "OrderKey": dht.string,
    "MsgType": dht.string,
    "ClOrdID": dht.string,
    "OrigClOrdID": dht.string,
    "OrderID": dht.string,
    "ExecID": dht.string,
    "Account": dht.string,
    "Symbol": dht.string,
    "RawFix": dht.string,
}

_EXEC_COLS = {
    "OrderKey": dht.string,
    "ExecID": dht.string,
    "ExecRefID": dht.string,
    "ExecType": dht.string,
    "ExecTransType": dht.string,
    "OrdStatus": dht.string,
    "LastQty": dht.double,
    "LastPx": dht.double,
    "CumQty": dht.double,
    "LeavesQty": dht.double,
    "AvgPx": dht.double,
    "ClOrdID": dht.string,
    "Symbol": dht.string,
}

_INDEX_COLS = {
    "Alias": dht.string,
    "AliasType": dht.string,
    "OrderKey": dht.string,
}

_ERR_COLS = {
    "ErrorType": dht.string,
    "Message": dht.string,
    "RawFix": dht.string,
}

order_writer = DynamicTableWriter(_ORDER_COLS)
event_writer = DynamicTableWriter(_EVENT_COLS)
exec_writer = DynamicTableWriter(_EXEC_COLS)
index_writer = DynamicTableWriter(_INDEX_COLS)
error_writer = DynamicTableWriter(_ERR_COLS)

order_state_updates = order_writer.table
order_events = event_writer.table
executions = exec_writer.table
id_index_updates = index_writer.table
fix_errors = error_writer.table


def _s(value):
    return "" if value is None else str(value)


def _write_state(result):
    state = result.getState()
    if state is None:
        return
    order_writer.write_row(
        _s(result.getOrderKey()),
        _s(result.getPreviousOrderKey()),
        False,
        _s(state.getClOrdId()),
        _s(state.getOrigClOrdId()),
        _s(state.getClOrdIdHistoryCsv()),
        _s(state.getOrderId()),
        _s(state.getAccount()),
        _s(state.getSymbol()),
        _s(state.getSide()),
        _s(state.getOrdType()),
        _s(state.getTimeInForce()),
        _s(state.getOrdStatus()),
        _s(state.getExecType()),
        _s(state.getExecTransType()),
        _s(state.getLastExecId()),
        float(state.getOrderQty()),
        float(state.getCumQty()),
        float(state.getLeavesQty()),
        float(state.getLastQty()),
        float(state.getLastPx()),
        float(state.getAvgPx()),
        float(state.getPrice()),
        _s(state.getParentOrderId()),
        _s(state.getParentClOrdId()),
        bool(state.isPendingCancel()),
        bool(state.isPendingReplace()),
        bool(state.isDkTrade()),
        _s(state.getText()),
        _s(state.getCxlRejReason()),
        _s(state.getCxlRejResponseTo()),
        _s(state.getDkReason()),
        _s(state.getOrdRejReason()),
        _s(state.getTransactTime()),
        _s(state.getLastMsgType()),
        int(state.getVersion()),
        bool(result.isCreated()),
        bool(result.isApplied()),
    )
    prev = _s(result.getPreviousOrderKey())
    if prev:
        order_writer.write_row(
            prev,
            "",
            True,
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            "",
            "",
            False,
            False,
            False,
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            0,
            False,
            False,
        )


def _write_event(result, raw):
    state = result.getState()
    event_writer.write_row(
        _s(result.getOrderKey()),
        _s(result.getMsgType()),
        _s(state.getClOrdId()) if state is not None else "",
        _s(state.getOrigClOrdId()) if state is not None else "",
        _s(state.getOrderId()) if state is not None else "",
        _s(state.getLastExecId()) if state is not None else "",
        _s(state.getAccount()) if state is not None else "",
        _s(state.getSymbol()) if state is not None else "",
        _s(raw),
    )


def _is_execution(result):
    if result.getMsgType() != "8" or result.getState() is None:
        return False
    exec_type = _s(result.getState().getExecType())
    trans = _s(result.getState().getExecTransType())
    return exec_type in ("1", "2") or trans in ("1", "2")


def _write_exec(result):
    state = result.getState()
    exec_writer.write_row(
        _s(result.getOrderKey()),
        _s(state.getLastExecId()),
        "",
        _s(state.getExecType()),
        _s(state.getExecTransType()),
        _s(state.getOrdStatus()),
        float(state.getLastQty()),
        float(state.getLastPx()),
        float(state.getCumQty()),
        float(state.getLeavesQty()),
        float(state.getAvgPx()),
        _s(state.getClOrdId()),
        _s(state.getSymbol()),
    )


def _write_indexes(result):
    state = result.getState()
    if state is None:
        return
    key = _s(result.getOrderKey())
    seen = set()

    def put(alias, alias_type):
        if alias and alias not in seen:
            seen.add(alias)
            index_writer.write_row(alias, alias_type, key)

    put(_s(state.getClOrdId()), "ClOrdID")
    put(_s(state.getOrigClOrdId()), "ClOrdID")
    put(_s(state.getOrderId()), "OrderID")
    put(_s(state.getLastExecId()), "ExecID")
    history = _s(state.getClOrdIdHistoryCsv())
    if history:
        for part in history.split(","):
            put(part, "ClOrdID")


def ingest_raw(raw):
    if raw is None:
        return
    text = str(raw)
    if not text:
        return
    try:
        result = oms_cache.ingest(text)
    except Exception as exc:  # noqa: BLE001 — keep the DAG alive
        error_writer.write_row(type(exc).__name__, str(exc), text)
        return
    _write_state(result)
    _write_event(result, text)
    _write_indexes(result)
    if _is_execution(result) and result.isApplied():
        _write_exec(result)


def _on_fix(update, is_replay):
    added = update.added()
    if not added or "RawFix" not in added:
        return
    for raw in added["RawFix"]:
        ingest_raw(raw)


try:
    fix_raw = kc.consume(
        {"bootstrap.servers": BOOTSTRAP},
        TOPIC,
        key_spec=kc.KeyValueSpec.IGNORE,
        value_spec=kc.simple_spec("RawFix", dht.string),
        table_type=kc.TableType.blink(),
        offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    )
except Exception as exc:  # noqa: BLE001
    from deephaven import new_table
    from deephaven.column import string_col

    fix_raw = new_table([string_col("RawFix", [])])
    error_writer.write_row("KafkaUnavailable", str(exc), "")

fix_tape = blink_to_append_only(fix_raw) if getattr(fix_raw, "is_blink", False) else fix_raw
_fix_handle = listen(fix_raw, _on_fix, do_replay=True)

orders_latest = order_state_updates.last_by("OrderKey").where("Tombstone = false")
clord_index = (
    id_index_updates.where("AliasType = `ClOrdID`").last_by("Alias").view(["ClOrdID = Alias", "OrderKey"])
)
order_id_index = (
    id_index_updates.where("AliasType = `OrderID`").last_by("Alias").view(["OrderID = Alias", "OrderKey"])
)
exec_index = (
    id_index_updates.where("AliasType = `ExecID`").last_by("Alias").view(["ExecID = Alias", "OrderKey"])
)
