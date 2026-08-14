"""Row models: frozen column names, defaults and enum rendering (doc 01 §4/§6)."""

from __future__ import annotations

from datetime import datetime, timezone

from fixhelpers import exec_report, new_order

from fix42cache.fixtags import ExecTransType, ExecType, OrdStatus, OrdType, Side, TimeInForce
from fix42cache.model import (
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
from fix42cache.parser import parse_fix

INGEST = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)


def test_order_state_row_uses_exact_frozen_columns() -> None:
    row = OrderState(order_key="K1").to_row()
    assert list(row) == list(ORDER_STATE_COLUMNS)
    assert ORDER_STATE_COLUMNS[:6] == (
        "OrderKey",
        "OrderID",
        "ClOrdID",
        "OrigClOrdID",
        "RootClOrdID",
        "ClOrdIDChain",
    )
    assert ORDER_STATE_COLUMNS[-1] == "Terminal"


def test_order_state_row_defaults() -> None:
    row = OrderState(order_key="K1").to_row()
    assert row["OrderKey"] == "K1"
    assert row["ClOrdIDChain"] == ""
    assert row["PendingAction"] == PendingAction.NONE
    assert row["PendingClOrdID"] == ""
    assert row["OrdStatus"] == ""
    assert row["OrderQty"] == 0.0 and row["AvgPx"] == 0.0
    assert row["ExecCount"] == 0 and row["MsgCount"] == 0
    assert row["FirstSeenTs"] is None and row["LastUpdateTs"] is None
    assert row["Terminal"] is False


def test_order_state_renders_enum_names_and_joined_chain() -> None:
    state = OrderState(
        order_key="K1",
        clordid_chain=["C1", "C2", "C3"],
        side=Side.SELL_SHORT,
        ord_type=OrdType.LIMIT,
        time_in_force=TimeInForce.GTC,
        ord_status=OrdStatus.PARTIALLY_FILLED,
        last_exec_type=ExecType.PARTIAL_FILL,
    )
    row = state.to_row()
    assert row["ClOrdIDChain"] == "C1,C2,C3"
    assert row["Side"] == "SELL_SHORT"
    assert row["OrdType"] == "LIMIT"
    assert row["TimeInForce"] == "GTC"
    assert row["OrdStatus"] == "PARTIALLY_FILLED"
    assert row["LastExecType"] == "PARTIAL_FILL"


def test_order_state_terminal_is_computed() -> None:
    for status in (OrdStatus.FILLED, OrdStatus.CANCELED, OrdStatus.REJECTED,
                   OrdStatus.EXPIRED, OrdStatus.DONE_FOR_DAY):
        assert OrderState(order_key="K", ord_status=status).terminal is True
    for status in (OrdStatus.NEW, OrdStatus.PARTIALLY_FILLED, OrdStatus.PENDING_CANCEL):
        assert OrderState(order_key="K", ord_status=status).terminal is False


def test_order_state_copy_is_independent() -> None:
    state = OrderState(order_key="K1", clordid_chain=["C1"])
    snapshot = state.copy()
    state.clordid_chain.append("C2")
    state.cum_qty = 500.0
    assert snapshot.clordid_chain == ["C1"]
    assert snapshot.cum_qty == 0.0


def test_execution_row_uses_exact_frozen_columns() -> None:
    row = ExecutionRow(order_key="K1").to_row()
    assert list(row) == list(EXECUTION_COLUMNS)
    assert row["FillStatus"] == FillStatus.NORMAL
    assert row["IsFill"] is False
    assert row["ExecTransType"] == "" and row["ExecType"] == ""


def test_execution_row_renders_enum_names() -> None:
    row = ExecutionRow(
        order_key="K1",
        exec_trans_type=ExecTransType.CORRECT,
        exec_type=ExecType.FILL,
        ord_status=OrdStatus.FILLED,
        is_fill=True,
        fill_status=FillStatus.CORRECTED,
    ).to_row()
    assert row["ExecTransType"] == "CORRECT"
    assert row["ExecType"] == "FILL"
    assert row["OrdStatus"] == "FILLED"
    assert row["FillStatus"] == "CORRECTED"


def test_order_event_row_uses_exact_frozen_columns() -> None:
    row = OrderEventRow(order_key="K1", event_type=EventType.NEW_ACK).to_row()
    assert list(row) == list(ORDER_EVENT_COLUMNS)
    assert row["EventType"] == "NEW_ACK"


def test_event_type_names_are_the_frozen_doc_list() -> None:
    assert set(EventType.ALL) == {
        "NEW_REQUEST", "NEW_ACK", "NEW_REJECT",
        "AMEND_REQUEST", "AMEND_ACK", "AMEND_REJECT",
        "CANCEL_REQUEST", "CANCEL_ACK", "CANCEL_REJECT",
        "PENDING_NEW", "PENDING_AMEND", "PENDING_CANCEL",
        "PARTIAL_FILL", "FULL_FILL", "FILL_BUST", "FILL_CORRECT",
        "DK_TRADE", "RESTATED", "STATUS", "EXPIRED", "DONE_FOR_DAY",
    }
    assert set(FillStatus.ALL) == {"NORMAL", "BUSTED", "CORRECTED", "DK"}
    assert set(PendingAction.ALL) == {"NONE", "NEW", "CANCEL", "REPLACE"}


def test_message_row_uses_exact_frozen_columns() -> None:
    row = MessageRow(order_key="K1").to_row()
    assert list(row) == list(MESSAGE_COLUMNS)


def test_message_row_from_fields_types_every_doc_tag() -> None:
    raw = exec_report(
        "1",
        exec_id="E3",
        clordid="C1",
        orig_clordid="C0",
        exec_ref_id="E2",
        exec_trans_type="0",
        cum_qty=400,
        leaves_qty=600,
        avg_px=185.48,
        last_shares=400,
        last_px=185.48,
        last_mkt="XNAS",
        account="ACC1",
        price=185.50,
        text="partial",
        seq=7,
    )
    row = MessageRow.from_fields(parse_fix(raw), "K1", raw, INGEST).to_row()

    assert row["OrderKey"] == "K1" and row["MsgType"] == "8"
    assert row["ClOrdID"] == "C1" and row["OrigClOrdID"] == "C0"
    assert row["ExecID"] == "E3" and row["ExecRefID"] == "E2"
    assert row["ExecTransType"] == "NEW" and row["ExecType"] == "PARTIAL_FILL"
    assert row["OrdStatus"] == "PARTIALLY_FILLED"
    assert row["Account"] == "ACC1" and row["Symbol"] == "IBM" and row["Side"] == "BUY"
    assert row["CumQty"] == 400.0 and row["LeavesQty"] == 600.0 and row["AvgPx"] == 185.48
    assert row["LastShares"] == 400.0 and row["LastPx"] == 185.48
    assert row["LastMkt"] == "XNAS" and row["Text"] == "partial"
    assert row["SeqNum"] == 7
    assert row["ChecksumOk"] is True
    assert row["RawFix"].startswith("8=FIX.4.2|") and "\x01" not in row["RawFix"]
    assert row["TransactTime"] == datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    assert row["SendingTime"] == datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    assert row["IngestTs"] == INGEST


def test_message_row_absent_numeric_tags_are_none() -> None:
    raw = new_order("C1", price=None, ord_type="1")
    row = MessageRow.from_fields(parse_fix(raw), "C1", raw, INGEST).to_row()
    assert row["Price"] is None
    assert row["CumQty"] is None and row["LeavesQty"] is None and row["AvgPx"] is None
    assert row["OrderQty"] == 1000.0
    assert row["OrdType"] == "MARKET"
    assert row["ExecType"] == "" and row["CxlRejResponseTo"] == ""
    assert row["HandlInst"] == "1"


def test_message_row_records_bad_checksum_without_rejecting() -> None:
    raw = new_order("C1").replace("|10=", "|10=9")
    row = MessageRow.from_fields(parse_fix(raw), "C1", raw, INGEST).to_row()
    assert row["ChecksumOk"] is False
    assert row["ClOrdID"] == "C1"
