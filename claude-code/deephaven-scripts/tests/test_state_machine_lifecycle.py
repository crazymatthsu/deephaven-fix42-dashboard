"""The reference worked lifecycle from the spec (doc 05 §3.1).

    D(C1, 1000 IBM @ 185.50 limit)
      -> 8(150=A) -> 8(150=0)
      -> 8(150=1, 32=400 @ 185.48, 14=400, 151=600, 6=185.48)
      -> G(C2, price 185.55) -> 8(150=E) -> 8(150=5, 39=1)
      -> 8(150=2, 32=600 @ 185.55, 14=1000, 151=0, 6=185.522)
"""

from __future__ import annotations

import pytest
from fixhelpers import exec_report, new_order, replace_request

from fix42cache import OrderStateMachine, Result
from fix42cache.fixtags import ExecType, OrdStatus
from fix42cache.model import EventType, FillStatus, PendingAction

EXEC_IDS = ["EXEC-1", "EXEC-2", "EXEC-3", "EXEC-4", "EXEC-5", "EXEC-6"]


def _run_reference_lifecycle(machine: OrderStateMachine) -> list[Result]:
    messages = [
        new_order("C1", symbol="IBM", side="1", qty=1000, ord_type="2", price=185.50, seq=1),
        exec_report("A", exec_id="EXEC-1", clordid="C1", order_id="ORD-1",
                    cum_qty=0, leaves_qty=1000, avg_px=0, seq=2),
        exec_report("0", exec_id="EXEC-2", clordid="C1", order_id="ORD-1",
                    cum_qty=0, leaves_qty=1000, avg_px=0, seq=3),
        exec_report("1", exec_id="EXEC-3", clordid="C1", order_id="ORD-1",
                    last_shares=400, last_px=185.48, last_mkt="XNAS",
                    cum_qty=400, leaves_qty=600, avg_px=185.48, seq=4),
        replace_request("C2", "C1", qty=1000, price=185.55, seq=5),
        exec_report("E", exec_id="EXEC-4", clordid="C2", orig_clordid="C1",
                    order_id="ORD-1", cum_qty=400, leaves_qty=600, avg_px=185.48, seq=6),
        exec_report("5", exec_id="EXEC-5", clordid="C2", orig_clordid="C1",
                    order_id="ORD-1", ord_status="1",
                    cum_qty=400, leaves_qty=600, avg_px=185.48, seq=7),
        exec_report("2", exec_id="EXEC-6", clordid="C2", orig_clordid="C1",
                    order_id="ORD-1", last_shares=600, last_px=185.55, last_mkt="XNAS",
                    cum_qty=1000, leaves_qty=0, avg_px=185.522, seq=8),
    ]
    return [machine.process(raw) for raw in messages]


@pytest.fixture
def lifecycle(machine: OrderStateMachine) -> tuple[OrderStateMachine, list[Result]]:
    return machine, _run_reference_lifecycle(machine)


def test_reference_lifecycle_has_no_errors(
    lifecycle: tuple[OrderStateMachine, list[Result]],
) -> None:
    _, results = lifecycle
    assert [result.error for result in results] == [None] * 8
    assert all(result.state is not None for result in results)
    assert all(result.message is not None for result in results)


def test_reference_lifecycle_final_state(
    lifecycle: tuple[OrderStateMachine, list[Result]],
) -> None:
    _, results = lifecycle
    state = results[-1].state
    assert state is not None
    row = state.to_row()

    assert row["OrdStatus"] == "FILLED"
    assert row["ClOrdID"] == "C2"
    assert row["RootClOrdID"] == "C1"
    assert row["ClOrdIDChain"] == "C1,C2"
    assert row["CumQty"] == 1000.0
    assert row["LeavesQty"] == 0.0
    assert row["AvgPx"] == 185.522
    assert row["Price"] == 185.55
    assert row["OrderQty"] == 1000.0
    assert row["OrderID"] == "ORD-1"
    assert row["OrigClOrdID"] == "C1"
    assert row["Symbol"] == "IBM" and row["Side"] == "BUY" and row["OrdType"] == "LIMIT"
    assert row["LastShares"] == 600.0 and row["LastPx"] == 185.55
    assert row["LastMkt"] == "XNAS"
    assert row["LastExecType"] == "FILL"
    assert row["PendingAction"] == PendingAction.NONE and row["PendingClOrdID"] == ""
    assert row["ExecCount"] == 6
    assert row["MsgCount"] == 8
    assert row["LastMsgType"] == "8"
    assert row["Terminal"] is True
    assert state.terminal is True


def test_reference_lifecycle_avg_px_matches_the_venue_snapshot(
    lifecycle: tuple[OrderStateMachine, list[Result]],
) -> None:
    """(400 * 185.48 + 600 * 185.55) / 1000 == 185.522, adopted from tag 6."""
    _, results = lifecycle
    state = results[-1].state
    assert state is not None
    assert state.avg_px == pytest.approx((400 * 185.48 + 600 * 185.55) / 1000)


def test_reference_lifecycle_every_identifier_resolves_to_one_order_key(
    lifecycle: tuple[OrderStateMachine, list[Result]],
) -> None:
    machine, results = lifecycle
    order_key = results[-1].state.order_key
    assert order_key == "C1"  # the D created the chain before any OrderID existed

    for clordid in ("C1", "C2"):
        found = machine.get_by_clordid(clordid)
        assert found is not None and found.order_key == order_key
    by_order_id = machine.get_by_order_id("ORD-1")
    assert by_order_id is not None and by_order_id.order_key == order_key
    for exec_id in EXEC_IDS:
        found = machine.get_by_execid(exec_id)
        assert found is not None and found.order_key == order_key
    assert machine.order_count() == 1


def test_reference_lifecycle_status_progression(
    lifecycle: tuple[OrderStateMachine, list[Result]],
) -> None:
    _, results = lifecycle
    assert [result.state.ord_status for result in results] == [
        OrdStatus.PENDING_NEW,
        OrdStatus.PENDING_NEW,
        OrdStatus.NEW,
        OrdStatus.PARTIALLY_FILLED,
        OrdStatus.PENDING_REPLACE,
        OrdStatus.PENDING_REPLACE,
        OrdStatus.PARTIALLY_FILLED,  # 150=5 with 39=1: tag 39 wins
        OrdStatus.FILLED,
    ]


def test_reference_lifecycle_event_stream(
    lifecycle: tuple[OrderStateMachine, list[Result]],
) -> None:
    _, results = lifecycle
    events = [event for result in results for event in result.events]
    assert [event.event_type for event in events] == [
        EventType.NEW_REQUEST,
        EventType.PENDING_NEW,
        EventType.NEW_ACK,
        EventType.PARTIAL_FILL,
        EventType.AMEND_REQUEST,
        EventType.PENDING_AMEND,
        EventType.AMEND_ACK,
        EventType.FULL_FILL,
    ]
    amend_request = events[4]
    assert amend_request.msg_type == "G"
    assert amend_request.clordid == "C2" and amend_request.orig_clordid == "C1"
    assert amend_request.price == 185.55  # proposed terms, not yet live
    assert all(event.order_key == "C1" for event in events)
    assert all(event.ingest_ts is not None for event in events)


def test_reference_lifecycle_execution_stream(
    lifecycle: tuple[OrderStateMachine, list[Result]],
) -> None:
    _, results = lifecycle
    executions = [row for result in results for row in result.executions]

    assert [row.exec_id for row in executions] == EXEC_IDS
    assert [row.is_fill for row in executions] == [False, False, True, False, False, True]
    assert all(row.fill_status == FillStatus.NORMAL for row in executions)
    assert all(row.order_key == "C1" for row in executions)

    partial = executions[2]
    assert partial.exec_type is ExecType.PARTIAL_FILL
    assert partial.last_shares == 400.0 and partial.last_px == 185.48
    assert partial.cum_qty == 400.0 and partial.leaves_qty == 600.0
    assert partial.to_row()["OrdStatus"] == "PARTIALLY_FILLED"

    final = executions[-1]
    assert final.cum_qty == 1000.0 and final.leaves_qty == 0.0 and final.avg_px == 185.522
    assert final.clordid == "C2"
    assert final.transact_time is not None


def test_reference_lifecycle_message_audit_rows(
    lifecycle: tuple[OrderStateMachine, list[Result]],
) -> None:
    _, results = lifecycle
    messages = [result.message for result in results]

    assert [message.msg_type for message in messages] == ["D", "8", "8", "8", "G", "8", "8", "8"]
    assert all(message.order_key == "C1" for message in messages)
    assert all(message.checksum_ok is True for message in messages)
    assert [message.seq_num for message in messages] == [1, 2, 3, 4, 5, 6, 7, 8]
    assert all("\x01" not in message.raw_fix for message in messages)
    assert messages[0].raw_fix.startswith("8=FIX.4.2|")
    assert messages[-1].to_row()["CumQty"] == 1000.0


def test_reference_lifecycle_state_snapshots_are_independent_copies(
    lifecycle: tuple[OrderStateMachine, list[Result]],
) -> None:
    machine, results = lifecycle
    early = results[3].state
    assert early is not None
    assert early.cum_qty == 400.0  # not mutated by the later full fill

    early.cum_qty = -1.0
    early.clordid_chain.append("MUTATED")
    live = machine.get_by_clordid("C1")
    assert live is not None
    assert live.cum_qty == 1000.0
    assert live.clordid_chain == ["C1", "C2"]
