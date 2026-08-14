"""The 12 edge cases enumerated in doc 01 §7 -- one named test each."""

from __future__ import annotations

from fixhelpers import (
    cancel_reject,
    cancel_request,
    dk_trade,
    exec_report,
    new_order,
    replace_request,
)

from fix42cache import OrderStateMachine
from fix42cache.fixtags import OrdStatus
from fix42cache.model import FillStatus, PendingAction


def test_edge_case_01_order_id_absent_until_first_execution_report(
    machine: OrderStateMachine,
) -> None:
    """Searches by ClOrdID work in the window before the venue assigns 37."""
    machine.process(new_order("C1"))

    assert machine.get_by_order_id("ORD-1") is None
    by_clordid = machine.get_by_clordid("C1")
    assert by_clordid is not None
    assert by_clordid.order_key == "C1" and by_clordid.order_id == ""

    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))

    healed = machine.get_by_order_id("ORD-1")
    assert healed is not None
    assert healed.order_key == "C1" and healed.order_id == "ORD-1"


def test_edge_case_02_amend_chain_c1_c2_c3_shares_one_order_key(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    machine.process(replace_request("C2", "C1", qty=1000, price=185.55))
    machine.process(
        exec_report("5", exec_id="E1", clordid="C2", orig_clordid="C1", ord_status="0",
                    cum_qty=0, leaves_qty=1000)
    )
    machine.process(replace_request("C3", "C2", qty=800, price=185.60))
    result = machine.process(
        exec_report("5", exec_id="E2", clordid="C3", orig_clordid="C2", ord_status="0",
                    cum_qty=0, leaves_qty=800)
    )

    state = result.state
    assert state is not None
    assert state.clordid == "C3" and state.root_clordid == "C1"
    assert state.to_row()["ClOrdIDChain"] == "C1,C2,C3"
    assert state.order_qty == 800.0 and state.price == 185.60
    for identifier in ("C1", "C2", "C3"):
        found = machine.get_by_clordid(identifier)
        assert found is not None and found.order_key == "C1"
    assert machine.order_count() == 1


def test_edge_case_03_duplicate_execid_replay_does_not_double_count(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    fill = exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                       cum_qty=400, leaves_qty=600, avg_px=185.48)
    machine.process(fill)
    machine.process(fill)
    machine.process(fill)

    state = machine.get_by_execid("E1")
    assert state is not None
    assert state.cum_qty == 400.0 and state.leaves_qty == 600.0
    assert state.exec_count == 2
    assert state.msg_count == 5


def test_edge_case_04_two_in_flight_requests_reject_reverts_only_the_cancel(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    machine.process(replace_request("C2", "C1", qty=1500, price=185.55))
    machine.process(cancel_request("C3", "C1"))
    result = machine.process(
        cancel_reject("C3", "C1", response_to="1", cxl_rej_reason="0", text="too late")
    )

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.PENDING_REPLACE
    assert state.pending_action == PendingAction.REPLACE
    assert state.pending_clordid == "C2"

    # The surviving replace still applies its staged terms when confirmed.
    confirmed = machine.process(
        exec_report("5", exec_id="E1", clordid="C2", orig_clordid="C1", ord_status="0",
                    cum_qty=0, leaves_qty=1500)
    )
    assert confirmed.state is not None
    assert confirmed.state.order_qty == 1500.0 and confirmed.state.price == 185.55
    assert confirmed.state.pending_action == PendingAction.NONE


def test_edge_case_05_reject_before_ack_is_terminal_with_no_new(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    result = machine.process(
        exec_report("8", exec_id="E0", ord_rej_reason="1", text="unknown symbol",
                    cum_qty=0, leaves_qty=0, avg_px=0)
    )

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.REJECTED
    assert state.terminal is True
    assert state.to_row()["LastExecType"] == "REJECTED"
    assert state.ord_rej_reason == "1"
    assert state.pending_action == PendingAction.NONE


def test_edge_case_06_bust_after_full_fill_reopens_partially_filled(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    machine.process(
        exec_report("2", exec_id="E2", last_shares=600, last_px=185.55,
                    cum_qty=1000, leaves_qty=0, avg_px=185.522)
    )
    assert machine.get_by_clordid("C1").ord_status is OrdStatus.FILLED

    result = machine.process(
        exec_report("D", exec_id="E3", exec_trans_type="1", exec_ref_id="E2",
                    ord_status="1", cum_qty=400, leaves_qty=600, avg_px=185.48)
    )

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.PARTIALLY_FILLED
    assert state.terminal is False
    assert (state.cum_qty, state.leaves_qty, state.avg_px) == (400.0, 600.0, 185.48)
    busted = [row for row in result.executions if row.exec_id == "E2"]
    assert len(busted) == 1 and busted[0].fill_status == FillStatus.BUSTED


def test_edge_case_07_correct_changes_price_only(machine: OrderStateMachine) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    result = machine.process(
        exec_report("D", exec_id="E2", exec_trans_type="2", exec_ref_id="E1",
                    ord_status="1", last_shares=400, last_px=185.52,
                    cum_qty=400, leaves_qty=600, avg_px=185.52)
    )

    state = result.state
    assert state is not None
    assert state.cum_qty == 400.0 and state.leaves_qty == 600.0
    assert state.avg_px == 185.52
    corrected = [row for row in result.executions if row.exec_id == "E1"]
    assert len(corrected) == 1
    assert corrected[0].fill_status == FillStatus.CORRECTED
    assert corrected[0].last_px == 185.52 and corrected[0].last_shares == 400.0


def test_edge_case_08_dk_on_unknown_execid_but_known_order_id(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    before = machine.get_by_clordid("C1")
    result = machine.process(dk_trade("E-UNKNOWN", order_id="ORD-1", dk_reason="B"))

    state = result.state
    assert before is not None and state is not None
    assert state.order_key == "C1"
    assert state.dk_reason == "B"
    assert (state.cum_qty, state.leaves_qty, state.avg_px) == (
        before.cum_qty, before.leaves_qty, before.avg_px
    )
    assert state.ord_status is before.ord_status
    assert state.exec_count == before.exec_count  # a DK is not an execution
    assert len(result.executions) == 1
    assert result.executions[0].exec_id == "E-UNKNOWN"
    assert result.executions[0].fill_status == FillStatus.DK
    attached = machine.get_by_execid("E-UNKNOWN")
    assert attached is not None and attached.order_key == "C1"


def test_edge_case_09_execution_before_new_order_then_late_d_merges(
    machine: OrderStateMachine,
) -> None:
    machine.process(
        exec_report("0", exec_id="E0", order_id="ORD-1", clordid="C1", ord_status="0",
                    symbol="IBM", side="1", qty=1000, cum_qty=0, leaves_qty=1000, avg_px=0)
    )
    created = machine.get_by_order_id("ORD-1")
    assert created is not None and created.order_key == "ORD-1"
    assert created.ord_status is OrdStatus.NEW

    result = machine.process(new_order("C1", account="ACC9", qty=1000, price=185.50))

    state = result.state
    assert state is not None
    assert state.order_key == "ORD-1"  # venue key wins, late D does not re-key
    assert state.ord_status is OrdStatus.NEW  # status untouched
    assert state.pending_action == PendingAction.NONE  # no PENDING_NEW resurrection
    assert state.price == 185.50  # empty term filled from the D
    assert state.account == "ACC9"
    assert state.root_clordid == "C1" and state.clordid == "C1"
    assert (state.cum_qty, state.leaves_qty) == (0.0, 1000.0)
    assert machine.order_count() == 1


def test_edge_case_10_fill_while_pending_cancel_applies_quantities(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    machine.process(cancel_request("C2", "C1"))
    result = machine.process(
        exec_report("1", exec_id="E1", ord_status="1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )

    state = result.state
    assert state is not None
    assert (state.cum_qty, state.leaves_qty, state.avg_px) == (400.0, 600.0, 185.48)
    assert state.ord_status is OrdStatus.PARTIALLY_FILLED  # venue truth via tag 39
    assert state.pending_action == PendingAction.CANCEL
    assert state.pending_clordid == "C2"

    cancelled = machine.process(
        exec_report("4", exec_id="E2", clordid="C2", orig_clordid="C1", ord_status="4",
                    cum_qty=400, leaves_qty=0, avg_px=185.48)
    )
    assert cancelled.state is not None
    assert cancelled.state.pending_action == PendingAction.NONE
    assert cancelled.state.ord_status is OrdStatus.CANCELED


def test_edge_case_10b_fill_while_pending_replace_keeps_the_request(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    machine.process(replace_request("C2", "C1", qty=1500, price=185.55))
    result = machine.process(
        exec_report("1", exec_id="E1", ord_status="1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )

    state = result.state
    assert state is not None
    assert state.cum_qty == 400.0
    assert state.ord_status is OrdStatus.PARTIALLY_FILLED
    assert state.pending_action == PendingAction.REPLACE
    assert state.order_qty == 1000.0  # staged terms still not applied


def test_edge_case_11_unsolicited_cancel_is_accepted(machine: OrderStateMachine) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    result = machine.process(
        exec_report("4", exec_id="E1", ord_status="4", cum_qty=0, leaves_qty=0, avg_px=0)
    )

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.CANCELED
    assert state.terminal is True
    assert state.pending_action == PendingAction.NONE


def test_edge_case_12_stale_lower_cum_qty_report_is_ignored_economically(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    machine.process(
        exec_report("2", exec_id="E2", last_shares=600, last_px=185.55,
                    cum_qty=1000, leaves_qty=0, avg_px=185.522)
    )
    before = machine.get_by_clordid("C1")
    result = machine.process(
        exec_report("1", exec_id="E3", ord_status="1", last_shares=100, last_px=180.00,
                    cum_qty=100, leaves_qty=900, avg_px=180.00)
    )

    state = result.state
    assert before is not None and state is not None
    assert (state.cum_qty, state.leaves_qty, state.avg_px) == (1000.0, 0.0, 185.522)
    assert (state.last_shares, state.last_px) == (600.0, 185.55)
    assert state.msg_count == before.msg_count + 1
    assert state.exec_count == before.exec_count + 1  # the ExecID is still recorded
