"""One test per transition rule in doc 01 §5 (rules 1-7)."""

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
from fix42cache.fixtags import ExecType, OrdStatus
from fix42cache.model import EventType, FillStatus, PendingAction


def _new_and_ack(machine: OrderStateMachine, clordid: str = "C1", qty: float = 1000) -> None:
    """`D` then the venue ack -- the common prelude."""
    machine.process(new_order(clordid, qty=qty))
    machine.process(
        exec_report("A", exec_id="EA", clordid=clordid, cum_qty=0, leaves_qty=qty, avg_px=0)
    )
    machine.process(
        exec_report("0", exec_id="E0", clordid=clordid, cum_qty=0, leaves_qty=qty, avg_px=0)
    )


# --------------------------------------------------------------------------- #
# Rule 1 -- D creates the chain
# --------------------------------------------------------------------------- #


def test_rule1_new_order_creates_chain_and_seeds_terms(machine: OrderStateMachine) -> None:
    result = machine.process(new_order("C1", qty=1000, price=185.50))

    assert result.error is None
    state = result.state
    assert state is not None
    row = state.to_row()
    assert state.order_key == "C1"
    assert row["OrdStatus"] == "PENDING_NEW"
    assert row["PendingAction"] == PendingAction.NEW
    assert row["Account"] == "ACC1" and row["Symbol"] == "IBM"
    assert row["Side"] == "BUY" and row["OrdType"] == "LIMIT" and row["TimeInForce"] == "DAY"
    assert row["OrderQty"] == 1000.0 and row["Price"] == 185.50
    assert row["CumQty"] == 0.0 and row["LeavesQty"] == 1000.0 and row["AvgPx"] == 0.0
    assert row["RootClOrdID"] == "C1" and row["ClOrdID"] == "C1"
    assert row["ClOrdIDChain"] == "C1"
    assert row["OrderID"] == ""
    assert row["MsgCount"] == 1 and row["ExecCount"] == 0
    assert row["LastMsgType"] == "D" and row["Terminal"] is False
    assert state.first_seen_ts is not None and state.last_update_ts is not None


def test_rule1_new_order_emits_new_request_event_with_proposed_terms(
    machine: OrderStateMachine,
) -> None:
    result = machine.process(new_order("C1", qty=1000, price=185.50))

    assert result.executions == []
    assert [event.event_type for event in result.events] == [EventType.NEW_REQUEST]
    event = result.events[0]
    assert event.msg_type == "D"
    assert event.order_qty == 1000.0 and event.price == 185.50
    assert "new order request" in event.detail
    assert event.to_row()["OrdStatus"] == "PENDING_NEW"


# --------------------------------------------------------------------------- #
# Rule 2 -- execution reports
# --------------------------------------------------------------------------- #


def test_rule2_ack_sets_new_and_clears_pending_new(machine: OrderStateMachine) -> None:
    machine.process(new_order("C1"))
    result = machine.process(
        exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000, avg_px=0)
    )

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.NEW
    assert state.last_exec_type is ExecType.NEW
    assert state.order_id == "ORD-1"
    assert state.pending_action == PendingAction.NONE and state.pending_clordid == ""
    assert state.exec_count == 1
    assert [event.event_type for event in result.events] == [EventType.NEW_ACK]
    assert len(result.executions) == 1
    assert result.executions[0].is_fill is False
    assert result.executions[0].fill_status == FillStatus.NORMAL


def test_rule2_pending_new_report_keeps_the_new_request_in_flight(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    result = machine.process(exec_report("A", exec_id="EA", cum_qty=0, leaves_qty=1000))

    assert result.state is not None
    assert result.state.ord_status is OrdStatus.PENDING_NEW
    assert result.state.pending_action == PendingAction.NEW
    assert [event.event_type for event in result.events] == [EventType.PENDING_NEW]


def test_rule2_ordstatus_always_comes_from_tag_39_not_exec_type(
    machine: OrderStateMachine,
) -> None:
    """A replace confirm on a partly filled order stays PARTIALLY_FILLED."""
    _new_and_ack(machine)
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    machine.process(replace_request("C2", "C1", qty=1000, price=185.55))
    result = machine.process(
        exec_report("5", exec_id="E2", clordid="C2", orig_clordid="C1", ord_status="1",
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )

    assert result.state is not None
    assert result.state.ord_status is OrdStatus.PARTIALLY_FILLED
    assert result.state.last_exec_type is ExecType.REPLACED


def test_rule2_adopts_absolute_snapshots_verbatim(machine: OrderStateMachine) -> None:
    _new_and_ack(machine)
    result = machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )

    state = result.state
    assert state is not None
    assert (state.cum_qty, state.leaves_qty, state.avg_px) == (400.0, 600.0, 185.48)


def test_rule2_fill_report_sets_last_shares_px_and_mkt(machine: OrderStateMachine) -> None:
    _new_and_ack(machine)
    result = machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48, last_mkt="XNAS",
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )

    state = result.state
    assert state is not None
    assert (state.last_shares, state.last_px, state.last_mkt) == (400.0, 185.48, "XNAS")
    execution = result.executions[0]
    assert execution.is_fill is True
    assert [event.event_type for event in result.events] == [EventType.PARTIAL_FILL]


def test_rule2_full_fill_emits_full_fill_event(machine: OrderStateMachine) -> None:
    _new_and_ack(machine)
    result = machine.process(
        exec_report("2", exec_id="E1", last_shares=1000, last_px=185.50,
                    cum_qty=1000, leaves_qty=0, avg_px=185.50)
    )

    assert result.state is not None
    assert result.state.ord_status is OrdStatus.FILLED
    assert result.state.terminal is True
    assert [event.event_type for event in result.events] == [EventType.FULL_FILL]


def test_rule2_execid_dedupe_binds_and_counts_but_applies_nothing(
    machine: OrderStateMachine,
) -> None:
    _new_and_ack(machine)
    fill = exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                       cum_qty=400, leaves_qty=600, avg_px=185.48)
    machine.process(fill)
    before = machine.get_by_clordid("C1")
    result = machine.process(fill)

    state = result.state
    assert before is not None and state is not None
    assert state.cum_qty == before.cum_qty == 400.0
    assert state.exec_count == before.exec_count
    assert state.msg_count == before.msg_count + 1
    assert result.events == []
    assert len(result.executions) == 1
    assert result.executions[0].exec_id == "E1"


def test_rule2_reject_is_terminal_and_records_reason(machine: OrderStateMachine) -> None:
    machine.process(new_order("C1"))
    result = machine.process(
        exec_report("8", exec_id="E1", ord_rej_reason="99", text="unknown symbol",
                    cum_qty=0, leaves_qty=0, avg_px=0)
    )

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.REJECTED
    assert state.terminal is True
    assert state.ord_rej_reason == "99" and state.text == "unknown symbol"
    assert state.pending_action == PendingAction.NONE
    assert [event.event_type for event in result.events] == [EventType.NEW_REJECT]
    assert "reject:" in result.events[0].detail


def test_rule2_unsolicited_cancel_is_accepted(machine: OrderStateMachine) -> None:
    _new_and_ack(machine)
    result = machine.process(
        exec_report("4", exec_id="E1", cum_qty=0, leaves_qty=0, avg_px=0)
    )

    assert result.state is not None
    assert result.state.ord_status is OrdStatus.CANCELED
    assert result.state.terminal is True
    assert result.state.pending_action == PendingAction.NONE
    assert [event.event_type for event in result.events] == [EventType.CANCEL_ACK]


def test_rule2_bust_adopts_restated_snapshots_and_marks_the_exec(
    machine: OrderStateMachine,
) -> None:
    _new_and_ack(machine)
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    result = machine.process(
        exec_report("D", exec_id="E2", exec_trans_type="1", exec_ref_id="E1",
                    ord_status="0", cum_qty=0, leaves_qty=1000, avg_px=0)
    )

    state = result.state
    assert state is not None
    assert (state.cum_qty, state.leaves_qty, state.avg_px) == (0.0, 1000.0, 0.0)
    assert state.ord_status is OrdStatus.NEW
    # LastShares/LastPx are untouched by a bust (doc 01 §5.2).
    assert (state.last_shares, state.last_px) == (400.0, 185.48)
    assert [event.event_type for event in result.events] == [EventType.FILL_BUST]
    assert [row.exec_id for row in result.executions] == ["E2", "E1"]
    assert result.executions[1].fill_status == FillStatus.BUSTED
    assert result.executions[1].cum_qty == 0.0


def test_rule2_correct_adopts_restated_snapshots_and_updates_the_exec(
    machine: OrderStateMachine,
) -> None:
    _new_and_ack(machine)
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    result = machine.process(
        exec_report("D", exec_id="E2", exec_trans_type="2", exec_ref_id="E1",
                    ord_status="1", last_shares=400, last_px=185.50,
                    cum_qty=400, leaves_qty=600, avg_px=185.50)
    )

    state = result.state
    assert state is not None
    assert state.cum_qty == 400.0 and state.avg_px == 185.50
    assert [event.event_type for event in result.events] == [EventType.FILL_CORRECT]
    corrected = result.executions[1]
    assert corrected.exec_id == "E1"
    assert corrected.fill_status == FillStatus.CORRECTED
    assert corrected.last_px == 185.50 and corrected.last_shares == 400.0


def test_rule2_stale_report_skips_economic_fields(machine: OrderStateMachine) -> None:
    _new_and_ack(machine)
    machine.process(
        exec_report("2", exec_id="E1", last_shares=1000, last_px=185.50,
                    cum_qty=1000, leaves_qty=0, avg_px=185.50)
    )
    result = machine.process(
        exec_report("1", exec_id="E2", last_shares=400, last_px=185.40,
                    cum_qty=400, leaves_qty=600, avg_px=185.40)
    )

    state = result.state
    assert state is not None
    assert (state.cum_qty, state.leaves_qty, state.avg_px) == (1000.0, 0.0, 185.50)
    assert (state.last_shares, state.last_px) == (1000.0, 185.50)
    assert state.msg_count == 5


def test_rule2_stale_report_still_takes_ordstatus_from_tag_39(
    machine: OrderStateMachine,
) -> None:
    """The stale guard skips *economic* fields; tag 39 stays venue truth (§5.2)."""
    _new_and_ack(machine)
    machine.process(
        exec_report("2", exec_id="E1", cum_qty=1000, leaves_qty=0, avg_px=185.50)
    )
    result = machine.process(
        exec_report("1", exec_id="E2", ord_status="1", cum_qty=400, leaves_qty=600)
    )

    assert result.state is not None
    assert result.state.ord_status is OrdStatus.PARTIALLY_FILLED
    assert result.state.cum_qty == 1000.0


def test_rule2_execution_report_may_create_the_chain(machine: OrderStateMachine) -> None:
    result = machine.process(
        exec_report("0", exec_id="E0", clordid="C1", cum_qty=0, leaves_qty=1000)
    )

    state = result.state
    assert state is not None
    assert state.order_key == "ORD-1"
    assert state.clordid == "C1" and state.root_clordid == "C1"
    assert state.symbol == "IBM" and state.order_qty == 1000.0
    assert machine.order_count() == 1


# --------------------------------------------------------------------------- #
# Rule 3 -- G stages the amend
# --------------------------------------------------------------------------- #


def test_rule3_amend_request_goes_pending_and_stages_terms(
    machine: OrderStateMachine,
) -> None:
    _new_and_ack(machine)
    result = machine.process(replace_request("C2", "C1", qty=1500, price=185.55, tif="1"))

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.PENDING_REPLACE
    assert state.pending_action == PendingAction.REPLACE
    assert state.pending_clordid == "C2"
    # Staged, not applied.
    assert state.order_qty == 1000.0 and state.price == 185.50
    assert state.to_row()["TimeInForce"] == "DAY"
    assert state.clordid == "C1"
    assert state.to_row()["ClOrdIDChain"] == "C1"
    assert state.orig_clordid == "C1"
    assert [event.event_type for event in result.events] == [EventType.AMEND_REQUEST]
    assert result.events[0].order_qty == 1500.0 and result.events[0].price == 185.55


def test_rule3_staged_terms_apply_only_on_replace_confirm(
    machine: OrderStateMachine,
) -> None:
    _new_and_ack(machine)
    machine.process(replace_request("C2", "C1", qty=1500, price=185.55, tif="1"))
    pending = machine.process(
        exec_report("E", exec_id="E1", clordid="C2", orig_clordid="C1",
                    cum_qty=0, leaves_qty=1000)
    )
    assert pending.state is not None
    assert pending.state.order_qty == 1000.0 and pending.state.price == 185.50
    assert pending.state.pending_action == PendingAction.REPLACE
    assert [event.event_type for event in pending.events] == [EventType.PENDING_AMEND]

    result = machine.process(
        exec_report("5", exec_id="E2", clordid="C2", orig_clordid="C1", ord_status="0",
                    cum_qty=0, leaves_qty=1500)
    )

    state = result.state
    assert state is not None
    assert state.order_qty == 1500.0 and state.price == 185.55
    assert state.to_row()["TimeInForce"] == "GTC"
    assert state.clordid == "C2"
    assert state.to_row()["ClOrdIDChain"] == "C1,C2"
    assert state.root_clordid == "C1"
    assert state.pending_action == PendingAction.NONE and state.pending_clordid == ""
    assert [event.event_type for event in result.events] == [EventType.AMEND_ACK]


# --------------------------------------------------------------------------- #
# Rule 4 -- F requests a cancel
# --------------------------------------------------------------------------- #


def test_rule4_cancel_request_goes_pending_cancel(machine: OrderStateMachine) -> None:
    _new_and_ack(machine)
    result = machine.process(cancel_request("C2", "C1"))

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.PENDING_CANCEL
    assert state.pending_action == PendingAction.CANCEL
    assert state.pending_clordid == "C2"
    assert state.clordid == "C1"  # a cancel id never becomes the order identity
    assert state.to_row()["ClOrdIDChain"] == "C1"
    assert [event.event_type for event in result.events] == [EventType.CANCEL_REQUEST]


def test_rule4_cancel_ack_clears_the_pending_cancel(machine: OrderStateMachine) -> None:
    _new_and_ack(machine)
    machine.process(cancel_request("C2", "C1"))
    result = machine.process(
        exec_report("4", exec_id="E1", clordid="C2", orig_clordid="C1",
                    cum_qty=0, leaves_qty=0, avg_px=0)
    )

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.CANCELED and state.terminal is True
    assert state.pending_action == PendingAction.NONE and state.pending_clordid == ""
    assert [event.event_type for event in result.events] == [EventType.CANCEL_ACK]


# --------------------------------------------------------------------------- #
# Rule 5 -- 9 reverts the pending request
# --------------------------------------------------------------------------- #


def test_rule5_cancel_reject_reverts_to_the_snapshotted_status(
    machine: OrderStateMachine,
) -> None:
    _new_and_ack(machine)
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    machine.process(cancel_request("C2", "C1"))
    result = machine.process(
        cancel_reject("C2", "C1", response_to="1", cxl_rej_reason="0",
                      text="too late to cancel")
    )

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.PARTIALLY_FILLED
    assert state.pending_action == PendingAction.NONE and state.pending_clordid == ""
    assert state.cxl_rej_reason == "0" and state.text == "too late to cancel"
    assert [event.event_type for event in result.events] == [EventType.CANCEL_REJECT]
    assert result.events[0].detail == "reject: too late to cancel (102=0)"


def test_rule5_venue_tag_39_on_the_reject_wins_over_the_snapshot(
    machine: OrderStateMachine,
) -> None:
    _new_and_ack(machine)
    machine.process(cancel_request("C2", "C1"))
    result = machine.process(
        cancel_reject("C2", "C1", response_to="1", ord_status="1", cxl_rej_reason="0")
    )

    assert result.state is not None
    assert result.state.ord_status is OrdStatus.PARTIALLY_FILLED


def test_rule5_amend_reject_discards_the_staged_terms(machine: OrderStateMachine) -> None:
    _new_and_ack(machine)
    machine.process(replace_request("C2", "C1", qty=1500, price=185.55))
    result = machine.process(
        cancel_reject("C2", "C1", response_to="2", cxl_rej_reason="2", text="too late")
    )

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.NEW
    assert state.order_qty == 1000.0 and state.price == 185.50
    assert state.clordid == "C1"
    assert state.pending_action == PendingAction.NONE
    assert [event.event_type for event in result.events] == [EventType.AMEND_REJECT]

    # A later replace confirm must not resurrect the discarded terms.
    later = machine.process(
        exec_report("5", exec_id="E9", clordid="C3", orig_clordid="C1", ord_status="0",
                    cum_qty=0, leaves_qty=1000)
    )
    assert later.state is not None
    assert later.state.order_qty == 1000.0 and later.state.price == 185.50


def test_rule5_tag_434_selects_which_pending_flag_clears(
    machine: OrderStateMachine,
) -> None:
    _new_and_ack(machine)
    machine.process(replace_request("C2", "C1", qty=1500, price=185.55))
    machine.process(cancel_request("C3", "C1"))
    result = machine.process(cancel_reject("C3", "C1", response_to="1", cxl_rej_reason="0"))

    state = result.state
    assert state is not None
    assert state.pending_action == PendingAction.REPLACE
    assert state.pending_clordid == "C2"
    assert state.ord_status is OrdStatus.PENDING_REPLACE


# --------------------------------------------------------------------------- #
# Rule 6 -- Q marks an execution disputed
# --------------------------------------------------------------------------- #


def test_rule6_dk_records_reason_without_economic_change(
    machine: OrderStateMachine,
) -> None:
    _new_and_ack(machine)
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    before = machine.get_by_clordid("C1")
    result = machine.process(dk_trade("E1", dk_reason="A", text="unknown order"))

    state = result.state
    assert before is not None and state is not None
    assert (state.cum_qty, state.leaves_qty, state.avg_px) == (
        before.cum_qty,
        before.leaves_qty,
        before.avg_px,
    )
    assert state.ord_status is before.ord_status
    assert state.dk_reason == "A"
    assert [event.event_type for event in result.events] == [EventType.DK_TRADE]
    assert len(result.executions) == 1
    dk_row = result.executions[0]
    assert dk_row.exec_id == "E1"
    assert dk_row.fill_status == FillStatus.DK
    assert dk_row.last_shares == 400.0  # the disputed execution's own values


# --------------------------------------------------------------------------- #
# Rule 7 -- bookkeeping applies to every message type
# --------------------------------------------------------------------------- #


def test_rule7_every_message_binds_ids_counts_and_stamps(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    machine.process(replace_request("C2", "C1", qty=1000, price=185.55))
    machine.process(
        exec_report("5", exec_id="E1", clordid="C2", orig_clordid="C1", ord_status="0",
                    cum_qty=0, leaves_qty=1000)
    )
    machine.process(cancel_request("C3", "C2"))
    result = machine.process(cancel_reject("C3", "C2", response_to="1"))

    state = result.state
    assert state is not None
    assert state.msg_count == 6
    assert state.last_msg_type == "9"
    assert state.last_update_ts is not None
    assert state.first_seen_ts is not None and state.first_seen_ts < state.last_update_ts
    for identifier in ("C1", "C2", "C3"):
        assert machine.key_by_clordid[identifier] == "C1"
    assert machine.key_by_order_id["ORD-1"] == "C1"
    assert machine.key_by_execid["E0"] == "C1"


def test_rule7_terminal_orders_still_accept_late_reports_and_can_reopen(
    machine: OrderStateMachine,
) -> None:
    _new_and_ack(machine)
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    machine.process(
        exec_report("2", exec_id="E2", last_shares=600, last_px=185.55,
                    cum_qty=1000, leaves_qty=0, avg_px=185.522)
    )
    filled = machine.get_by_clordid("C1")
    assert filled is not None and filled.terminal is True

    result = machine.process(
        exec_report("D", exec_id="E3", exec_trans_type="1", exec_ref_id="E2",
                    ord_status="1", cum_qty=400, leaves_qty=600, avg_px=185.48)
    )

    state = result.state
    assert state is not None
    assert state.ord_status is OrdStatus.PARTIALLY_FILLED
    assert state.terminal is False
    assert state.cum_qty == 400.0
