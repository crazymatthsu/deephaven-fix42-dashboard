"""Lookups, error handling and the row-emission contract (doc 01 §3/§6, doc 05 §3)."""

from __future__ import annotations

from fixhelpers import build_fix, cancel_reject, dk_trade, exec_report, new_order

from fix42cache import OrderStateMachine
from fix42cache.model import EXECUTION_COLUMNS, MESSAGE_COLUMNS, ORDER_STATE_COLUMNS, FillStatus
from fix42cache.parser import SOH, parse_fix


def _two_orders(machine: OrderStateMachine) -> None:
    machine.process(new_order("C1", account="ACC1", symbol="IBM"))
    machine.process(
        exec_report("0", exec_id="E1", clordid="C1", order_id="ORD-1",
                    cum_qty=0, leaves_qty=1000)
    )
    machine.process(new_order("D1", account="ACC2", symbol="MSFT"))
    machine.process(
        exec_report("0", exec_id="E2", clordid="D1", order_id="ORD-2",
                    symbol="MSFT", cum_qty=0, leaves_qty=1000)
    )


# --------------------------------------------------------------------------- #
# lookups
# --------------------------------------------------------------------------- #


def test_lookups_by_every_identifier(machine: OrderStateMachine) -> None:
    _two_orders(machine)

    assert machine.get_by_clordid("C1").order_key == "C1"
    assert machine.get_by_order_id("ORD-1").order_key == "C1"
    assert machine.get_by_execid("E1").order_key == "C1"
    assert machine.get_by_key("C1").order_key == "C1"
    assert machine.get_by_order_id("ORD-2").order_key == "D1"


def test_lookups_return_none_for_unknown_identifiers(machine: OrderStateMachine) -> None:
    _two_orders(machine)

    assert machine.get_by_clordid("NOPE") is None
    assert machine.get_by_order_id("NOPE") is None
    assert machine.get_by_execid("NOPE") is None
    assert machine.get_by_key("") is None


def test_lookups_return_snapshots_not_live_objects(machine: OrderStateMachine) -> None:
    _two_orders(machine)
    first = machine.get_by_clordid("C1")
    first.cum_qty = 999.0
    assert machine.get_by_clordid("C1").cum_qty == 0.0


def test_find_by_account_and_symbol(machine: OrderStateMachine) -> None:
    _two_orders(machine)

    assert [state.order_key for state in machine.find_by_account("ACC1")] == ["C1"]
    assert [state.order_key for state in machine.find_by_account("ACC2")] == ["D1"]
    assert machine.find_by_account("ACC3") == []
    assert [state.order_key for state in machine.find_by_symbol("IBM")] == ["C1"]
    assert [state.order_key for state in machine.find_by_symbol("MSFT")] == ["D1"]


def test_order_count_and_snapshot_all(machine: OrderStateMachine) -> None:
    assert machine.order_count() == 0
    _two_orders(machine)
    assert machine.order_count() == 2
    assert sorted(state.order_key for state in machine.snapshot_all()) == ["C1", "D1"]


def test_binding_is_idempotent_across_replays(machine: OrderStateMachine) -> None:
    raw_new = new_order("C1")
    raw_ack = exec_report("0", exec_id="E1", clordid="C1", cum_qty=0, leaves_qty=1000)
    for _ in range(3):
        machine.process(raw_new)
        machine.process(raw_ack)

    assert machine.order_count() == 1
    assert machine.key_by_clordid == {"C1": "C1"}
    assert machine.key_by_order_id == {"ORD-1": "C1"}
    assert machine.key_by_execid == {"E1": "C1"}


def test_order_id_none_sentinel_is_never_bound(machine: OrderStateMachine) -> None:
    """A `9` may carry 37=NONE when the target was never acked (doc 01 §2)."""
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E1", cum_qty=0, leaves_qty=1000))
    result = machine.process(
        cancel_reject("C2", "C1", response_to="1", order_id="NONE", cxl_rej_reason="1")
    )

    assert result.error is None
    assert result.state.order_key == "C1"
    assert result.state.order_id == "ORD-1"
    assert "NONE" not in machine.key_by_order_id


# --------------------------------------------------------------------------- #
# errors -- process() never raises
# --------------------------------------------------------------------------- #


def test_unknown_msg_type_sets_error_and_changes_nothing(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    before = machine.get_by_clordid("C1")
    result = machine.process(build_fix("V", {11: "C1", 55: "IBM"}))

    assert result.error is not None and "MsgType" in result.error
    assert result.state is None
    assert result.executions == [] and result.events == []
    assert result.message is not None
    assert result.message.order_key == "C1"  # still attributed for the audit table
    assert result.message.msg_type == "V"
    after = machine.get_by_clordid("C1")
    assert after.msg_count == before.msg_count


def test_missing_msg_type_sets_error(machine: OrderStateMachine) -> None:
    result = machine.process("11=C1|55=IBM|")
    assert result.error is not None and "MsgType" in result.error
    assert result.state is None
    assert result.message is not None and result.message.msg_type == ""


def test_unparseable_input_sets_error_without_raising(machine: OrderStateMachine) -> None:
    for raw in ("", "not a fix message", "||||", "\x01\x01"):
        result = machine.process(raw)
        assert result.error is not None
        assert result.state is None and result.message is None


def test_unresolvable_message_still_produces_an_audit_row(
    machine: OrderStateMachine,
) -> None:
    result = machine.process(dk_trade("E-UNKNOWN", order_id=None))

    assert result.error is not None and "unresolvable" in result.error
    assert result.state is None
    assert result.message is not None
    assert result.message.order_key == ""
    assert result.message.msg_type == "Q"
    assert machine.order_count() == 0


def test_process_never_raises_on_malformed_values(machine: OrderStateMachine) -> None:
    machine.process(new_order("C1"))
    result = machine.process(
        build_fix("8", {37: "ORD-1", 11: "C1", 17: "E1", 150: "0", 39: "0",
                        14: "not-a-number", 151: "", 6: "x", 60: "garbage"})
    )

    assert result.error is None
    assert result.state is not None
    assert result.state.cum_qty == 0.0
    assert result.message.transact_time is None


def test_unknown_enum_codes_degrade_to_unknown(machine: OrderStateMachine) -> None:
    machine.process(new_order("C1"))
    result = machine.process(
        build_fix("8", {37: "ORD-1", 11: "C1", 17: "E1", 150: "Z", 39: "Z",
                        14: 0, 151: 1000, 6: 0})
    )

    assert result.error is None
    assert result.state.to_row()["OrdStatus"] == "UNKNOWN"
    assert result.state.to_row()["LastExecType"] == "UNKNOWN"
    assert result.events[0].event_type == "STATUS"


def test_process_fields_accepts_pre_parsed_input(machine: OrderStateMachine) -> None:
    raw = new_order("C1")
    result = machine.process_fields(parse_fix(raw))

    assert result.error is None
    assert result.state.order_key == "C1"
    assert result.message is not None
    assert result.message.raw_fix.startswith("8=FIX.4.2|")
    assert result.message.ingest_ts is not None


def test_soh_and_pipe_delimited_inputs_behave_identically(
    machine: OrderStateMachine,
) -> None:
    soh_machine = OrderStateMachine()
    pipe_machine = OrderStateMachine()
    soh_result = soh_machine.process(new_order("C1", delimiter=SOH))
    pipe_result = pipe_machine.process(new_order("C1"))

    soh_row = soh_result.state.to_row()
    pipe_row = pipe_result.state.to_row()
    for column in ORDER_STATE_COLUMNS:
        if column in ("FirstSeenTs", "LastUpdateTs"):
            continue
        assert soh_row[column] == pipe_row[column]
    assert soh_result.message.raw_fix == pipe_result.message.raw_fix


# --------------------------------------------------------------------------- #
# emission contract
# --------------------------------------------------------------------------- #


def test_every_message_produces_exactly_one_audit_row(machine: OrderStateMachine) -> None:
    results = [
        machine.process(new_order("C1")),
        machine.process(exec_report("0", exec_id="E1", cum_qty=0, leaves_qty=1000)),
        machine.process(dk_trade("E1")),
    ]
    for result in results:
        assert result.message is not None
        assert list(result.message.to_row()) == list(MESSAGE_COLUMNS)


def test_requests_emit_no_execution_rows(machine: OrderStateMachine) -> None:
    result = machine.process(new_order("C1"))
    assert result.executions == []


def test_execution_and_dk_rows_use_the_frozen_columns(machine: OrderStateMachine) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E1", cum_qty=0, leaves_qty=1000))
    dk = machine.process(dk_trade("E1"))

    row = dk.executions[0].to_row()
    assert list(row) == list(EXECUTION_COLUMNS)
    assert row["FillStatus"] == FillStatus.DK


def test_bust_reemission_keeps_disposition_across_a_replay(
    machine: OrderStateMachine,
) -> None:
    """A replayed original must not reset a busted ExecID back to NORMAL."""
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    fill = exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                       cum_qty=400, leaves_qty=600, avg_px=185.48)
    machine.process(fill)
    machine.process(
        exec_report("D", exec_id="E2", exec_trans_type="1", exec_ref_id="E1",
                    ord_status="0", cum_qty=0, leaves_qty=1000, avg_px=0)
    )
    replay = machine.process(fill)

    assert len(replay.executions) == 1
    assert replay.executions[0].exec_id == "E1"
    assert replay.executions[0].fill_status == FillStatus.BUSTED


def test_bust_of_an_unseen_execid_still_emits_a_busted_row(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    result = machine.process(
        exec_report("D", exec_id="E9", exec_trans_type="1", exec_ref_id="E-UNSEEN",
                    ord_status="0", cum_qty=0, leaves_qty=1000, avg_px=0)
    )

    assert [row.exec_id for row in result.executions] == ["E9", "E-UNSEEN"]
    assert result.executions[1].fill_status == FillStatus.BUSTED
    assert result.executions[0].fill_status == FillStatus.NORMAL


def test_dk_then_correct_shows_the_latest_disposition(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    machine.process(dk_trade("E1"))
    corrected = machine.process(
        exec_report("D", exec_id="E2", exec_trans_type="2", exec_ref_id="E1",
                    ord_status="1", last_shares=400, last_px=185.49,
                    cum_qty=400, leaves_qty=600, avg_px=185.49)
    )

    reemitted = corrected.executions[1]
    assert reemitted.exec_id == "E1"
    assert reemitted.fill_status == FillStatus.CORRECTED
    assert reemitted.last_px == 185.49


def test_is_fill_is_false_for_bust_and_correct_reports(
    machine: OrderStateMachine,
) -> None:
    machine.process(new_order("C1"))
    machine.process(exec_report("0", exec_id="E0", cum_qty=0, leaves_qty=1000))
    machine.process(
        exec_report("1", exec_id="E1", last_shares=400, last_px=185.48,
                    cum_qty=400, leaves_qty=600, avg_px=185.48)
    )
    bust = machine.process(
        exec_report("1", exec_id="E2", exec_trans_type="1", exec_ref_id="E1",
                    ord_status="0", last_shares=400, last_px=185.48,
                    cum_qty=0, leaves_qty=1000, avg_px=0)
    )

    assert bust.executions[0].is_fill is False  # 150=1 but 20=1
    assert bust.events[0].event_type == "FILL_BUST"  # ExecTransType wins over 150
