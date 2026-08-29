"""The sticky link map, key building, name sanitizing and row augmentation.

Doc 09 section 4 steps 4-5. Pure python -- no Deephaven, no ``fix42cache``.
"""

from __future__ import annotations

import pytest

from multi_oms.linking import (
    GLOBAL_KEY_SEPARATOR,
    LinkTracker,
    augment_hub_row,
    augment_row,
    augment_state_row,
    global_key,
    sanitize_hub,
)


# --------------------------------------------------------------------------------------
# global_key
# --------------------------------------------------------------------------------------


def test_global_key_joins_hub_and_order_key():
    assert global_key("OMS-A", "K1") == "OMS-A|K1"
    assert GLOBAL_KEY_SEPARATOR == "|"


def test_global_key_is_unique_per_hub():
    assert global_key("OMS-A", "K1") != global_key("OMS-B", "K1")


def test_global_key_tolerates_none_and_non_strings():
    assert global_key(None, None) == "|"
    assert global_key("OMS-A", 7) == "OMS-A|7"


# --------------------------------------------------------------------------------------
# sanitize_hub
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,expected",
    [
        ("OMS-A", "OMS_A"),
        ("OMS-B-parent", "OMS_B_parent"),
        ("OMS-B-child", "OMS_B_child"),
        ("OMS-C", "OMS_C"),
        ("already_ok", "already_ok"),
        ("a b.c", "a_b_c"),
        ("hub/1", "hub_1"),
    ],
)
def test_sanitize_hub_maps_to_column_safe_names(name, expected):
    assert sanitize_hub(name) == expected


def test_sanitize_hub_never_returns_an_illegal_identifier():
    assert sanitize_hub("") == "_"
    assert sanitize_hub("---") == "___"
    assert sanitize_hub(None) == "_"
    assert sanitize_hub("1hub") == "_1hub"
    assert sanitize_hub("9") == "_9"


def test_sanitized_names_are_valid_python_identifiers():
    for name in ("OMS-A", "OMS-B-parent", "1x", "", "a b"):
        assert sanitize_hub(name).isidentifier()


# --------------------------------------------------------------------------------------
# LinkTracker -- stickiness
# --------------------------------------------------------------------------------------


def test_first_non_empty_value_wins():
    tracker = LinkTracker()
    assert tracker.record("K1", "PARENT-1") == "PARENT-1"
    assert tracker.get("K1") == "PARENT-1"
    assert len(tracker) == 1


def test_a_later_conflicting_value_is_ignored():
    tracker = LinkTracker()
    tracker.record("K1", "PARENT-1")
    assert tracker.record("K1", "PARENT-2") == "PARENT-1"
    assert tracker.get("K1") == "PARENT-1"
    assert tracker.conflicts == 1


def test_a_later_empty_value_does_not_unlink():
    tracker = LinkTracker()
    tracker.record("K1", "PARENT-1")
    assert tracker.record("K1", "") == "PARENT-1"
    assert tracker.record("K1", None) == "PARENT-1"
    assert tracker.conflicts == 0


def test_a_repeated_identical_value_is_not_a_conflict():
    tracker = LinkTracker()
    tracker.record("K1", "PARENT-1")
    tracker.record("K1", "PARENT-1")
    assert tracker.conflicts == 0


def test_an_empty_first_value_leaves_the_chain_unlinked_until_one_arrives():
    tracker = LinkTracker()
    assert tracker.record("K1", "") == ""
    assert tracker.get("K1") == ""
    assert "K1" not in tracker
    assert len(tracker) == 0
    assert tracker.record("K1", "PARENT-1") == "PARENT-1"
    assert "K1" in tracker


def test_values_are_stripped():
    tracker = LinkTracker()
    assert tracker.record("K1", "  PARENT-1  ") == "PARENT-1"


def test_whitespace_only_value_counts_as_absent():
    tracker = LinkTracker()
    assert tracker.record("K1", "   ") == ""
    assert len(tracker) == 0


def test_blank_order_key_is_never_recorded():
    tracker = LinkTracker()
    assert tracker.record("", "PARENT-1") == ""
    assert tracker.record(None, "PARENT-1") == ""
    assert len(tracker) == 0


def test_unknown_order_key_reads_as_empty():
    assert LinkTracker().get("nope") == ""


def test_chains_are_independent():
    tracker = LinkTracker()
    tracker.record("K1", "P1")
    tracker.record("K2", "P2")
    assert tracker.links == {"K1": "P1", "K2": "P2"}


def test_links_view_is_a_copy():
    tracker = LinkTracker()
    tracker.record("K1", "P1")
    snapshot = tracker.links
    snapshot["K1"] = "TAMPERED"
    assert tracker.get("K1") == "P1"


def test_order_keys_are_stringified():
    tracker = LinkTracker()
    tracker.record(7, "P1")
    assert tracker.get("7") == "P1"


# --------------------------------------------------------------------------------------
# Row augmentation -- doc 09 section 4.1
# --------------------------------------------------------------------------------------


def test_augment_row_adds_oms_and_global_key_leading():
    row = {"OrderKey": "K1", "CumQty": 100.0}
    out = augment_row(row, "OMS-A")
    assert list(out)[:2] == ["Oms", "GlobalKey"]
    assert out["Oms"] == "OMS-A"
    assert out["GlobalKey"] == "OMS-A|K1"
    assert out["CumQty"] == 100.0
    assert "ExtOrdID" not in out


def test_augment_row_does_not_mutate_the_source():
    row = {"OrderKey": "K1"}
    augment_row(row, "OMS-A")
    assert row == {"OrderKey": "K1"}


def test_augment_state_row_carries_the_sticky_ext_ord_id_leading():
    out = augment_state_row({"OrderKey": "K1", "OrdStatus": "FILLED"}, "OMS-C", "PARENT-1")
    assert list(out)[:3] == ["Oms", "GlobalKey", "ExtOrdID"]
    assert out["ExtOrdID"] == "PARENT-1"
    assert out["OrdStatus"] == "FILLED"


def test_augment_state_row_uses_empty_string_for_an_unlinked_order():
    assert augment_state_row({"OrderKey": "K1"}, "OMS-A", "")["ExtOrdID"] == ""
    assert augment_state_row({"OrderKey": "K1"}, "OMS-A", None)["ExtOrdID"] == ""


def test_augment_hub_row_adds_only_oms():
    out = augment_hub_row({"OrderKey": "K1", "MsgType": "D"}, "OMS-B-parent")
    assert list(out)[0] == "Oms"
    assert out["Oms"] == "OMS-B-parent"
    assert "GlobalKey" not in out


def test_added_columns_win_over_a_colliding_source_key():
    out = augment_row({"OrderKey": "K1", "Oms": "SPOOFED"}, "OMS-A")
    assert out["Oms"] == "OMS-A"


def test_a_row_without_an_order_key_still_builds_a_key():
    out = augment_row({"ExecID": "E1"}, "OMS-A")
    assert out["GlobalKey"] == "OMS-A|"
