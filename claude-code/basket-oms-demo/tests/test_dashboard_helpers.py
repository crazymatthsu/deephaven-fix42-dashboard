"""The dashboard's pure helpers: payload extraction, per-status actions, coercion, split preview."""

from __future__ import annotations

import pytest

from basket_oms_demo.dashboard import (
    actions_for,
    cell_value,
    coerce_float,
    coerce_int,
    parse_qty_list,
    row_key,
    row_to_dict,
    split_preview,
)


def test_cell_value_unwraps_payload_cells():
    assert cell_value({"value": "O-000001", "text": "O-000001", "type": "java.lang.String"}) == "O-000001"
    assert cell_value({"value": None, "text": "null"}) == "null"
    assert cell_value({"type": "double"}) is None
    assert cell_value(42) == 42 and cell_value("x") == "x"


def test_row_to_dict_and_row_key():
    row = {"OrderId": {"value": "O-000007", "text": "O-000007"}, "Qty": {"value": 100, "text": "100"}, "Venue": {"value": None}}
    assert row_to_dict(row) == {"OrderId": "O-000007", "Qty": 100, "Venue": None}
    assert row_key(row, "OrderId") == "O-000007"
    assert row_key(row, "Venue") is None
    assert row_key(row, "Missing") is None
    assert row_key(None, "OrderId") is None
    assert row_key({"BasketId": "B-0001"}, "BasketId") == "B-0001"  # bare values too


def test_row_to_dict_from_object_attributes():
    class Row:
        OrderId = "O-1"
        Qty = {"value": 5}

    assert row_to_dict(Row())["OrderId"] == "O-1"
    assert row_to_dict(Row())["Qty"] == 5


@pytest.mark.parametrize(
    "status, qty, expected",
    [
        ("NEW", 100, ["modify", "route", "split", "cancel"]),
        ("NEW", 1, ["modify", "route", "cancel"]),
        ("ROUTED", 100, ["modify", "cancel"]),
        ("WORKING", 100, ["modify", "cancel"]),
        ("PARTIAL", 100, ["modify", "cancel"]),
        ("FILLED", 100, []),
        ("CANCELLED", 100, []),
        ("REJECTED", 100, []),
        ("SPLIT", 100, []),
    ],
)
def test_actions_for_status(status, qty, expected):
    assert actions_for(status, qty) == expected


def test_coercions():
    assert coerce_int("2,500".replace(",", "")) == 2500
    assert coerce_int("2500.0") == 2500
    assert coerce_int("") is None and coerce_int(None, 7) == 7 and coerce_int("abc") is None
    assert coerce_float("189.50") == 189.5 and coerce_float("x", 1.0) == 1.0 and coerce_float(None) is None


def test_parse_qty_list():
    assert parse_qty_list("600, 400") == [600, 400]
    assert parse_qty_list("600 400; 200") == [600, 400, 200]
    assert parse_qty_list("") == []
    with pytest.raises(Exception, match="whole number"):
        parse_qty_list("600, four hundred")


def test_split_preview_by_count_and_by_list():
    assert split_preview(1000, 3) == ([334, 333, 333], None)
    assert split_preview(1000, None, "600, 400") == ([600, 400], None)
    sizes, err = split_preview(1000, None, "600, 500")
    assert sizes == [] and "sum to 1,100" in err
    assert split_preview(1000, None, "1000")[1] == "give at least two quantities"
    assert split_preview(1000, None, "1000, 0")[1] == "every slice must be > 0"
    assert split_preview(1000, None, "")[1] == "give a slice count or a list of quantities"
    assert split_preview(2, 3)[1] == "cannot split 2 shares into 3 slices"
    assert split_preview(1000, None, "abc")[1] == "'abc' is not a whole number"
