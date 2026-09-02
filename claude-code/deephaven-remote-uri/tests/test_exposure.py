"""The frozen exposure formulas and their python reference -- doc 10 section 7.

Two halves of one contract: the query strings the engine compiles, and the reference
implementation the e2e compares the engine's numbers against. A change to one that
is not mirrored in the other fails here, which is the only place that catches a
silent redefinition of "exposure".
"""

from __future__ import annotations

import pytest

from remote_uri import exposure


# --------------------------------------------------------------------------------------
# The frozen formula text
# --------------------------------------------------------------------------------------


def test_the_five_formulas_are_doc_10_section_7_verbatim():
    assert exposure.EXPOSURE_FORMULAS == (
        "ExecNotional = (isNull(AvgPx) ? 0.0 : AvgPx) * (isNull(CumQty) ? 0.0 : CumQty)",
        "MarkPx = isNull(Mid) ? (isNull(Price) ? 0.0 : Price) : Mid",
        "OpenNotional = (isNull(LeavesQty) ? 0.0 : LeavesQty) * MarkPx",
        "TotalNotional = ExecNotional + OpenNotional",
        "SignedExposure = (Side == `BUY` ? 1.0 : -1.0) * TotalNotional",
    )


def test_the_formulas_are_in_dependency_order():
    # update_view evaluates left to right: MarkPx must exist before OpenNotional
    # uses it, and both before TotalNotional.
    defined = []
    for formula in exposure.EXPOSURE_FORMULAS:
        name, _, body = formula.partition(" = ")
        for earlier in exposure.EXPOSURE_COLUMNS:
            if earlier in body:
                assert earlier in defined, f"{name} uses {earlier} before it is defined"
        defined.append(name)
    assert tuple(defined) == exposure.EXPOSURE_COLUMNS


def test_exec_notional_is_doc_09s_notional():
    # Identical arithmetic to multi_oms.dag's `Notional`, so the collector's
    # ExecNotional and the leaves' rx_exposure.Notional cannot disagree.
    assert exposure.EXEC_NOTIONAL_FORMULA.endswith(
        "(isNull(AvgPx) ? 0.0 : AvgPx) * (isNull(CumQty) ? 0.0 : CumQty)"
    )


def test_mark_px_is_never_summed():
    # Summing a price would be meaningless; the aggregate column list must not
    # contain it even though update_view defines it alongside the others.
    assert "MarkPx" in exposure.EXPOSURE_COLUMNS
    assert "MarkPx" not in exposure.EXPOSURE_SUM_COLUMNS


def test_aggregation_groupings_are_frozen():
    assert exposure.LEVEL_BY == ("RootOms", "RootAccount", "RootSymbol", "Oms", "HubDepth")
    assert exposure.LEVEL_SORT == ("RootOms", "RootAccount", "RootSymbol", "HubDepth", "Oms")
    assert exposure.SOURCE_BY == ("RootOms", "RootAccount", "RootSymbol")
    # The totals group by a strict prefix of the per-level grouping: the same rows,
    # one level of detail removed.
    assert exposure.LEVEL_BY[: len(exposure.SOURCE_BY)] == exposure.SOURCE_BY


# --------------------------------------------------------------------------------------
# The reference implementation
# --------------------------------------------------------------------------------------


def test_a_fully_filled_buy():
    order = {
        "AvgPx": 10.0,
        "CumQty": 100.0,
        "LeavesQty": 0.0,
        "Price": 10.5,
        "Side": "BUY",
    }
    marked = exposure.order_exposure(order, mid=11.0)
    assert marked["ExecNotional"] == 1000.0
    assert marked["MarkPx"] == 11.0
    assert marked["OpenNotional"] == 0.0
    assert marked["TotalNotional"] == 1000.0
    assert marked["SignedExposure"] == 1000.0


def test_a_partially_filled_sell_is_negative():
    order = {
        "AvgPx": 10.0,
        "CumQty": 40.0,
        "LeavesQty": 60.0,
        "Price": 9.0,
        "Side": "SELL",
    }
    marked = exposure.order_exposure(order, mid=12.0)
    assert marked["ExecNotional"] == 400.0
    assert marked["OpenNotional"] == 720.0
    assert marked["TotalNotional"] == 1120.0
    assert marked["SignedExposure"] == -1120.0


def test_sell_short_is_negative_too():
    order = {"AvgPx": 1.0, "CumQty": 1.0, "LeavesQty": 0.0, "Side": "SELL_SHORT"}
    assert exposure.order_exposure(order, mid=1.0)["SignedExposure"] == -1.0


def test_an_unquoted_symbol_marks_at_its_own_limit_price():
    order = {"AvgPx": None, "CumQty": None, "LeavesQty": 50.0, "Price": 7.0, "Side": "BUY"}
    marked = exposure.order_exposure(order, mid=None)
    assert marked["ExecNotional"] == 0.0
    assert marked["MarkPx"] == 7.0
    assert marked["OpenNotional"] == 350.0
    assert marked["TotalNotional"] == 350.0


def test_a_market_order_with_no_quote_contributes_zero_open_exposure():
    # Neither a Mid nor a Price: the formula's nested ternary yields 0.0. Reporting
    # zero is the contract; a null would poison every sum it takes part in.
    order = {"AvgPx": 5.0, "CumQty": 2.0, "LeavesQty": 8.0, "Price": None, "Side": "BUY"}
    marked = exposure.order_exposure(order, mid=None)
    assert marked["MarkPx"] == 0.0
    assert marked["OpenNotional"] == 0.0
    assert marked["TotalNotional"] == 10.0


def test_order_exposure_reads_a_mid_carried_on_the_row():
    order = {"AvgPx": 0.0, "CumQty": 0.0, "LeavesQty": 10.0, "Mid": 3.0, "Side": "BUY"}
    assert exposure.order_exposure(order)["OpenNotional"] == 30.0


@pytest.mark.parametrize("value", [None, "", "abc", float("nan"), float("inf")])
def test_as_double_never_produces_a_nan(value):
    assert exposure.as_double(value) == 0.0


def test_sum_exposure_is_the_python_twin_of_the_aggregate():
    orders = [
        {
            "Symbol": "AAPL",
            "AvgPx": 10.0,
            "CumQty": 100.0,
            "LeavesQty": 0.0,
            "OrderQty": 100.0,
            "Price": 10.0,
            "Side": "BUY",
        },
        {
            "Symbol": "AAPL",
            "AvgPx": 0.0,
            "CumQty": 0.0,
            "LeavesQty": 50.0,
            "OrderQty": 50.0,
            "Price": 10.0,
            "Side": "SELL",
        },
    ]
    totals = exposure.sum_exposure(orders, {"AAPL": 20.0})
    assert totals["Orders"] == 2
    assert totals["OrderQty"] == 150.0
    assert totals["CumQty"] == 100.0
    assert totals["LeavesQty"] == 50.0
    assert totals["ExecNotional"] == 1000.0
    assert totals["OpenNotional"] == 1000.0
    assert totals["TotalNotional"] == 2000.0
    # +1000 for the buy, -1000 for the sell: a hedged book nets to zero.
    assert totals["SignedExposure"] == 0.0


def test_sum_exposure_marks_an_unlisted_symbol_at_its_limit_price():
    orders = [
        {"Symbol": "ZZZ", "AvgPx": 0.0, "CumQty": 0.0, "LeavesQty": 2.0, "Price": 4.0, "Side": "BUY"}
    ]
    assert exposure.sum_exposure(orders, {"AAPL": 100.0})["TotalNotional"] == 8.0


def test_sum_exposure_of_nothing_is_all_zeros():
    totals = exposure.sum_exposure([], {})
    assert totals["Orders"] == 0
    assert all(totals[name] == 0.0 for name in exposure.EXPOSURE_SUM_COLUMNS)
