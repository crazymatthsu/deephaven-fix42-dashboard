"""Blotter filter construction and id sanitizing -- doc 09 sections 6 and 7.

``multi_oms.dashboard`` and ``multi_oms.query_api`` import ``deephaven`` nowhere at
module scope (``deephaven.ui`` is imported lazily inside :func:`build_dashboard`),
so the pure filter/sanitizing layer is unit-testable on a bare host python.
"""

from __future__ import annotations

import pytest

from multi_oms import dashboard
from multi_oms.config import parse_topology
from multi_oms.dashboard import blotter_filters, hub_filter, search_filter
from multi_oms.query_api import sanitize_id

HUBS = parse_topology(None).names


# --------------------------------------------------------------------------------------
# sanitize_id
# --------------------------------------------------------------------------------------


def test_sanitize_id_passes_ordinary_identifiers_through():
    assert sanitize_id("ORD-123_x") == "ORD-123_x"


@pytest.mark.parametrize("value", ["a`b", 'a"b', "a'b", "a\\b", "a\nb", "a\tb"])
def test_sanitize_id_strips_quoting_and_control_characters(value):
    cleaned = sanitize_id(value)
    assert cleaned == "ab"


def test_sanitize_id_trims_and_handles_none():
    assert sanitize_id("  x  ") == "x"
    assert sanitize_id(None) == ""
    assert sanitize_id(42) == "42"


def test_sanitize_id_cannot_escape_a_backtick_literal():
    hostile = "x` || true || `y"
    assert "`" not in sanitize_id(hostile)


# --------------------------------------------------------------------------------------
# hub_filter
# --------------------------------------------------------------------------------------


def test_all_hubs_selected_needs_no_filter():
    assert hub_filter(HUBS, {name: True for name in HUBS}) == ""


def test_missing_hub_entries_count_as_selected():
    assert hub_filter(HUBS, {}) == ""


def test_no_hub_selected_returns_none():
    assert hub_filter(HUBS, {name: False for name in HUBS}) is None


def test_a_subset_builds_an_or_clause():
    clause = hub_filter(HUBS, {"OMS-A": True, "OMS-B-parent": True, "OMS-B-child": False, "OMS-C": False})
    assert clause == "(Oms == `OMS-A` || Oms == `OMS-B-parent`)"


def test_single_hub_selection():
    on = {name: name == "OMS-C" for name in HUBS}
    assert hub_filter(HUBS, on) == "(Oms == `OMS-C`)"


# --------------------------------------------------------------------------------------
# search_filter
# --------------------------------------------------------------------------------------


def test_blank_search_produces_no_clause():
    assert search_filter("") == ""
    assert search_filter(None) == ""
    assert search_filter("   ") == ""


def test_search_covers_every_id_column():
    clause = search_filter("ABC")
    for column in dashboard.SEARCH_COLUMNS:
        assert f"{column}.contains(`ABC`)" in clause
    assert clause.count("||") == len(dashboard.SEARCH_COLUMNS) - 1


def test_search_text_is_sanitized():
    # The clause legitimately contains backticks (they delimit the literal); what
    # must not survive is a backtick coming from the *needle*, which would close the
    # literal early and let the rest of the text compile as query language.
    assert search_filter("A`B") == search_filter("AB")
    assert search_filter("x` || true || `y").count("`") == search_filter("xtruey").count("`")


# --------------------------------------------------------------------------------------
# blotter_filters
# --------------------------------------------------------------------------------------


def test_no_filters_at_all():
    assert blotter_filters(HUBS) == []


def test_every_filter_combines():
    filters = blotter_filters(
        HUBS,
        account="ACC1",
        symbol="AAPL",
        side="BUY",
        hubs_on={"OMS-A": True, "OMS-B-parent": False, "OMS-B-child": False, "OMS-C": False},
        breaks_only=True,
        search="X1",
    )
    assert "Account == `ACC1`" in filters
    assert "Symbol == `AAPL`" in filters
    assert "Side == `BUY`" in filters
    assert "(Oms == `OMS-A`)" in filters
    assert "BreakKind != `NONE` || OnBrokenEdge" in filters
    assert any("ClOrdID.contains(`X1`)" in clause for clause in filters)
    assert len(filters) == 6


def test_none_and_blank_values_are_skipped():
    assert blotter_filters(HUBS, account=None, symbol="", side="   ", search=None) == []


def test_breaks_only_shows_both_ends_of_a_broken_edge():
    filters = blotter_filters(HUBS, breaks_only=True)
    # OnBrokenEdge is what keeps the *healthy child* of a broken parent on screen.
    assert filters == ["BreakKind != `NONE` || OnBrokenEdge"]


def test_no_source_system_selected_returns_none():
    assert blotter_filters(HUBS, hubs_on={name: False for name in HUBS}) is None


def test_no_source_system_selected_wins_over_other_filters():
    assert (
        blotter_filters(HUBS, account="ACC1", hubs_on={name: False for name in HUBS}) is None
    )


def test_filter_values_are_sanitized():
    filters = blotter_filters(HUBS, account="A`CC")
    assert filters == ["Account == `ACC`"]


# --------------------------------------------------------------------------------------
# Column contracts
# --------------------------------------------------------------------------------------


def test_blotter_view_carries_the_selection_keys():
    # The row-press handler reads both out of the pressed row, so they must survive
    # the display `view`.
    assert "GlobalKey" in dashboard.BLOTTER_COLUMNS
    assert "RootKey" in dashboard.BLOTTER_COLUMNS


def test_blotter_columns_are_unique():
    assert len(set(dashboard.BLOTTER_COLUMNS)) == len(dashboard.BLOTTER_COLUMNS)


def test_red_break_kinds_match_the_taxonomy():
    assert dashboard.RED_BREAK_KINDS == ("QTY_BREAK", "NOTIONAL_BREAK", "DANGLING", "NO_LINK")
    assert "UNROUTED" not in dashboard.RED_BREAK_KINDS


# --------------------------------------------------------------------------------------
# Defensive on_row_press extraction
# --------------------------------------------------------------------------------------


def test_row_press_reads_both_keys_from_a_flat_payload():
    captured = []
    handler = dashboard._selection_handler(captured.append)
    handler({"GlobalKey": "OMS-A|K1", "RootKey": "OMS-A|K1"})
    assert captured == [("OMS-A|K1", "OMS-A|K1")]


def test_row_press_reads_the_nested_cell_shape():
    captured = []
    handler = dashboard._selection_handler(captured.append)
    handler({"GlobalKey": {"value": "OMS-C|K9"}, "RootKey": {"value": "OMS-A|K1"}})
    assert captured == [("OMS-C|K9", "OMS-A|K1")]


def test_row_press_accepts_the_index_plus_row_form_and_keywords():
    captured = []
    handler = dashboard._selection_handler(captured.append)
    handler(3, {"GlobalKey": "g", "RootKey": "r"})
    handler(row={"GlobalKey": "g2", "RootKey": "r2"})
    assert captured == [("g", "r"), ("g2", "r2")]


def test_row_press_ignores_an_unusable_payload():
    captured = []
    handler = dashboard._selection_handler(captured.append)
    handler()
    handler(None)
    handler({"Other": "x"})
    assert captured == []


def test_safe_swallows_a_failing_factory():
    assert dashboard._safe(lambda: 1 / 0) is None
    assert dashboard._safe(lambda: "ok") == "ok"


def test_first_returns_the_first_working_factory():
    def boom():
        raise RuntimeError("nope")

    assert dashboard._first(boom, lambda: "second") == "second"
    assert dashboard._first(boom, boom) is None
