"""The pure filter builders and row-press parsing shared by the API and the dashboard."""

from __future__ import annotations

from order_tree_recon.dashboard import row_bucket
from order_tree_recon.query_api import bucket_filters, node_filters, sanitize_id


def test_no_filter():
    assert node_filters() == []
    assert bucket_filters() == []


def test_exact_lookup_keeps_unlinked_trees_by_default():
    assert node_filters("ACME", "AAPL", "BUY") == [
        "(RootMoniker == `ACME` || RootLinkState != `ROOT`)",
        "RootSymbol == `AAPL`",
        "RootSide == `BUY`",
    ]
    assert node_filters("ACME", include_unlinked=False) == ["RootMoniker == `ACME`"]
    assert bucket_filters("ACME")[0] == "(RootMoniker == `ACME` || RootMoniker == ``)"


def test_prefix_mode_is_case_insensitive_for_moniker_and_symbol_only():
    clauses = node_filters("ac", "aa", "BUY", include_unlinked=False, prefix=True)
    assert clauses == [
        "RootMoniker.toUpperCase().startsWith(`AC`)",
        "RootSymbol.toUpperCase().startsWith(`AA`)",
        "RootSide == `BUY`",
    ]


def test_breaks_only():
    assert node_filters(breaks_only=True) == ["TreeBreaks > 0"]
    assert bucket_filters(breaks_only=True) == ["Breaks > 0"]


def test_values_are_sanitized():
    assert sanitize_id(" A`B'C\"D\\E\n ") == "ABCDE"
    assert sanitize_id(None) == ""
    assert node_filters("x`) || true || (`", include_unlinked=False) == ["RootMoniker == `x) || true || (`"]


def test_row_bucket_payload_shapes():
    plain = {"RootMoniker": "ACME", "RootSymbol": "AAPL", "RootSide": "BUY"}
    assert row_bucket((plain,), {}) == ("ACME", "AAPL", "BUY")
    cells = {k: {"value": v, "text": v} for k, v in plain.items()}
    assert row_bucket((3, cells), {}) == ("ACME", "AAPL", "BUY")
    assert row_bucket((), {"row": plain}) == ("ACME", "AAPL", "BUY")
    assert row_bucket((), {}) is None
    assert row_bucket(({"Other": 1},), {}) is None
