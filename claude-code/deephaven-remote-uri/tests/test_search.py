"""The lookup's filter clauses and sort order -- doc 10 section 9.

The assignment's question is one clause list applied to three tables, so the clauses
are built once and tested once. ``deephaven`` is never imported: ``apply_filters``
only calls ``Table.where``, which a two-line stub can stand in for.
"""

from __future__ import annotations

import pytest

from remote_uri import search
from remote_uri.query_api import executions_expression


class _StubTable:
    """Records the ``where`` clauses it was handed (the only method used)."""

    def __init__(self):
        self.clauses = None

    def where(self, clauses):
        self.clauses = clauses
        return self


# --------------------------------------------------------------------------------------
# root_filters
# --------------------------------------------------------------------------------------


def test_all_three_arguments_build_three_clauses():
    assert search.root_filters("OMS-A", "ACC-1", "NVDA") == [
        "RootOms == `OMS-A`",
        "RootAccount == `ACC-1`",
        "RootSymbol == `NVDA`",
    ]


@pytest.mark.parametrize("blank", [None, "", "   "])
def test_a_blank_argument_means_any(blank):
    assert search.root_filters(blank, blank, blank) == []
    assert search.root_filters("OMS-A", blank, "NVDA") == [
        "RootOms == `OMS-A`",
        "RootSymbol == `NVDA`",
    ]


def test_the_match_is_on_the_root_not_the_hop():
    # Asking for OMS-A must return the OMS-C legs of those families too: they carry
    # Oms == "OMS-C" while sharing RootOms == "OMS-A".
    assert all(clause.startswith("Root") for clause in search.root_filters("OMS-A", "A", "S"))


def test_identifiers_are_sanitised_before_interpolation():
    clauses = search.root_filters("OMS-A`) || true || (`", None, None)
    assert "`" not in clauses[0].replace("RootOms == `", "").rstrip("`")
    assert clauses == ["RootOms == `OMS-A) || true || (`"]


def test_control_characters_are_stripped():
    assert search.root_filters("A\nB", None, None) == ["RootOms == `AB`"]


# --------------------------------------------------------------------------------------
# Sort order and application
# --------------------------------------------------------------------------------------


def test_family_sort_is_upstream_to_downstream_within_a_family():
    assert search.FAMILY_SORT == ["RootKey", "Depth", "Oms", "OrderKey"]


def test_apply_filters_passes_the_clause_list_through():
    table = _StubTable()
    assert search.apply_filters(table, ["RootOms == `OMS-A`"]) is table
    assert table.clauses == ["RootOms == `OMS-A`"]


@pytest.mark.parametrize("clauses", [None, []])
def test_no_clauses_means_the_unfiltered_table(clauses):
    # Not an always-true `where`: that would compile a filter for nothing.
    table = _StubTable()
    assert search.apply_filters(table, clauses) is table
    assert table.clauses is None


def test_describe_filters_reads_back_as_the_panel_title():
    assert search.describe_filters("OMS-A", None, "NVDA") == "oms=OMS-A account=* symbol=NVDA"
    assert search.describe_filters(None, None, None) == "oms=* account=* symbol=*"


# --------------------------------------------------------------------------------------
# The remote query's expression (doc 10 section 3)
# --------------------------------------------------------------------------------------


def test_the_remote_expression_nests_the_two_quoting_layers():
    # Python string quotes outside (it is python that runs on the leaf), Deephaven
    # backtick literal inside -- the two must never collide.
    assert (
        executions_expression("OMS-A|A-0001")
        == 'oms_executions.where("GlobalKey == `OMS-A|A-0001`")'
    )


def test_the_remote_expression_reads_a_table_the_collector_does_not_hold():
    # Executions are ~70% of the message count and stay on the leaf (doc 10 s2.5).
    assert executions_expression("k").startswith("oms_executions.where(")
