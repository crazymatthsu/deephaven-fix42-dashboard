"""Universe parsing and the seeded quote walk -- doc 10 sections 4.3 and 6.

The properties the collector's ``market_data_latest`` depends on, none of which need
a Deephaven server: determinism per seed (so the e2e's ``OpenNotional`` assertion is
reproducible), bounded steps (so a demo cannot wander to zero), ``bid < mid < ask``,
and self-seeding for symbols that show up in the order flow.
"""

from __future__ import annotations

import pytest

from remote_uri import config
from remote_uri.marketdata import (
    DEFAULT_REFERENCE_PRICE,
    MIN_PRICE,
    MarketDataWalk,
    parse_universe,
    seed_from_rows,
)


# --------------------------------------------------------------------------------------
# parse_universe
# --------------------------------------------------------------------------------------


def test_the_default_universe_is_the_generators_eight_symbols():
    universe = parse_universe(config.DEFAULT_MD_SYMBOLS)
    assert list(universe) == ["AAPL", "MSFT", "NVDA", "AMZN", "TSLA", "META", "GOOGL", "JPM"]
    assert universe["NVDA"] == 120.0
    assert universe["JPM"] == 200.0


def test_parse_universe_normalises_case_and_whitespace():
    assert parse_universe(" aapl : 1.5 , msft:2 ") == {"AAPL": 1.5, "MSFT": 2.0}


@pytest.mark.parametrize("raw", [None, "", "   ", ","])
def test_a_blank_universe_parses_to_nothing(raw):
    # The *caller* substitutes the default; this function never invents symbols.
    assert parse_universe(raw) == {}


@pytest.mark.parametrize(
    "raw",
    ["AAPL", "AAPL:", "AAPL:abc", "AAPL:0", "AAPL:-1", "AAPL:inf", ":190", "AAPL:1,AAPL:2"],
)
def test_a_malformed_universe_is_a_startup_error(raw):
    with pytest.raises(ValueError) as excinfo:
        parse_universe(raw)
    assert "REMOTEURI_MD_SYMBOLS" in str(excinfo.value)


def test_parse_universe_names_the_caller_variable():
    with pytest.raises(ValueError) as excinfo:
        parse_universe("AAPL", env_name="SOMETHING_ELSE")
    assert "SOMETHING_ELSE" in str(excinfo.value)


# --------------------------------------------------------------------------------------
# The walk
# --------------------------------------------------------------------------------------


def _walk(seed=42, **kwargs):
    return MarketDataWalk(universe={"AAPL": 190.0, "MSFT": 420.0}, seed=seed, **kwargs)


def test_the_initial_snapshot_is_the_reference_prices():
    rows = _walk().snapshot_rows()
    assert [row[0] for row in rows] == ["AAPL", "MSFT"]
    assert rows[0][3] == 190.0
    assert rows[1][3] == 420.0


def test_rows_are_sorted_by_symbol():
    walk = MarketDataWalk(universe={"ZZZ": 1.0, "AAA": 2.0, "MMM": 3.0}, seed=1)
    assert [row[0] for row in walk.snapshot_rows()] == ["AAA", "MMM", "ZZZ"]


def test_bid_is_below_mid_is_below_ask():
    walk = _walk()
    for _ in range(20):
        for symbol, bid, ask, mid in walk.step():
            assert bid < mid < ask, symbol
            # A 5 bp half-spread, symmetric around the mid.
            assert mid - bid == pytest.approx(ask - mid, rel=1e-6)


def test_a_zero_spread_collapses_bid_mid_ask():
    walk = _walk(spread_bps=0.0)
    symbol, bid, ask, mid = walk.snapshot_rows()[0]
    assert bid == mid == ask


def test_the_walk_is_deterministic_for_a_seed():
    # Two fresh walks with the same seed agree step for step -- what makes the e2e's
    # OpenNotional assertion reproducible for a given start.
    first, second = _walk(seed=7), _walk(seed=7)
    assert [first.step() for _ in range(5)] == [second.step() for _ in range(5)]
    assert first.steps == second.steps == 5


def test_different_seeds_diverge():
    first, second = _walk(seed=1), _walk(seed=2)
    assert first.step() != second.step()


def test_a_step_moves_every_price_by_at_most_sigma():
    walk = _walk(sigma_bps=10.0)
    before = walk.prices
    walk.step()
    after = walk.prices
    for symbol, price in after.items():
        assert abs(price - before[symbol]) <= before[symbol] * (10.0 / 10_000.0) + 1e-9


def test_a_long_walk_stays_positive_and_finite():
    walk = MarketDataWalk(universe={"PENNY": MIN_PRICE}, seed=3, sigma_bps=500.0)
    for _ in range(500):
        walk.step()
    assert walk.prices["PENNY"] >= MIN_PRICE


# --------------------------------------------------------------------------------------
# Self-seeding (doc 10 section 4.3)
# --------------------------------------------------------------------------------------


def test_an_unseen_symbol_is_added_with_its_first_price():
    walk = _walk()
    assert walk.ensure("NVDA", 120.5) is True
    assert walk.prices["NVDA"] == 120.5
    assert walk.added == 1


def test_an_unseen_symbol_without_a_price_falls_back_to_100():
    walk = _walk()
    assert walk.ensure("NVDA", None) is True
    assert walk.prices["NVDA"] == DEFAULT_REFERENCE_PRICE


def test_a_defaulted_symbol_is_upgraded_by_the_first_real_price():
    # "added with their first non-null Price (else 100.0)": a market order arriving
    # before any limit order must not pin the symbol at 100 forever.
    walk = _walk()
    walk.ensure("NVDA", None)
    assert walk.ensure("NVDA", 121.0) is True
    assert walk.prices["NVDA"] == 121.0
    # ...but only once: a later price is just another tick, not a reset.
    assert walk.ensure("NVDA", 999.0) is False
    assert walk.prices["NVDA"] == 121.0


def test_a_known_symbol_is_never_reseeded():
    walk = _walk()
    assert walk.ensure("AAPL", 1.0) is False
    assert walk.prices["AAPL"] == 190.0


@pytest.mark.parametrize("price", [None, 0, -5, "abc", float("nan")])
def test_an_unusable_price_seeds_the_default(price):
    walk = _walk()
    walk.ensure("ZZZ", price)
    assert walk.prices["ZZZ"] == DEFAULT_REFERENCE_PRICE


@pytest.mark.parametrize("symbol", [None, "", "   "])
def test_a_blank_symbol_is_ignored(symbol):
    walk = _walk()
    assert walk.ensure(symbol, 1.0) is False
    assert len(walk) == 2


def test_symbols_are_normalised_to_upper_case():
    walk = _walk()
    walk.ensure("nvda", 120.0)
    assert "NVDA" in walk
    assert walk.ensure("NVDA", 130.0) is False


def test_seed_from_rows_counts_the_changes():
    walk = _walk()
    rows = [
        {"Symbol": "AAPL", "Price": 1.0},
        {"Symbol": "NVDA", "Price": 120.0},
        {"Symbol": "NVDA", "Price": 121.0},
        {"Symbol": "TSLA", "Price": None},
    ]
    assert seed_from_rows(walk, rows) == 2
    assert walk.prices["NVDA"] == 120.0
    assert walk.prices["TSLA"] == DEFAULT_REFERENCE_PRICE


def test_a_new_symbol_appears_in_the_next_snapshot():
    walk = _walk()
    walk.ensure("NVDA", 120.0)
    assert [row[0] for row in walk.step()] == ["AAPL", "MSFT", "NVDA"]


def test_spread_and_sigma_must_be_sane():
    for kwargs in ({"spread_bps": -1}, {"sigma_bps": -1}, {"sigma_bps": float("inf")}):
        with pytest.raises(ValueError):
            MarketDataWalk(universe={"A": 1.0}, **kwargs)
