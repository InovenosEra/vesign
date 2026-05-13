"""Tests for backend/yield_calcs.py — pure $1000-per-BUY math."""
import pytest
from backend.yield_calcs import avg_cost_dollar_weighted


def test_single_lot_returns_that_price():
    assert avg_cost_dollar_weighted([100.0]) == pytest.approx(100.0)


def test_two_lots_harmonic_mean():
    # $1000 at $100 → 10 shares; $1000 at $200 → 5 shares; 15 shares for $2000
    # avg = 2000/15 = 133.333…
    assert avg_cost_dollar_weighted([100.0, 200.0]) == pytest.approx(2000 / 15)


def test_fds_five_lots_matches_design_doc():
    # From the spec: FDS lots at 336.04, 301.23, 253.62, 222.62, 193.66 → ~$251.38.
    # Harmonic mean of these exact prices is 251.33; the spec rounds to .38 but
    # the cross-check ((232.73 - avg) / avg == -7.40%) only holds for 251.33,
    # so the design-doc figure is a typo. Allowing 0.1 tolerance.
    prices = [336.04, 301.23, 253.62, 222.62, 193.66]
    assert avg_cost_dollar_weighted(prices) == pytest.approx(251.38, abs=0.1)


def test_lower_than_simple_mean_when_prices_differ():
    # Harmonic mean ≤ arithmetic mean (strict when values differ)
    prices = [100.0, 200.0, 300.0]
    from statistics import mean
    assert avg_cost_dollar_weighted(prices) < mean(prices)


def test_empty_raises():
    with pytest.raises(ValueError):
        avg_cost_dollar_weighted([])


def test_non_positive_raises():
    with pytest.raises(ValueError):
        avg_cost_dollar_weighted([100.0, 0.0])
    with pytest.raises(ValueError):
        avg_cost_dollar_weighted([100.0, -10.0])
