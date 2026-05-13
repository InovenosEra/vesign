"""Tests for backend/yield_calcs.py — pure $1000-per-BUY math."""
import pytest
from backend.yield_calcs import avg_cost_dollar_weighted, per_trade_yield_dca


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


def test_per_trade_yield_single_lot_equals_return_pct():
    # n=1 → harmonic mean = the single price, so yield = (sell - p)/p
    y = per_trade_yield_dca(sell_price=120.0, lot_prices=[100.0])
    assert y == pytest.approx(0.20)


def test_per_trade_yield_fds_loss():
    # From spec: FDS sold at $232.73, 5 lots; yield = -7.40%
    prices = [336.04, 301.23, 253.62, 222.62, 193.66]
    y = per_trade_yield_dca(sell_price=232.73, lot_prices=prices)
    assert y == pytest.approx(-0.0740, abs=0.001)


def test_per_trade_yield_two_lot_winner():
    # $1000@100 → 10 sh, $1000@200 → 5 sh; total 15 sh for $2000; avg=133.33
    # Sell at $300: 15 * 300 = $4500; profit $2500; yield = 1.25
    y = per_trade_yield_dca(sell_price=300.0, lot_prices=[100.0, 200.0])
    assert y == pytest.approx(1.25, abs=0.001)
