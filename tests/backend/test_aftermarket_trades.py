"""Tests for data.fmp.aftermarket_trades — FMP extended-hours client."""
from unittest.mock import patch
from data.fmp import aftermarket_trades


def test_returns_price_for_traded_ticker():
    def fake_get(endpoint, params):
        assert endpoint == "aftermarket-trade"
        return [{"symbol": params["symbol"], "price": 298.76,
                 "tradeSize": 307, "timestamp": 1778765539000}]
    with patch("data.fmp._get", side_effect=fake_get):
        out = aftermarket_trades(["AAPL"])
    assert out == {"AAPL": 298.76}


def test_empty_response_yields_none():
    with patch("data.fmp._get", return_value=[]):
        out = aftermarket_trades(["DIME"])
    assert out == {"DIME": None}


def test_handles_multiple_tickers():
    def fake_get(endpoint, params):
        prices = {"AAPL": 298.76, "MSFT": 404.36, "DIME": None}
        p = prices.get(params["symbol"])
        return [{"symbol": params["symbol"], "price": p}] if p else []
    with patch("data.fmp._get", side_effect=fake_get):
        out = aftermarket_trades(["AAPL", "MSFT", "DIME"])
    assert out == {"AAPL": 298.76, "MSFT": 404.36, "DIME": None}


def test_empty_input_returns_empty_dict():
    assert aftermarket_trades([]) == {}
