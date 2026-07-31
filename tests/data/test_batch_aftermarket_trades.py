"""Tests for fmp.batch_aftermarket_trades — batched extended-hours last trade price."""
from unittest.mock import patch
from data import fmp


def test_batches_and_returns_price():
    calls = []

    def fake_get(endpoint, params, _retries=3):
        calls.append((endpoint, params))
        return [
            {"symbol": "AAPL", "price": 308.89, "tradeSize": 167},
            {"symbol": "MSFT", "price": 419.20, "tradeSize": 88},
        ]

    with patch.object(fmp, "_get", side_effect=fake_get):
        out = fmp.batch_aftermarket_trades(["AAPL", "MSFT"])

    assert calls[0][0] == "batch-aftermarket-trade"
    assert calls[0][1]["symbols"] == "AAPL,MSFT"
    assert out["AAPL"] == 308.89
    assert out["MSFT"] == 419.20


def test_empty_list_no_call():
    with patch.object(fmp, "_get") as g:
        assert fmp.batch_aftermarket_trades([]) == {}
        g.assert_not_called()


def test_non_list_response_yields_empty():
    with patch.object(fmp, "_get", return_value=None):
        assert fmp.batch_aftermarket_trades(["AAPL"]) == {}
