"""Tests for fmp.batch_aftermarket_quotes — batched extended-hours bid/ask."""
from unittest.mock import patch
from data import fmp


def test_batches_and_returns_bid_ask():
    calls = []

    def fake_get(endpoint, params, _retries=3):
        calls.append((endpoint, params))
        return [
            {"symbol": "AAPL", "bidPrice": 310.16, "askPrice": 310.35, "volume": 1000},
            {"symbol": "MSFT", "bidPrice": 418.91, "askPrice": 419.49, "volume": 500},
        ]

    with patch.object(fmp, "_get", side_effect=fake_get):
        out = fmp.batch_aftermarket_quotes(["AAPL", "MSFT"])

    assert calls[0][0] == "batch-aftermarket-quote"
    assert calls[0][1]["symbols"] == "AAPL,MSFT"
    assert out["AAPL"] == {"bidPrice": 310.16, "askPrice": 310.35}
    assert out["MSFT"] == {"bidPrice": 418.91, "askPrice": 419.49}


def test_empty_list_no_call():
    with patch.object(fmp, "_get") as g:
        assert fmp.batch_aftermarket_quotes([]) == {}
        g.assert_not_called()


def test_non_list_response_yields_empty():
    with patch.object(fmp, "_get", return_value=None):
        assert fmp.batch_aftermarket_quotes(["AAPL"]) == {}
