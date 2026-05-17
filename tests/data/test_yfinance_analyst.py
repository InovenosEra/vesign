"""Tests for data.yfinance_analyst — yfinance adapter."""
from unittest.mock import patch, MagicMock
from data import yfinance_analyst


def test_returns_parsed_dict_for_well_covered_ticker():
    fake_info = {
        "targetMeanPrice": 250.0,
        "targetHighPrice": 290.0,
        "targetLowPrice": 210.0,
        "numberOfAnalystOpinions": 35,
    }
    with patch("data.yfinance_analyst.yf.Ticker") as MockT:
        MockT.return_value.info = fake_info
        out = yfinance_analyst.get_targets("AAPL")
    assert out == {
        "target_mean_price": 250.0,
        "target_high_price": 290.0,
        "target_low_price":  210.0,
        "number_of_analysts": 35,
    }


def test_returns_none_when_no_mean():
    """yfinance returning info dict without targetMeanPrice — treat as no data."""
    with patch("data.yfinance_analyst.yf.Ticker") as MockT:
        MockT.return_value.info = {"longName": "SomeETF"}
        assert yfinance_analyst.get_targets("SPY") is None


def test_returns_none_on_exception():
    """Network error or any yfinance failure — return None, never raise."""
    with patch("data.yfinance_analyst.yf.Ticker", side_effect=Exception("blocked")):
        assert yfinance_analyst.get_targets("AAPL") is None


def test_zero_or_negative_mean_treated_as_no_data():
    """Sometimes yfinance returns 0.0 for stubs — same as no data."""
    with patch("data.yfinance_analyst.yf.Ticker") as MockT:
        MockT.return_value.info = {"targetMeanPrice": 0.0}
        assert yfinance_analyst.get_targets("WEIRD") is None


def test_batch_returns_dict_per_ticker():
    """Batch fetch returns a dict keyed by ticker, including Nones for misses."""
    def fake_get_targets(t):
        return {"target_mean_price": 100.0, "target_high_price": 120.0,
                "target_low_price": 80.0, "number_of_analysts": 5} if t != "NOPE" else None

    with patch("data.yfinance_analyst.get_targets", side_effect=fake_get_targets):
        out = yfinance_analyst.get_targets_batch(["AAPL", "MSFT", "NOPE"], max_workers=2, sleep_sec=0)

    assert set(out.keys()) == {"AAPL", "MSFT", "NOPE"}
    assert out["AAPL"]["target_mean_price"] == 100.0
    assert out["NOPE"] is None


def test_batch_isolates_per_ticker_failures():
    """If get_targets raises for one ticker, others still succeed."""
    def fake_get_targets(t):
        if t == "BAD":
            raise RuntimeError("boom")
        return {"target_mean_price": 50.0, "target_high_price": None,
                "target_low_price": None, "number_of_analysts": None}

    with patch("data.yfinance_analyst.get_targets", side_effect=fake_get_targets):
        out = yfinance_analyst.get_targets_batch(["BAD", "OK"], max_workers=2, sleep_sec=0)

    assert out["BAD"] is None
    assert out["OK"]["target_mean_price"] == 50.0
