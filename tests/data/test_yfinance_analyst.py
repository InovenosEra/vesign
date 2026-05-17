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
