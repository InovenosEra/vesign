"""Tests for backend.live_snapshot pure helpers (no IO)."""
from backend import live_snapshot as ls


def test_mid_price_average_of_bid_ask():
    assert ls.mid_price({"bidPrice": 310.16, "askPrice": 310.36}) == 310.26


def test_mid_price_none_when_side_missing_or_zero():
    assert ls.mid_price({"bidPrice": None, "askPrice": 310.0}) is None
    assert ls.mid_price({"bidPrice": 0, "askPrice": 310.0}) is None
    assert ls.mid_price({}) is None


def test_compute_rows_uses_live_price_and_change_vs_prev_close():
    baseline = {"AAA": {"prev_close": 100.0, "sector": "Tech", "market_cap": 5}}
    rows = ls.compute_universe_rows({"AAA": 110.0}, baseline)
    r = rows[0]
    assert r["ticker"] == "AAA"
    assert r["price"] == 110.0
    assert r["change_pct"] == 10.0
    assert r["sector"] == "Tech"
    assert r["market_cap"] == 5


def test_compute_rows_falls_back_to_prev_close_zero_change():
    baseline = {"BBB": {"prev_close": 50.0}}
    rows = ls.compute_universe_rows({}, baseline)
    assert rows[0]["price"] == 50.0
    assert rows[0]["change_pct"] == 0.0


def test_compute_rows_none_change_when_prev_close_falsy():
    baseline = {"CCC": {"prev_close": 0}}
    rows = ls.compute_universe_rows({"CCC": 5.0}, baseline)
    assert rows[0]["change_pct"] is None


def test_last_session_rows_uses_latest_close_and_change_vs_prior():
    # When the market is idle (closed), the panels show the last COMPLETED
    # session's move: price = latest close (prev_close), change = vs prior_close.
    baseline = {"AAA": {"prev_close": 110.0, "prior_close": 100.0,
                        "sector": "Tech", "market_cap": 5}}
    rows = ls.last_session_rows(baseline)
    r = rows[0]
    assert r["ticker"] == "AAA"
    assert r["price"] == 110.0          # latest completed-session close
    assert r["change_pct"] == 10.0      # (110 - 100) / 100 * 100
    assert r["sector"] == "Tech"
    assert r["market_cap"] == 5


def test_last_session_rows_none_change_when_prior_close_missing():
    # A ticker with only one session of history has no prior close -> no move.
    baseline = {"NEW": {"prev_close": 42.0, "prior_close": None}}
    rows = ls.last_session_rows(baseline)
    assert rows[0]["price"] == 42.0
    assert rows[0]["change_pct"] is None


def test_day_change_pct_live_measures_intraday_vs_latest_close():
    # With a live price, the move is live vs the latest completed close.
    assert ls.day_change_pct(latest_close=100.0, prior_close=90.0, live_price=110.0) == 10.0


def test_day_change_pct_no_live_falls_back_to_last_session():
    # No live price -> latest close vs prior close (last completed session).
    assert ls.day_change_pct(latest_close=99.0, prior_close=90.0) == 10.0


def test_day_change_pct_none_when_prices_missing_or_zero():
    assert ls.day_change_pct(latest_close=None, prior_close=90.0) is None
    assert ls.day_change_pct(latest_close=100.0, prior_close=None) is None
    assert ls.day_change_pct(latest_close=100.0, prior_close=0) is None
    assert ls.day_change_pct(latest_close=0, prior_close=90.0, live_price=0) is None
