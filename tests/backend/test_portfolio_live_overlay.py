"""Tests that /api/portfolio/holdings overlays the live snapshot price on latest_close."""
import os
import tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def portfolio_app():
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "test_portfolio.db")
    os.environ["BYPASS_AUTH"] = "1"
    os.environ.pop("BYPASS_USER_ID", None)   # ensure uid == 'dev-bypass'
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}", poolclass=None)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE watchlist_lists (id INTEGER PRIMARY KEY, user_id TEXT, name TEXT)"))
        conn.execute(text("CREATE TABLE holdings (id INTEGER PRIMARY KEY, user_id TEXT, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT)"))
        conn.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT, industry TEXT, description TEXT, description_short TEXT, logo_url TEXT, domain TEXT)"))
        conn.execute(text("CREATE TABLE fundamentals (ticker TEXT, market_cap REAL)"))
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        conn.execute(text("CREATE TABLE company_health (ticker TEXT PRIMARY KEY, score INTEGER, reason TEXT)"))
        conn.execute(text("CREATE TABLE signals (ticker TEXT, date TEXT, signal TEXT, fair_value_upside REAL, prediction_score REAL, target_mean_price REAL)"))
        conn.execute(text("INSERT INTO watchlist_lists (id, user_id, name) VALUES (1, 'dev-bypass', 'Mine')"))
        conn.execute(text("INSERT INTO holdings (user_id, ticker, quantity, buy_price, buy_date) VALUES ('dev-bypass','AAA',10,100.0,'2026-01-01')"))
        conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES ('AAA','AAA Corp','US')"))
        conn.execute(text("INSERT INTO fundamentals (ticker, market_cap) VALUES ('AAA', 1000)"))
        conn.execute(text("INSERT INTO daily_prices (date,ticker,open,high,low,close,volume) VALUES ('2026-05-22 00:00:00','AAA',105,105,105,105,1)"))
    eng.dispose()
    import importlib, backend.main as bm
    importlib.reload(bm)
    # load_dotenv in main.py re-sets BYPASS_USER_ID from .env; clear it again so
    # get_current_user returns the synthetic 'dev-bypass' id (not the real user).
    os.environ.pop("BYPASS_USER_ID", None)
    yield bm, TestClient(bm.app)
    for f in os.listdir(tmpdir):
        try: os.remove(os.path.join(tmpdir, f))
        except OSError: pass
    os.rmdir(tmpdir)


def test_holdings_use_live_price(portfolio_app):
    bm, client = portfolio_app
    with patch.object(bm, "_phase_info", return_value={"phase": "regular"}), \
         patch.object(bm, "fetch_live_prices", return_value={"AAA": 120.0}):
        body = client.get("/api/portfolio/holdings?market=US").json()
    h = next(x for x in body if x["ticker"] == "AAA")
    assert h["latest_close"] == 120.0      # live price, not the 105 daily close
    assert h["avg_price"] == 100.0         # unchanged


def test_holdings_fallback_to_daily_close(portfolio_app):
    bm, client = portfolio_app
    with patch.object(bm, "_phase_info", return_value={"phase": "idle"}):
        body = client.get("/api/portfolio/holdings?market=US").json()
    h = next(x for x in body if x["ticker"] == "AAA")
    assert h["latest_close"] == 105.0      # no live -> daily close


def _add_prior_days(bm):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO daily_prices (date,ticker,close,volume) VALUES ('2026-05-21 00:00:00','AAA',100,1)"))
        conn.execute(text("INSERT INTO daily_prices (date,ticker,close,volume) VALUES ('2026-05-20 00:00:00','AAA',90,1)"))


def test_today_baseline_is_latest_daily_close_during_live(portfolio_app):
    """During a live session today's bar isn't in daily_prices yet, so latest_close
    is overlaid with the live price. The 'today' baseline (prev_close) must then be
    the latest COMPLETED session close (rn=1 = 105), NOT the session before it
    (rn=2 = 100). Mirrors the Market panel baseline (_build_universe_baseline)."""
    bm, client = portfolio_app
    _add_prior_days(bm)   # daily closes: 05-22=105 (latest), 05-21=100, 05-20=90
    with patch.object(bm, "_phase_info", return_value={"phase": "regular"}), \
         patch.object(bm, "fetch_live_prices", return_value={"AAA": 120.0}):
        body = client.get("/api/portfolio/holdings?market=US").json()
    h = next(x for x in body if x["ticker"] == "AAA")
    assert h["latest_close"] == 120.0      # live intraday price
    assert h["prev_close"] == 105.0        # latest completed daily close, not 100


def test_today_baseline_is_prior_close_when_idle(portfolio_app):
    """Idle (no live overlay): latest_close = latest daily close (rn=1 = 105) and the
    baseline is the prior session (rn=2 = 100) — the last completed session's move."""
    bm, client = portfolio_app
    _add_prior_days(bm)
    with patch.object(bm, "_phase_info", return_value={"phase": "idle"}):
        body = client.get("/api/portfolio/holdings?market=US").json()
    h = next(x for x in body if x["ticker"] == "AAA")
    assert h["latest_close"] == 105.0      # daily close
    assert h["prev_close"] == 100.0        # prior session (rn=2)
