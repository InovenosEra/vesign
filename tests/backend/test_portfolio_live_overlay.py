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
        conn.execute(text("CREATE TABLE watchlist_holdings (id INTEGER PRIMARY KEY, watchlist_id INTEGER, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT)"))
        conn.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT, industry TEXT, description TEXT, description_short TEXT, logo_url TEXT, domain TEXT)"))
        conn.execute(text("CREATE TABLE fundamentals (ticker TEXT, market_cap REAL)"))
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        conn.execute(text("INSERT INTO watchlist_lists (id, user_id, name) VALUES (1, 'dev-bypass', 'Mine')"))
        conn.execute(text("INSERT INTO watchlist_holdings (watchlist_id, ticker, quantity, buy_price, buy_date) VALUES (1,'AAA',10,100.0,'2026-01-01')"))
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
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": {"AAA": 120.0}}):
        body = client.get("/api/portfolio/holdings?market=US").json()
    h = next(x for x in body if x["ticker"] == "AAA")
    assert h["latest_close"] == 120.0      # live price, not the 105 daily close
    assert h["avg_price"] == 100.0         # unchanged


def test_holdings_fallback_to_daily_close(portfolio_app):
    bm, client = portfolio_app
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "idle", "prices": {}}):
        body = client.get("/api/portfolio/holdings?market=US").json()
    h = next(x for x in body if x["ticker"] == "AAA")
    assert h["latest_close"] == 105.0      # no live -> daily close
