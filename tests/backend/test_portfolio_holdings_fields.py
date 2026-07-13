"""Tests for sector + signal fields on /api/portfolio/holdings."""
import os, tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def hold_app():
    tmp = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmp, "t.db")
    os.environ["BYPASS_AUTH"] = "1"
    os.environ.pop("BYPASS_USER_ID", None)
    # Pro+ so signal isn't redacted by entitlements.redact_holdings() (unrelated
    # to holdings/watchlist decoupling — this fixture predates that gating).
    os.environ["DEV_PLAN"] = "pro"
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}", poolclass=None)
    with eng.begin() as c:
        c.execute(text("CREATE TABLE watchlist_lists (id INTEGER PRIMARY KEY, user_id TEXT, name TEXT)"))
        c.execute(text("CREATE TABLE holdings (id INTEGER PRIMARY KEY, user_id TEXT, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT)"))
        c.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, industry TEXT, logo_url TEXT, domain TEXT)"))
        c.execute(text("CREATE TABLE fundamentals (ticker TEXT, market_cap REAL)"))
        c.execute(text("CREATE TABLE company_health (ticker TEXT PRIMARY KEY, score INTEGER, reason TEXT)"))
        c.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        c.execute(text("CREATE TABLE signals (ticker TEXT, date TEXT, signal TEXT, fair_value_upside REAL, prediction_score REAL, target_mean_price REAL)"))
        c.execute(text("CREATE TABLE daily_prices (ticker TEXT, date TEXT, close REAL)"))
        c.execute(text("INSERT INTO watchlist_lists VALUES (1,'dev-bypass','Mine')"))
        c.execute(text("INSERT INTO holdings (user_id,ticker,quantity,buy_price,buy_date) VALUES ('dev-bypass','AAPL',10,100.0,'2026-01-01')"))
        c.execute(text("INSERT INTO companies (ticker,company,sector,industry) VALUES ('AAPL','Apple','Technology','Consumer Electronics')"))
        c.execute(text("INSERT INTO fundamentals VALUES ('AAPL',3.1e12)"))
        c.execute(text("INSERT INTO company_health VALUES ('AAPL',5,'ok')"))
        c.execute(text("INSERT INTO signals VALUES ('AAPL','2026-06-01','HOLD',0.1,0.8,250.0),('AAPL','2026-06-28','BUY',0.2,0.9,260.0)"))
        c.execute(text("INSERT INTO daily_prices VALUES ('AAPL','2026-06-27',240.0),('AAPL','2026-06-28',245.0)"))
    eng.dispose()
    import importlib, backend.main as bm
    importlib.reload(bm)
    os.environ.pop("BYPASS_USER_ID", None)
    yield bm, TestClient(bm.app)
    os.environ.pop("DEV_PLAN", None)
    for f in os.listdir(tmp):
        try: os.remove(os.path.join(tmp, f))
        except OSError: pass
    os.rmdir(tmp)


def test_holdings_includes_sector_and_latest_signal(hold_app):
    bm, client = hold_app
    with patch.object(bm, "_phase_info", return_value={"phase": "idle"}):
        rows = client.get("/api/portfolio/holdings").json()
    assert len(rows) == 1
    r = rows[0]
    assert r["sector"] == "Technology"
    assert r["signal"] == "BUY"   # latest by date, not HOLD
