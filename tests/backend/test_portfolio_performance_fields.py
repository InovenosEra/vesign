"""Tests for spy + value fields on /api/portfolio/performance."""
import os, tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def perf_app():
    tmp = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmp, "t.db")
    os.environ["BYPASS_AUTH"] = "1"
    os.environ.pop("BYPASS_USER_ID", None)
    from sqlalchemy import create_engine, text
    from datetime import date, timedelta
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}", poolclass=None)
    with eng.begin() as c:
        c.execute(text("CREATE TABLE watchlist_lists (id INTEGER PRIMARY KEY, user_id TEXT, name TEXT)"))
        c.execute(text("CREATE TABLE watchlist_holdings (id INTEGER PRIMARY KEY, watchlist_id INTEGER, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT)"))
        c.execute(text("CREATE TABLE daily_prices (ticker TEXT, date TEXT, close REAL)"))
        c.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, industry TEXT, logo_url TEXT, domain TEXT)"))
        c.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        c.execute(text("INSERT INTO watchlist_lists VALUES (1,'dev-bypass','Mine')"))
        # one lot, bought ~13 months ago so it's present across the whole window
        start = date.today() - timedelta(days=400)
        c.execute(text("INSERT INTO watchlist_holdings (watchlist_id,ticker,quantity,buy_price,buy_date) "
                       f"VALUES (1,'AAPL',10,100.0,'{start.isoformat()}')"))
        # daily prices for AAPL + SPY + QQQ across the last 400 days (rising)
        d = start
        i = 0
        while d <= date.today():
            c.execute(text("INSERT INTO daily_prices VALUES ('AAPL',:dt,:p)"), {"dt": d.isoformat(), "p": 100.0 + i})
            c.execute(text("INSERT INTO daily_prices VALUES ('SPY',:dt,:p)"), {"dt": d.isoformat(), "p": 400.0 + i})
            c.execute(text("INSERT INTO daily_prices VALUES ('QQQ',:dt,:p)"), {"dt": d.isoformat(), "p": 300.0 + i})
            d += timedelta(days=1); i += 1
    eng.dispose()
    import importlib, backend.main as bm
    importlib.reload(bm)
    os.environ.pop("BYPASS_USER_ID", None)
    yield bm, TestClient(bm.app)
    for f in os.listdir(tmp):
        try: os.remove(os.path.join(tmp, f))
        except OSError: pass
    os.rmdir(tmp)


def test_performance_has_spy_and_value(perf_app):
    bm, client = perf_app
    # Empty Vesign sim (no trade_log in this fixture); we assert spy/value/portfolio.
    empty_cache = {"lots": [], "price_at": lambda *a: None}
    with patch.object(bm, "_get_vesign_cache", return_value=empty_cache):
        pts = client.get("/api/portfolio/performance?months=12").json()
    assert len(pts) >= 2
    assert "spy" in pts[0] and "value" in pts[0]
    assert pts[0]["spy"] == 0.0          # normalized to 0 at window start
    assert pts[-1]["spy"] > 0            # SPY rose over the window
    assert pts[-1]["value"] is not None and pts[-1]["value"] > 0


def test_performance_extra_ticker_series(perf_app):
    bm, client = perf_app
    empty_cache = {"lots": [], "price_at": lambda *a: None}
    with patch.object(bm, "_get_vesign_cache", return_value=empty_cache):
        pts = client.get("/api/portfolio/performance?months=12&extra=qqq").json()
    assert "QQQ" in pts[0]               # uppercased extra ticker keyed in
    assert pts[0]["QQQ"] == 0.0          # normalized to 0 at window start
    assert pts[-1]["QQQ"] > 0            # QQQ rose over the window
