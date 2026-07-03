"""Tests for /api/portfolio/comparison after holdings/watchlist decoupling.

Holdings no longer belong to a watchlist, so the bar chart collapses from
"one bar per watchlist" to a single aggregate "Mine" bar (see task-2-report.md).
"""
import os, tempfile
from datetime import date, timedelta
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def cmp_app():
    tmp = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmp, "t.db")
    os.environ["BYPASS_AUTH"] = "1"
    os.environ.pop("BYPASS_USER_ID", None)
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}", poolclass=None)
    with eng.begin() as c:
        c.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, industry TEXT, logo_url TEXT, domain TEXT)"))
        c.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        c.execute(text("CREATE TABLE holdings (id INTEGER PRIMARY KEY, user_id TEXT, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT)"))
        c.execute(text("CREATE TABLE daily_prices (ticker TEXT, date TEXT, close REAL)"))
    eng.dispose()
    import importlib, backend.main as bm
    importlib.reload(bm)
    os.environ.pop("BYPASS_USER_ID", None)
    yield bm, TestClient(bm.app)
    for f in os.listdir(tmp):
        try: os.remove(os.path.join(tmp, f))
        except OSError: pass
    os.rmdir(tmp)


def _seed_holdings_and_prices(bm, ticker_a="AAA", ticker_b="BBB"):
    """Seed two tickers' holdings (bought long before the 52-week window, so
    price_at_start is used as the base — not buy_price) plus exactly the two
    price points the endpoint needs: at the window start and today."""
    from sqlalchemy import text
    today = date.today()
    start_date = today - timedelta(weeks=52)
    old_buy_date = "2015-01-01"

    with bm.engine.begin() as c:
        c.execute(text("INSERT INTO holdings (user_id,ticker,quantity,buy_price,buy_date) VALUES "
                        "('dev-bypass',:t,10,999.0,:bd)"), {"t": ticker_a, "bd": old_buy_date})
        c.execute(text("INSERT INTO holdings (user_id,ticker,quantity,buy_price,buy_date) VALUES "
                        "('dev-bypass',:t,5,999.0,:bd)"), {"t": ticker_b, "bd": old_buy_date})
        # ticker_a: base 100 -> current 150   (10 * 150 = 1500 val, 10 * 100 = 1000 base)
        # ticker_b: base 80  -> current 100   (5 * 100 = 500 val,  5 * 80  = 400 base)
        c.execute(text("INSERT INTO daily_prices VALUES (:t, :d, :p)"),
                   {"t": ticker_a, "d": start_date.isoformat(), "p": 100.0})
        c.execute(text("INSERT INTO daily_prices VALUES (:t, :d, :p)"),
                   {"t": ticker_a, "d": today.isoformat(), "p": 150.0})
        c.execute(text("INSERT INTO daily_prices VALUES (:t, :d, :p)"),
                   {"t": ticker_b, "d": start_date.isoformat(), "p": 80.0})
        c.execute(text("INSERT INTO daily_prices VALUES (:t, :d, :p)"),
                   {"t": ticker_b, "d": today.isoformat(), "p": 100.0})


def test_comparison_collapses_to_single_mine_bar(cmp_app):
    bm, client = cmp_app
    _seed_holdings_and_prices(bm)
    empty_cache = {"lots": [], "price_at": lambda *a: None}
    with patch.object(bm, "_get_vesign_cache", return_value=empty_cache):
        result = client.get("/api/portfolio/comparison").json()

    # Vesign bar is skipped (empty sim -> peak_bank == 0), so only "Mine" remains.
    non_vesign = [r for r in result if r["name"] != "Vesign"]
    assert len(non_vesign) == 1
    mine = non_vesign[0]
    assert mine["name"] == "Mine"

    # total_val = 1500 + 500 = 2000; total_base = 1000 + 400 = 1400
    expected_yield = round((2000 / 1400 - 1) * 100, 2)
    assert mine["yield"] == expected_yield

    # No leftover watchlist-name bars of any kind.
    assert all(r["name"] in ("Vesign", "Mine") for r in result)


def test_comparison_empty_holdings_returns_empty_list(cmp_app):
    bm, client = cmp_app
    empty_cache = {"lots": [], "price_at": lambda *a: None}
    with patch.object(bm, "_get_vesign_cache", return_value=empty_cache):
        result = client.get("/api/portfolio/comparison").json()
    assert result == []
