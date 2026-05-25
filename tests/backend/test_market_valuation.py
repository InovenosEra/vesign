"""Tests for GET /api/market/valuation — most under/overvalued vs analyst target.

Hardened to the global last-close date: a ticker whose latest price row is older
than the last close (pipeline gap) must NOT leak in with a stale price.
"""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def val_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_valuation.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
        conn.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, domain TEXT, market TEXT, logo_url TEXT, industry TEXT, description TEXT, description_short TEXT)"))
        conn.execute(text("CREATE TABLE analyst_expectations (ticker TEXT, target_mean_price FLOAT, target_low_price FLOAT, target_high_price FLOAT, number_of_analysts INTEGER)"))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
    eng.dispose()

    import importlib
    import backend.main as bm
    importlib.reload(bm)
    yield bm, TestClient(bm.app)

    for fname in os.listdir(tmpdir):
        try:
            os.remove(os.path.join(tmpdir, fname))
        except OSError:
            pass
    try:
        os.rmdir(tmpdir)
    except OSError:
        pass


def _company(bm, t):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES (:t, :t, 'US')"), {"t": t})


def _price(bm, t, date, close):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO daily_prices (date, ticker, open, high, low, close, volume) VALUES (:d, :t, :c, :c, :c, :c, 0)"),
                     {"d": date, "t": t, "c": close})


def _ae(bm, t, mean, n):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO analyst_expectations (ticker, target_mean_price, number_of_analysts) VALUES (:t, :m, :n)"),
                     {"t": t, "m": mean, "n": n})


def test_undervalued_ranks_by_upside(val_app):
    bm, client = val_app
    _company(bm, "FRESH"); _company(bm, "MILD")
    _price(bm, "FRESH", "2026-05-22 00:00:00", 100.0)
    _price(bm, "MILD",  "2026-05-22 00:00:00", 100.0)
    _ae(bm, "FRESH", 150.0, 10)  # +50%
    _ae(bm, "MILD",  110.0, 10)  # +10%
    body = client.get("/api/market/valuation?limit=10").json()
    unders = [r["ticker"] for r in body["undervalued"]]
    assert unders[0] == "FRESH"  # highest upside first
    assert "MILD" in unders


def test_stale_ticker_excluded_from_valuation(val_app):
    # STALE's only price row is older than the last close — it must NOT appear,
    # even though its (stale) price implies a huge upside.
    bm, client = val_app
    _company(bm, "FRESH"); _company(bm, "STALE")
    _price(bm, "FRESH", "2026-05-22 00:00:00", 100.0)  # defines the last close
    _price(bm, "STALE", "2026-05-15 00:00:00", 50.0)   # stale
    _ae(bm, "FRESH", 150.0, 10)   # +50% off fresh price
    _ae(bm, "STALE", 200.0, 10)   # +300% off the stale $50 — would top the list if leaked

    body = client.get("/api/market/valuation?limit=10").json()
    unders = [r["ticker"] for r in body["undervalued"]]
    assert "FRESH" in unders
    assert "STALE" not in unders


def test_thin_names_excluded(val_app):
    # < 3 analysts shouldn't drive the list.
    bm, client = val_app
    _company(bm, "THIN"); _company(bm, "OK")
    _price(bm, "THIN", "2026-05-22 00:00:00", 100.0)
    _price(bm, "OK",   "2026-05-22 00:00:00", 100.0)
    _ae(bm, "THIN", 300.0, 2)   # huge upside but only 2 analysts
    _ae(bm, "OK",   120.0, 5)
    unders = [r["ticker"] for r in client.get("/api/market/valuation?limit=10").json()["undervalued"]]
    assert "THIN" not in unders
    assert "OK" in unders
