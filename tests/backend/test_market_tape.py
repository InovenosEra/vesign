"""Tests for GET /api/market/tape — single-roundtrip payload for the 32px ticker tape."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def tape_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_tape.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE daily_prices (
                date DATETIME, ticker TEXT,
                open FLOAT, high FLOAT, low FLOAT, close FLOAT,
                volume BIGINT
            )
        """))
        conn.execute(text("""
            CREATE TABLE vix (date DATETIME, close FLOAT)
        """))
        conn.execute(text("""
            CREATE TABLE companies (
                ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT,
                industry TEXT, description TEXT, description_short TEXT,
                logo_url TEXT, domain TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE company_health_history (
                ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT
            )
        """))
    temp_engine.dispose()

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


def _insert_two_day(bm, *, ticker, prev, today):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        for d, c in [("2026-05-21 00:00:00", prev), ("2026-05-22 00:00:00", today)]:
            conn.execute(text("""
                INSERT INTO daily_prices (date, ticker, open, high, low, close, volume)
                VALUES (:d, :t, :c, :c, :c, :c, 0)
            """), {"d": d, "t": ticker, "c": c})


def _insert_vix(bm, *, date, close):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO vix (date, close) VALUES (:d, :c)"),
                     {"d": date, "c": close})


def test_tape_returns_configured_tickers_in_order(tape_app):
    bm, client = tape_app
    # Insert minimal data for the configured set (all but a couple, to exercise missing too).
    _insert_two_day(bm, ticker="SPY", prev=740.0, today=745.0)   # +0.6757
    _insert_two_day(bm, ticker="NVDA", prev=100.0, today=102.0)  # +2.0
    _insert_two_day(bm, ticker="TSLA", prev=300.0, today=297.0)  # -1.0
    _insert_vix(bm, date="2026-05-21 00:00:00", close=17.0)
    _insert_vix(bm, date="2026-05-22 00:00:00", close=16.7)      # -1.7647

    body = client.get("/api/market/tape").json()
    assert "tape" in body
    items = body["tape"]
    # Configured order from the mockup (top of file).
    expected_order = ["SPY", "QQQ", "DIA", "IWM", "VIX",
                      "NVDA", "MSFT", "AAPL", "META", "TSLA",
                      "AMZN", "GOOGL", "MU", "PM", "MTD", "JPM"]
    assert [r["ticker"] for r in items] == expected_order

    by_ticker = {r["ticker"]: r for r in items}
    assert by_ticker["SPY"]["close"] == pytest.approx(745.0)
    assert by_ticker["SPY"]["change_pct"] == pytest.approx(0.6757, abs=1e-3)
    assert by_ticker["NVDA"]["change_pct"] == pytest.approx(2.0, abs=1e-3)
    assert by_ticker["TSLA"]["change_pct"] == pytest.approx(-1.0, abs=1e-3)
    assert by_ticker["VIX"]["close"] == pytest.approx(16.7)
    assert by_ticker["VIX"]["change_pct"] == pytest.approx(-1.7647, abs=1e-3)


def test_missing_data_returns_nulls_not_404(tape_app):
    bm, client = tape_app
    # Only SPY has data; the rest should appear with close=None.
    _insert_two_day(bm, ticker="SPY", prev=740.0, today=745.0)

    body = client.get("/api/market/tape").json()
    by_ticker = {r["ticker"]: r for r in body["tape"]}
    assert by_ticker["SPY"]["close"] == pytest.approx(745.0)
    assert by_ticker["NVDA"]["close"] is None
    assert by_ticker["NVDA"]["change_pct"] is None
    assert by_ticker["VIX"]["close"] is None  # vix table empty
