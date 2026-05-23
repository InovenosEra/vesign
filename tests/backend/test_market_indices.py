"""Tests for GET /api/market/indices — the 5 headline-index cards."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def indices_app():
    """Build an isolated FastAPI app over a temp SQLite DB with daily_prices + vix."""
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_indices.db")
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
                ticker TEXT PRIMARY KEY, company TEXT, domain TEXT,
                market TEXT, logo_url TEXT, industry TEXT,
                description TEXT, description_short TEXT
            )
        """))
        # backend.main._ensure_indexes() runs at module load and touches this table.
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


def _insert_price(bm, *, date, ticker, close):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO daily_prices (date, ticker, open, high, low, close, volume)
            VALUES (:date, :ticker, :close, :close, :close, :close, 0)
        """), dict(date=date, ticker=ticker, close=close))


def _insert_vix(bm, *, date, close):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO vix (date, close) VALUES (:date, :close)
        """), dict(date=date, close=close))


def test_returns_five_indices_with_close_and_change(indices_app):
    """SPY/QQQ/DIA/IWM come from daily_prices; VIX comes from the vix table."""
    bm, client = indices_app
    # Two days each so change_pct can be computed.
    for ticker, prev, today in [
        ("SPY", 740.00, 745.00),  # +0.6757%
        ("QQQ", 500.00, 495.00),  # -1.0%
        ("DIA", 380.00, 380.95),  # +0.25%
        ("IWM", 220.00, 222.20),  # +1.0%
    ]:
        _insert_price(bm, date="2026-05-21 00:00:00", ticker=ticker, close=prev)
        _insert_price(bm, date="2026-05-22 00:00:00", ticker=ticker, close=today)
    _insert_vix(bm, date="2026-05-21 00:00:00", close=17.00)
    _insert_vix(bm, date="2026-05-22 00:00:00", close=16.70)  # -1.7647%

    resp = client.get("/api/market/indices")
    assert resp.status_code == 200
    body = resp.json()
    assert "indices" in body
    by_ticker = {row["ticker"]: row for row in body["indices"]}

    assert set(by_ticker.keys()) == {"SPY", "QQQ", "DIA", "IWM", "VIX"}
    assert by_ticker["SPY"]["close"] == pytest.approx(745.00)
    assert by_ticker["SPY"]["change_pct"] == pytest.approx(0.6757, abs=1e-3)
    assert by_ticker["QQQ"]["change_pct"] == pytest.approx(-1.0, abs=1e-3)
    assert by_ticker["VIX"]["close"] == pytest.approx(16.70)
    assert by_ticker["VIX"]["change_pct"] == pytest.approx(-1.7647, abs=1e-3)


def test_sparkline_is_chronological_and_capped_at_30(indices_app):
    """40 prior days inserted; only the last 30 returned, oldest→newest."""
    bm, client = indices_app
    # Insert 40 ascending daily closes (oldest=100, newest=139).
    for i in range(40):
        date = f"2026-04-{i+1:02d} 00:00:00" if i < 30 else f"2026-05-{i-29:02d} 00:00:00"
        _insert_price(bm, date=date, ticker="SPY", close=100.0 + i)

    body = client.get("/api/market/indices").json()
    spy = next(r for r in body["indices"] if r["ticker"] == "SPY")
    spark = spy["sparkline"]
    assert len(spark) == 30
    # Must be ascending (oldest→newest) ending with the newest close.
    assert spark == sorted(spark)
    assert spark[-1] == pytest.approx(139.0)
    assert spy["close"] == pytest.approx(139.0)


def test_missing_ticker_returns_null_close(indices_app):
    """When daily_prices has no rows for QQQ, the response still lists it with close=null."""
    bm, client = indices_app
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="SPY", close=745.0)
    # VIX, QQQ, DIA, IWM intentionally absent.

    body = client.get("/api/market/indices").json()
    by_ticker = {row["ticker"]: row for row in body["indices"]}
    assert set(by_ticker.keys()) == {"SPY", "QQQ", "DIA", "IWM", "VIX"}
    assert by_ticker["QQQ"]["close"] is None
    assert by_ticker["QQQ"]["change_pct"] is None
    assert by_ticker["QQQ"]["sparkline"] == []
    assert by_ticker["VIX"]["close"] is None


def test_change_pct_null_with_single_day(indices_app):
    """Only one close exists → change_pct is None (no prior to compare against)."""
    bm, client = indices_app
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="SPY", close=745.0)

    body = client.get("/api/market/indices").json()
    spy = next(r for r in body["indices"] if r["ticker"] == "SPY")
    assert spy["close"] == pytest.approx(745.0)
    assert spy["change_pct"] is None
    assert spy["sparkline"] == [745.0]
