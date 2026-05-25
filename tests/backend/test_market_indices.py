"""Tests for GET /api/market/indices — the 5 headline-index cards.

Real index levels (^GSPC/^NDX/^DJI/^RUT) come from the index_prices table;
VIX comes from the vix table. Both are populated from yfinance by the pipeline.
"""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient

INDEXES = ["^GSPC", "^NDX", "^DJI", "^RUT"]


@pytest.fixture
def indices_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_indices.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
        conn.execute(text("CREATE TABLE index_prices (date DATETIME, ticker TEXT, close FLOAT)"))
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
        conn.execute(text("CREATE TABLE vix (date DATETIME, close FLOAT)"))
        conn.execute(text("""
            CREATE TABLE companies (
                ticker TEXT PRIMARY KEY, company TEXT, domain TEXT,
                market TEXT, logo_url TEXT, industry TEXT,
                description TEXT, description_short TEXT
            )
        """))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
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
        conn.execute(text("INSERT INTO index_prices (date, ticker, close) VALUES (:date, :ticker, :close)"),
                     dict(date=date, ticker=ticker, close=close))


def _insert_vix(bm, *, date, close):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO vix (date, close) VALUES (:date, :close)"), dict(date=date, close=close))


def test_returns_five_indices_with_close_and_change(indices_app):
    bm, client = indices_app
    for ticker, prev, today in [
        ("^GSPC", 7440.0, 7470.0),  # +0.4032%
        ("^NDX", 29600.0, 29400.0),  # -0.6757%
        ("^DJI", 50400.0, 50526.0),  # +0.25%
        ("^RUT", 2840.0, 2868.4),    # +1.0%
    ]:
        _insert_price(bm, date="2026-05-21 00:00:00", ticker=ticker, close=prev)
        _insert_price(bm, date="2026-05-22 00:00:00", ticker=ticker, close=today)
    _insert_vix(bm, date="2026-05-21 00:00:00", close=17.00)
    _insert_vix(bm, date="2026-05-22 00:00:00", close=16.70)  # -1.7647%

    body = client.get("/api/market/indices").json()
    by = {row["ticker"]: row for row in body["indices"]}
    assert set(by.keys()) == {"^GSPC", "^NDX", "^DJI", "^RUT", "VIX"}
    assert by["^GSPC"]["close"] == pytest.approx(7470.0)
    assert by["^GSPC"]["change_pct"] == pytest.approx(0.4032, abs=1e-3)
    assert by["^NDX"]["change_pct"] == pytest.approx(-0.6757, abs=1e-3)
    assert by["VIX"]["close"] == pytest.approx(16.70)
    assert by["VIX"]["change_pct"] == pytest.approx(-1.7647, abs=1e-3)


def test_sparkline_is_chronological_and_capped_at_30(indices_app):
    bm, client = indices_app
    for i in range(40):
        date = f"2026-04-{i+1:02d} 00:00:00" if i < 30 else f"2026-05-{i-29:02d} 00:00:00"
        _insert_price(bm, date=date, ticker="^GSPC", close=100.0 + i)

    spy = next(r for r in client.get("/api/market/indices").json()["indices"] if r["ticker"] == "^GSPC")
    spark = spy["sparkline"]
    assert len(spark) == 30
    assert spark == sorted(spark)
    assert spark[-1] == pytest.approx(139.0)
    assert spy["close"] == pytest.approx(139.0)


def test_missing_ticker_returns_null_close(indices_app):
    bm, client = indices_app
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="^GSPC", close=7470.0)
    body = client.get("/api/market/indices").json()
    by = {row["ticker"]: row for row in body["indices"]}
    assert set(by.keys()) == {"^GSPC", "^NDX", "^DJI", "^RUT", "VIX"}
    assert by["^NDX"]["close"] is None
    assert by["^NDX"]["change_pct"] is None
    assert by["^NDX"]["sparkline"] == []
    assert by["VIX"]["close"] is None


def test_change_pct_null_with_single_day(indices_app):
    bm, client = indices_app
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="^GSPC", close=7470.0)
    spy = next(r for r in client.get("/api/market/indices").json()["indices"] if r["ticker"] == "^GSPC")
    assert spy["close"] == pytest.approx(7470.0)
    assert spy["change_pct"] is None
    assert spy["sparkline"] == [7470.0]
