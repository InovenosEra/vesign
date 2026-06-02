"""Tests for GET /api/market/analyst-changes/top — top moves in analyst targets."""
import os
import tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def analyst_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_analyst_changes.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE analyst_targets_history (
                date TEXT, ticker TEXT,
                target_mean_price REAL, target_high_price REAL, target_low_price REAL,
                number_of_analysts REAL, source TEXT
            )
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
        # The endpoint enriches top changes with a current price from daily_prices
        # (live-snapshot first, daily close as fallback).
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
    temp_engine.dispose()

    import importlib
    import backend.main as bm
    importlib.reload(bm)
    # Stub the whole-universe live snapshot so price-enrichment is deterministic and
    # never reaches yfinance (the tests assert on TP changes, not on live price).
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "idle", "prices": {}}):
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


def _insert_company(bm, ticker, market="US"):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO companies (ticker, company, market) VALUES (:t, :t, :m)
        """), {"t": ticker, "m": market})


def _insert_target(bm, *, date, ticker, mean, n=10):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO analyst_targets_history
                (date, ticker, target_mean_price, target_high_price, target_low_price,
                 number_of_analysts, source)
            VALUES (:d, :t, :m, :m, :m, :n, 'test')
        """), {"d": date, "t": ticker, "m": mean, "n": n})


def test_classifies_raise_lower_initiate(analyst_app):
    bm, client = analyst_app
    for t in ["UPSIDE", "DOWNSIDE", "NEW"]:
        _insert_company(bm, t)
    # UPSIDE: 100 → 120 (+20%, RAISE-TP)
    _insert_target(bm, date="2026-05-21", ticker="UPSIDE", mean=100.0)
    _insert_target(bm, date="2026-05-22", ticker="UPSIDE", mean=120.0)
    # DOWNSIDE: 100 → 90 (-10%, LOWER-TP)
    _insert_target(bm, date="2026-05-21", ticker="DOWNSIDE", mean=100.0)
    _insert_target(bm, date="2026-05-22", ticker="DOWNSIDE", mean=90.0)
    # NEW: no prior row → INITIATE
    _insert_target(bm, date="2026-05-22", ticker="NEW", mean=50.0)

    body = client.get("/api/market/analyst-changes/top?days=1&limit=5").json()
    by_ticker = {r["ticker"]: r for r in body["changes"]}
    assert by_ticker["UPSIDE"]["kind"] == "RAISE-TP"
    assert by_ticker["UPSIDE"]["change_pct"] == pytest.approx(20.0, abs=1e-3)
    assert by_ticker["UPSIDE"]["target_mean_price"] == pytest.approx(120.0)
    assert by_ticker["UPSIDE"]["prev_target_mean_price"] == pytest.approx(100.0)

    assert by_ticker["DOWNSIDE"]["kind"] == "LOWER-TP"
    assert by_ticker["DOWNSIDE"]["change_pct"] == pytest.approx(-10.0, abs=1e-3)

    assert by_ticker["NEW"]["kind"] == "INITIATE"
    assert by_ticker["NEW"]["target_mean_price"] == pytest.approx(50.0)
    assert by_ticker["NEW"]["prev_target_mean_price"] is None
    assert by_ticker["NEW"]["change_pct"] is None


def test_sorted_by_absolute_change_desc(analyst_app):
    bm, client = analyst_app
    for t in ["A", "B", "C", "D"]:
        _insert_company(bm, t)
    # A: +25%, B: -15%, C: +5%, D: -30%
    pairs = [("A", 100, 125), ("B", 100, 85), ("C", 100, 105), ("D", 100, 70)]
    for t, prev, today in pairs:
        _insert_target(bm, date="2026-05-21", ticker=t, mean=prev)
        _insert_target(bm, date="2026-05-22", ticker=t, mean=today)

    body = client.get("/api/market/analyst-changes/top?days=1&limit=4").json()
    # Sorted by |change_pct| DESC: D(30), A(25), B(15), C(5)
    assert [r["ticker"] for r in body["changes"]] == ["D", "A", "B", "C"]


def test_respects_days_parameter(analyst_app):
    """days=7 compares latest against 7 days back, ignoring intermediate days."""
    bm, client = analyst_app
    _insert_company(bm, "X")
    # 7 days ago = 100, intermediate days = 110, today = 120
    # days=1 should compare today vs day-1 (still 110 if we had it, but we only have day-7 and day-0)
    _insert_target(bm, date="2026-05-15", ticker="X", mean=100.0)  # 7 days ago
    _insert_target(bm, date="2026-05-22", ticker="X", mean=120.0)  # today

    body = client.get("/api/market/analyst-changes/top?days=7&limit=5").json()
    by_ticker = {r["ticker"]: r for r in body["changes"]}
    assert by_ticker["X"]["change_pct"] == pytest.approx(20.0, abs=1e-3)
    assert by_ticker["X"]["prev_target_mean_price"] == pytest.approx(100.0)


def test_limit_caps_output(analyst_app):
    bm, client = analyst_app
    for i, t in enumerate(["A", "B", "C", "D", "E", "F"]):
        _insert_company(bm, t)
        _insert_target(bm, date="2026-05-21", ticker=t, mean=100.0)
        _insert_target(bm, date="2026-05-22", ticker=t, mean=100.0 + (i + 1))  # +1..+6%

    body = client.get("/api/market/analyst-changes/top?days=1&limit=3").json()
    assert len(body["changes"]) == 3


def test_no_change_excluded(analyst_app):
    """A ticker whose TP didn't move shouldn't appear in 'top changes'."""
    bm, client = analyst_app
    _insert_company(bm, "FLAT")
    _insert_company(bm, "MOVED")
    _insert_target(bm, date="2026-05-21", ticker="FLAT", mean=100.0)
    _insert_target(bm, date="2026-05-22", ticker="FLAT", mean=100.0)
    _insert_target(bm, date="2026-05-21", ticker="MOVED", mean=100.0)
    _insert_target(bm, date="2026-05-22", ticker="MOVED", mean=105.0)

    body = client.get("/api/market/analyst-changes/top?days=1&limit=5").json()
    tickers = [r["ticker"] for r in body["changes"]]
    assert "FLAT" not in tickers
    assert "MOVED" in tickers
