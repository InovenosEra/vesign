"""Tests for GET /api/market/earnings/week — next-N-days earnings calendar."""
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def earnings_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_earnings.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
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
        conn.execute(text("""
            CREATE TABLE company_health (ticker TEXT PRIMARY KEY, score INTEGER, reason TEXT)
        """))
        conn.execute(text("""
            CREATE TABLE fundamentals (
                ticker TEXT, market_cap REAL, pe_ttm REAL
            )
        """))
        conn.execute(text("""
            INSERT INTO companies (ticker, company) VALUES ('NVDA', 'NVIDIA Corp')
        """))
        conn.execute(text("INSERT INTO company_health (ticker, score) VALUES ('NVDA', 5)"))
        conn.execute(text(
            "INSERT INTO fundamentals (ticker, market_cap, pe_ttm) VALUES ('NVDA', 3100000000000, 55.2)"
        ))
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


def test_returns_week_with_company_join_and_day_of_week(earnings_app):
    bm, client = earnings_app
    fmp_fixture = [
        {"date": "2026-05-26", "symbol": "NVDA", "epsEstimated": 4.5, "time": "amc"},
        {"date": "2026-05-28", "symbol": "CRM",  "epsEstimated": 2.1, "time": "bmo"},
    ]
    with patch.object(bm, "_fetch_earnings_week", return_value=fmp_fixture) as m:
        body = client.get("/api/market/earnings/week").json()

    # Fetcher gets called with [today, today + default days].
    args = m.call_args[0]
    assert len(args) == 2
    start, end = args
    today = datetime.now(timezone.utc).date()
    assert date.fromisoformat(start) == today
    assert date.fromisoformat(end) == today + timedelta(days=7)

    rows = body["earnings"]
    assert len(rows) == 2
    by_ticker = {r["ticker"]: r for r in rows}
    nvda = by_ticker["NVDA"]
    assert nvda["company"] == "NVIDIA Corp"     # join from companies
    assert nvda["eps_estimated"] == pytest.approx(4.5)
    assert nvda["time"] == "AMC"                # normalized to upper
    assert nvda["day_of_week"] == "Tuesday"     # 2026-05-26 was a Tuesday
    assert nvda["health_score"] == 5            # join from company_health
    assert nvda["market_cap"] == pytest.approx(3.1e12)  # join from fundamentals
    assert nvda["pe_ttm"] == pytest.approx(55.2)
    crm = by_ticker["CRM"]
    assert crm["company"] is None               # not in companies table
    assert crm["time"] == "BMO"
    assert crm["day_of_week"] == "Thursday"
    assert crm["health_score"] is None          # no indicator rows
    assert crm["market_cap"] is None
    assert crm["pe_ttm"] is None


def test_days_param_widens_window(earnings_app):
    bm, client = earnings_app
    with patch.object(bm, "_fetch_earnings_week", return_value=[]) as m:
        client.get("/api/market/earnings/week?days=14")
    start, end = m.call_args[0]
    today = datetime.now(timezone.utc).date()
    assert date.fromisoformat(start) == today
    assert date.fromisoformat(end) == today + timedelta(days=14)


def test_unknown_time_passes_through_as_null(earnings_app):
    bm, client = earnings_app
    fixture = [{"date": "2026-05-27", "symbol": "X", "epsEstimated": 1.0, "time": ""}]
    with patch.object(bm, "_fetch_earnings_week", return_value=fixture):
        body = client.get("/api/market/earnings/week").json()
    assert body["earnings"][0]["time"] is None


def test_empty_when_fetch_returns_none(earnings_app):
    bm, client = earnings_app
    with patch.object(bm, "_fetch_earnings_week", return_value=None):
        body = client.get("/api/market/earnings/week").json()
    assert body == {"earnings": []}
