"""Tests for GET /api/market/economic-calendar — US macro events ahead."""
import os
import tempfile
from datetime import date
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def econ_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_economic.db")
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


def test_filters_us_and_maps_importance(econ_app):
    bm, client = econ_app
    fixture = [
        {"date": "2026-05-26 12:30:00", "country": "US", "event": "Initial Jobless Claims",
         "estimate": 230000, "previous": 220000, "actual": None, "impact": "High"},
        {"date": "2026-05-27 14:00:00", "country": "US", "event": "FOMC Minutes",
         "estimate": None, "previous": None, "actual": None, "impact": "Medium"},
        {"date": "2026-05-28 08:00:00", "country": "UK", "event": "BoE Speech",
         "estimate": None, "previous": None, "actual": None, "impact": "Medium"},
        {"date": "2026-05-29 10:00:00", "country": "US", "event": "Consumer Sentiment",
         "estimate": 70.0, "previous": 69.5, "actual": None, "impact": "Low"},
    ]
    with patch.object(bm, "_fetch_economic_calendar", return_value=fixture):
        body = client.get("/api/market/economic-calendar?days=7").json()

    events = body["events"]
    # UK row dropped; 3 US rows kept.
    assert len(events) == 3
    assert all(e.get("country") == "US" for e in events) or all("country" not in e for e in events)
    by_event = {e["event"]: e for e in events}
    assert by_event["Initial Jobless Claims"]["importance"] == 3
    assert by_event["FOMC Minutes"]["importance"] == 2
    assert by_event["Consumer Sentiment"]["importance"] == 1
    # Estimate / prior pass through.
    assert by_event["Initial Jobless Claims"]["estimate"] == 230000
    assert by_event["Initial Jobless Claims"]["prior"] == 220000


def test_days_parameter_sets_range(econ_app):
    bm, client = econ_app
    with patch.object(bm, "_fetch_economic_calendar", return_value=[]) as m:
        client.get("/api/market/economic-calendar?days=14")
    start_s, end_s = m.call_args[0]
    start, end = date.fromisoformat(start_s), date.fromisoformat(end_s)
    assert (end - start).days == 14


def test_empty_when_fetch_returns_none(econ_app):
    bm, client = econ_app
    with patch.object(bm, "_fetch_economic_calendar", return_value=None):
        body = client.get("/api/market/economic-calendar?days=7").json()
    assert body["events"] == []  # holidays may still be present (separate key)
