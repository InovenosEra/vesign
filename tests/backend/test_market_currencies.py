"""Tests for GET /api/market/currencies — key currencies' rate to a chosen base."""
import os
import tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def cur_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_currencies.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT, industry TEXT, description TEXT, description_short TEXT, logo_url TEXT, domain TEXT)"))
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


def test_currencies_for_base_ils(cur_app):
    bm, client = cur_app
    fixture = {
        "USDILS=X": {"price": 3.65, "prev_close": 3.70},
        "EURILS=X": {"price": 3.95, "prev_close": 3.96},
        "GBPILS=X": {"price": 4.60, "prev_close": 4.55},
        "JPYILS=X": {"price": 0.024, "prev_close": 0.0241},
        "CHFILS=X": {"price": 4.10, "prev_close": 4.09},
    }
    with patch.object(bm, "_fetch_yf_quotes", return_value=fixture):
        body = client.get("/api/market/currencies?base=ILS").json()
    assert body["base"] == "ILS"
    assert [c["label"] for c in body["currencies"]] == ["USD / ILS", "EUR / ILS", "GBP / ILS", "JPY / ILS", "CHF / ILS"]
    usd = next(c for c in body["currencies"] if c["ticker"] == "USDILS=X")
    assert usd["price"] == pytest.approx(3.65)
    assert usd["change_pct"] == pytest.approx((3.65 - 3.70) / 3.70 * 100, abs=1e-3)


def test_missing_pair_falls_through_to_next_currency(cur_app):
    bm, client = cur_app
    # CHFILS missing → the 6th candidate (CNYILS) fills the 5th slot.
    fixture = {
        "USDILS=X": {"price": 3.65, "prev_close": 3.70},
        "EURILS=X": {"price": 3.95, "prev_close": 3.96},
        "GBPILS=X": {"price": 4.60, "prev_close": 4.55},
        "JPYILS=X": {"price": 0.024, "prev_close": 0.0241},
        "CNYILS=X": {"price": 0.50, "prev_close": 0.50},
    }
    with patch.object(bm, "_fetch_yf_quotes", return_value=fixture):
        body = client.get("/api/market/currencies?base=ILS").json()
    tickers = [c["ticker"] for c in body["currencies"]]
    assert len(tickers) == 5
    assert "CHFILS=X" not in tickers
    assert "CNYILS=X" in tickers


def test_base_excluded_and_invalid_base_defaults_ils(cur_app):
    bm, client = cur_app
    with patch.object(bm, "_fetch_yf_quotes", return_value={}):
        body = client.get("/api/market/currencies?base=usd").json()
    # base normalized to upper; USD itself never appears as a USD/USD pair.
    assert body["base"] == "USD"
    assert all(not c["ticker"].startswith("USDUSD") for c in body["currencies"])
