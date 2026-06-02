"""Tests for GET /api/market/commodities — Gold/Silver/WTI/Brent/NatGas/Copper."""
import os
import tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def comm_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_commodities.db")
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


FIXTURE = {
    "GC=F": {"price": 4520.0, "prev_close": 4500.0},  # +0.4444%
    "SI=F": {"price": 76.0,   "prev_close": 75.0},     # +1.3333%
    "CL=F": {"price": 96.0,   "prev_close": 98.0},     # -2.0408%
    "BZ=F": {"price": 100.0,  "prev_close": 100.0},    # 0%
    "NG=F": {"price": 3.00,   "prev_close": 2.90},     # +3.4483%
    "HG=F": {"price": 6.40,   "prev_close": 6.30},     # +1.5873%
}


def test_returns_six_commodities_with_change_pct(comm_app):
    bm, client = comm_app
    with patch.object(bm, "_fetch_yf_quotes", return_value=FIXTURE):
        body = client.get("/api/market/commodities").json()
    assert "commodities" in body
    by = {r["ticker"]: r for r in body["commodities"]}
    assert set(by.keys()) == {"GC=F", "SI=F", "CL=F", "BZ=F", "NG=F", "HG=F"}
    assert by["GC=F"]["label"] == "Gold"
    assert by["GC=F"]["change_pct"] == pytest.approx(0.4444, abs=1e-3)
    assert by["CL=F"]["change_pct"] == pytest.approx(-2.0408, abs=1e-3)
    assert all(r["stale"] is False for r in body["commodities"])


def test_stale_cached_when_fetch_fails(comm_app):
    bm, client = comm_app
    with patch.object(bm, "_fetch_yf_quotes", return_value=FIXTURE):
        first = client.get("/api/market/commodities").json()
    assert first["commodities"][0]["stale"] is False
    with bm._market_cache_lock:
        bm._market_cache["commodities"]["t"] = 0.0  # force expiry
    with patch.object(bm, "_fetch_yf_quotes", return_value=None):
        body = client.get("/api/market/commodities").json()
    assert len(body["commodities"]) == 6
    assert all(r["stale"] is True for r in body["commodities"])


def test_empty_when_no_cache_and_fetch_fails(comm_app):
    bm, client = comm_app
    with patch.object(bm, "_fetch_yf_quotes", return_value=None):
        body = client.get("/api/market/commodities").json()
    assert body == {"commodities": []}


FULL = {
    "GC=F": {"price": 4520.0, "prev_close": 4500.0},
    "SI=F": {"price": 76.0,   "prev_close": 75.0},
    "PL=F": {"price": 1959.0, "prev_close": 1920.0},
    "PA=F": {"price": 1402.0, "prev_close": 1360.0},
    "CL=F": {"price": 96.0,   "prev_close": 98.0},
    "BZ=F": {"price": 100.0,  "prev_close": 100.0},
    "NG=F": {"price": 3.00,   "prev_close": 2.90},
    "HG=F": {"price": 6.40,   "prev_close": 6.30},
}


def test_partial_fetch_keeps_previously_seen_cards(comm_app):
    """A later partial yfinance batch (live-snapshot contention) must NOT drop
    cards already seen — they're served stale at their last-good price instead.
    Reproduces the disappearing-cards bug."""
    bm, client = comm_app
    with patch.object(bm, "_fetch_yf_quotes", return_value=FULL):
        first = client.get("/api/market/commodities").json()
    assert len(first["commodities"]) == 8  # all eight present after a clean fetch

    with bm._market_cache_lock:
        bm._market_cache["commodities"]["t"] = 0.0  # force expiry → rebuild
    partial = {  # only two come back this time
        "GC=F": {"price": 4530.0, "prev_close": 4500.0},
        "CL=F": {"price": 95.0,   "prev_close": 98.0},
    }
    with patch.object(bm, "_fetch_yf_quotes", return_value=partial):
        body = client.get("/api/market/commodities").json()

    by = {r["ticker"]: r for r in body["commodities"]}
    assert set(by.keys()) == set(FULL.keys())            # none vanish
    assert by["GC=F"]["stale"] is False and by["GC=F"]["price"] == 4530.0  # refreshed
    assert by["SI=F"]["stale"] is True and by["SI=F"]["price"] == 76.0     # last-good
    assert by["PA=F"]["stale"] is True and by["PA=F"]["price"] == 1402.0   # last-good
