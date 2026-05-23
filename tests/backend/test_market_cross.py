"""Tests for GET /api/market/cross — cross-asset strip (USD/yields/gold/oil/BTC/EURUSD)."""
import os
import tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def cross_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_cross.db")
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


CROSS_FIXTURE = {
    "DX-Y.NYB": {"price": 103.50, "prev_close": 103.60},   # -0.0965%
    "^TNX":     {"price": 4.35,   "prev_close": 4.40},     # -1.1364%
    "GC=F":     {"price": 2350.0, "prev_close": 2340.0},   # +0.4274%
    "CL=F":     {"price": 78.50,  "prev_close": 80.00},    # -1.875%
    "BTC-USD":  {"price": 69000,  "prev_close": 68000},    # +1.4706%
    "EURUSD=X": {"price": 1.08,   "prev_close": 1.0805},   # ~-0.0463%
}


def test_returns_six_cross_quotes_with_change_pct(cross_app):
    bm, client = cross_app
    # Patch the fetcher to return canned data; endpoint should compute change_pct.
    with patch.object(bm, "_fetch_cross_quotes", return_value=CROSS_FIXTURE):
        body = client.get("/api/market/cross").json()

    assert "cross" in body
    by_t = {r["ticker"]: r for r in body["cross"]}
    assert set(by_t.keys()) == set(CROSS_FIXTURE.keys())
    assert by_t["DX-Y.NYB"]["price"] == pytest.approx(103.50)
    assert by_t["DX-Y.NYB"]["change_pct"] == pytest.approx(-0.0965, abs=1e-3)
    assert by_t["BTC-USD"]["change_pct"] == pytest.approx(1.4706, abs=1e-3)
    # Fresh data → stale=False
    assert all(r["stale"] is False for r in body["cross"])
    # Each row carries a human label for the UI
    assert by_t["DX-Y.NYB"]["label"]
    assert by_t["BTC-USD"]["label"]


def test_returns_stale_cached_when_both_sources_fail(cross_app):
    bm, client = cross_app
    # First call: fresh data populates the cache.
    with patch.object(bm, "_fetch_cross_quotes", return_value=CROSS_FIXTURE):
        first = client.get("/api/market/cross").json()
    assert first["cross"][0]["stale"] is False

    # Clear TTL so the next call would re-fetch — but fetcher now fails (returns None).
    with bm._market_cache_lock:
        bm._market_cache["cross"]["t"] = 0.0  # force expiry
    with patch.object(bm, "_fetch_cross_quotes", return_value=None):
        body = client.get("/api/market/cross").json()

    assert "cross" in body
    assert len(body["cross"]) == 6
    # Cached prices reused, but `stale: true` so the UI can show a pill.
    assert all(r["stale"] is True for r in body["cross"])
    by_t = {r["ticker"]: r for r in body["cross"]}
    assert by_t["DX-Y.NYB"]["price"] == pytest.approx(103.50)


def test_returns_empty_when_no_cache_and_fetch_fails(cross_app):
    bm, client = cross_app
    # No prior fresh fetch → cache empty → fetcher fails → empty `cross` list.
    with patch.object(bm, "_fetch_cross_quotes", return_value=None):
        body = client.get("/api/market/cross").json()
    assert body == {"cross": []}
