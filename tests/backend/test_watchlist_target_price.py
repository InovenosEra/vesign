"""Tests for target_price on watchlist ticker membership rows."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def wl_app():
    tmp = tempfile.mkdtemp()
    db_path = os.path.join(tmp, "t.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"
    os.environ.pop("BYPASS_USER_ID", None)
    # Pre-create necessary tables before reloading the module
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS companies (
                ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT,
                industry TEXT, description TEXT, description_short TEXT,
                logo_url TEXT, domain TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS company_health_history (
                ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT
            )
        """))
    eng.dispose()
    import importlib, backend.main as bm
    importlib.reload(bm)
    os.environ.pop("BYPASS_USER_ID", None)
    client = TestClient(bm.app)
    list_id = client.post("/api/watchlists", json={"name": "Dip candidates"}).json()["id"]
    client.post(f"/api/watchlists/{list_id}/tickers", json={"ticker": "AAPL"})
    yield bm, client, list_id
    for f in os.listdir(tmp):
        try: os.remove(os.path.join(tmp, f))
        except OSError: pass
    os.rmdir(tmp)


def test_ticker_starts_with_no_target_price(wl_app):
    bm, client, list_id = wl_app
    rows = client.get(f"/api/watchlists/{list_id}/tickers").json()
    assert rows[0]["ticker"] == "AAPL"
    assert rows[0]["target_price"] is None


def test_patch_sets_target_price(wl_app):
    bm, client, list_id = wl_app
    r = client.patch(f"/api/watchlists/{list_id}/tickers/AAPL", json={"target_price": 150.5})
    assert r.status_code == 200
    rows = client.get(f"/api/watchlists/{list_id}/tickers").json()
    assert rows[0]["target_price"] == 150.5


def test_patch_rejects_non_positive_target_price(wl_app):
    bm, client, list_id = wl_app
    assert client.patch(f"/api/watchlists/{list_id}/tickers/AAPL", json={"target_price": 0}).status_code == 400
    assert client.patch(f"/api/watchlists/{list_id}/tickers/AAPL", json={"target_price": -5}).status_code == 400


def test_patch_note_still_works_independent_of_target_price(wl_app):
    bm, client, list_id = wl_app
    client.patch(f"/api/watchlists/{list_id}/tickers/AAPL", json={"target_price": 150.5})
    client.patch(f"/api/watchlists/{list_id}/tickers/AAPL", json={"note": "watching for earnings"})
    rows = client.get(f"/api/watchlists/{list_id}/tickers").json()
    assert rows[0]["note"] == "watching for earnings"
    assert rows[0]["target_price"] == 150.5   # unchanged by the note-only patch
