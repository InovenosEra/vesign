"""Tests for /api/portfolio/holdings/lots and add_holding validation."""
import os
import tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def lots_app():
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "test_lots.db")
    os.environ["BYPASS_AUTH"] = "1"
    os.environ.pop("BYPASS_USER_ID", None)   # uid == 'dev-bypass'
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}", poolclass=None)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE watchlist_lists (id INTEGER PRIMARY KEY, user_id TEXT, name TEXT)"))
        conn.execute(text("CREATE TABLE watchlist_holdings (id INTEGER PRIMARY KEY, watchlist_id INTEGER, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT)"))
        conn.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT, industry TEXT, description TEXT, description_short TEXT, logo_url TEXT, domain TEXT)"))
        conn.execute(text("CREATE TABLE watchlist (list_id INTEGER, ticker TEXT, note TEXT)"))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        conn.execute(text("INSERT INTO watchlist_lists (id, user_id, name) VALUES (1,'dev-bypass','Mine'), (2,'someone-else','Theirs')"))
        conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES ('AAPL','Apple','US'), ('MSFT','Microsoft','US')"))
        conn.execute(text("INSERT INTO watchlist_holdings (watchlist_id,ticker,quantity,buy_price,buy_date) VALUES (1,'AAPL',10,100.0,'2026-01-01'), (1,'AAPL',5,120.0,'2026-02-01'), (2,'MSFT',3,300.0,'2026-01-01')"))
    eng.dispose()
    import importlib, backend.main as bm
    importlib.reload(bm)
    os.environ.pop("BYPASS_USER_ID", None)
    yield bm, TestClient(bm.app)
    for f in os.listdir(tmpdir):
        try: os.remove(os.path.join(tmpdir, f))
        except OSError: pass
    os.rmdir(tmpdir)


def test_lots_returns_user_ticker_lots_newest_first(lots_app):
    bm, client = lots_app
    rows = client.get("/api/portfolio/holdings/lots?ticker=aapl").json()
    assert [r["buy_date"] for r in rows] == ["2026-02-01", "2026-01-01"]
    assert all(r["ticker"] == "AAPL" for r in rows)
    assert all("id" in r and r["watchlist_id"] == 1 and r["watchlist_name"] == "Mine" for r in rows)


def test_lots_excludes_other_users(lots_app):
    bm, client = lots_app
    assert client.get("/api/portfolio/holdings/lots?ticker=MSFT").json() == []


def test_lots_empty_ticker(lots_app):
    bm, client = lots_app
    assert client.get("/api/portfolio/holdings/lots?ticker=").json() == []


def test_add_holding_rejects_bad_input(lots_app):
    bm, client = lots_app
    base = {"ticker": "AAPL", "quantity": 1, "buy_price": 100.0, "buy_date": "2026-01-01"}
    assert client.post("/api/watchlists/1/holdings", json={**base, "quantity": 0}).status_code == 400
    assert client.post("/api/watchlists/1/holdings", json={**base, "buy_price": -5}).status_code == 400
    assert client.post("/api/watchlists/1/holdings", json={**base, "buy_date": "2099-01-01"}).status_code == 400
    assert client.post("/api/watchlists/1/holdings", json={**base, "ticker": "NOPE"}).status_code == 400


def test_add_then_delete_lot_roundtrip(lots_app):
    bm, client = lots_app
    r = client.post("/api/watchlists/1/holdings",
                    json={"ticker": "msft", "quantity": 2, "buy_price": 310.0, "buy_date": "2026-03-01"})
    assert r.status_code == 201
    hid = r.json()["id"]
    lots = client.get("/api/portfolio/holdings/lots?ticker=MSFT").json()
    assert any(l["id"] == hid and l["ticker"] == "MSFT" for l in lots)
    assert client.delete(f"/api/watchlists/1/holdings/{hid}").status_code == 204
    assert client.get("/api/portfolio/holdings/lots?ticker=MSFT").json() == []


def test_add_holding_normalizes_date(lots_app):
    bm, client = lots_app
    r = client.post("/api/watchlists/1/holdings",
                    json={"ticker": "AAPL", "quantity": 1, "buy_price": 50.0, "buy_date": "2026-3-5"})
    assert r.status_code == 201
    lots = client.get("/api/portfolio/holdings/lots?ticker=AAPL").json()
    assert any(l["id"] == r.json()["id"] and l["buy_date"] == "2026-03-05" for l in lots)


def test_delete_watchlist_cascades_holdings(lots_app):
    """delete_watchlist must also remove watchlist_holdings for that list —
    there's no FK/ON DELETE CASCADE, so the endpoint has to do it explicitly
    (mirrors what remove_ticker already does for a single ticker)."""
    bm, client = lots_app
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}")

    # Seeded fixture already has two AAPL lots under list_id=1 ("Mine").
    with eng.connect() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM watchlist_holdings WHERE watchlist_id = 1")).scalar()
    assert before == 2

    assert client.delete("/api/watchlists/1").status_code == 204

    with eng.connect() as conn:
        after = conn.execute(text("SELECT COUNT(*) FROM watchlist_holdings WHERE watchlist_id = 1")).scalar()
        list_row = conn.execute(text("SELECT id FROM watchlist_lists WHERE id = 1")).fetchone()
        wl_rows = conn.execute(text("SELECT ticker FROM watchlist WHERE list_id = 1")).fetchall()
    assert after == 0
    assert list_row is None
    assert wl_rows == []
    eng.dispose()
