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
        conn.execute(text("CREATE TABLE holdings (id INTEGER PRIMARY KEY, user_id TEXT, ticker TEXT, quantity REAL, buy_price REAL, buy_date TEXT)"))
        conn.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT, industry TEXT, description TEXT, description_short TEXT, logo_url TEXT, domain TEXT)"))
        conn.execute(text("CREATE TABLE watchlist (list_id INTEGER, ticker TEXT, note TEXT)"))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        conn.execute(text("INSERT INTO watchlist_lists (id, user_id, name) VALUES (1,'dev-bypass','Mine'), (2,'someone-else','Theirs')"))
        conn.execute(text("INSERT INTO watchlist (list_id, ticker, note) VALUES (1, 'AAPL', '')"))
        conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES ('AAPL','Apple','US'), ('MSFT','Microsoft','US')"))
        conn.execute(text("INSERT INTO holdings (user_id,ticker,quantity,buy_price,buy_date) VALUES ('dev-bypass','AAPL',10,100.0,'2026-01-01'), ('dev-bypass','AAPL',5,120.0,'2026-02-01'), ('someone-else','MSFT',3,300.0,'2026-01-01')"))
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
    assert all("id" in r for r in rows)


def test_lots_excludes_other_users(lots_app):
    bm, client = lots_app
    assert client.get("/api/portfolio/holdings/lots?ticker=MSFT").json() == []


def test_get_user_holdings_scoped_to_current_user(lots_app):
    bm, client = lots_app
    rows = client.get("/api/holdings").json()
    assert {r["ticker"] for r in rows} == {"AAPL"}   # not MSFT (seeded under 'someone-else')
    assert sum(r["quantity"] for r in rows) == 15     # the two seeded AAPL lots, 10 + 5


def test_lots_empty_ticker(lots_app):
    bm, client = lots_app
    assert client.get("/api/portfolio/holdings/lots?ticker=").json() == []


def test_add_holding_rejects_bad_input(lots_app):
    bm, client = lots_app
    base = {"ticker": "AAPL", "quantity": 1, "buy_price": 100.0, "buy_date": "2026-01-01"}
    assert client.post("/api/holdings", json={**base, "quantity": 0}).status_code == 400
    assert client.post("/api/holdings", json={**base, "buy_price": -5}).status_code == 400
    assert client.post("/api/holdings", json={**base, "buy_date": "2099-01-01"}).status_code == 400
    assert client.post("/api/holdings", json={**base, "ticker": "NOPE"}).status_code == 400


def test_add_then_delete_lot_roundtrip(lots_app):
    bm, client = lots_app
    r = client.post("/api/holdings",
                    json={"ticker": "msft", "quantity": 2, "buy_price": 310.0, "buy_date": "2026-03-01"})
    assert r.status_code == 201
    hid = r.json()["id"]
    lots = client.get("/api/portfolio/holdings/lots?ticker=MSFT").json()
    assert any(l["id"] == hid and l["ticker"] == "MSFT" for l in lots)
    assert client.delete(f"/api/holdings/{hid}").status_code == 204
    assert client.get("/api/portfolio/holdings/lots?ticker=MSFT").json() == []


def test_add_holding_normalizes_date(lots_app):
    bm, client = lots_app
    r = client.post("/api/holdings",
                    json={"ticker": "AAPL", "quantity": 1, "buy_price": 50.0, "buy_date": "2026-3-5"})
    assert r.status_code == 201
    lots = client.get("/api/portfolio/holdings/lots?ticker=AAPL").json()
    assert any(l["id"] == r.json()["id"] and l["buy_date"] == "2026-03-05" for l in lots)


def test_delete_watchlist_does_not_touch_holdings(lots_app):
    """Holdings are independent of watchlist_id now — deleting a watchlist
    must only remove ticker membership, never the user's actual positions."""
    bm, client = lots_app
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}")

    with eng.connect() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM holdings WHERE user_id = 'dev-bypass'")).scalar()
    assert before == 2

    assert client.delete("/api/watchlists/1").status_code == 204

    with eng.connect() as conn:
        after = conn.execute(text("SELECT COUNT(*) FROM holdings WHERE user_id = 'dev-bypass'")).scalar()
        list_row = conn.execute(text("SELECT id FROM watchlist_lists WHERE id = 1")).fetchone()
        wl_rows = conn.execute(text("SELECT ticker FROM watchlist WHERE list_id = 1")).fetchall()
    assert after == 2          # unchanged — holdings survive
    assert list_row is None    # list itself is gone
    assert wl_rows == []       # ticker membership for that list is gone
    eng.dispose()


def test_remove_ticker_does_not_error_and_removes_membership(lots_app):
    """remove_ticker (DELETE /api/watchlists/{list_id}/tickers/{ticker})
    must remove ticker from watchlist table but NOT touch holdings."""
    bm, client = lots_app
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}")

    with eng.connect() as conn:
        before_wl = conn.execute(text("SELECT COUNT(*) FROM watchlist WHERE list_id = 1 AND ticker = 'AAPL'")).scalar()
        before_holdings = conn.execute(text("SELECT COUNT(*) FROM holdings WHERE user_id = 'dev-bypass' AND ticker = 'AAPL'")).scalar()
    assert before_wl == 1      # AAPL is in watchlist 1
    assert before_holdings == 2  # dev-bypass has 2 AAPL lots

    # Remove the ticker from the watchlist
    assert client.delete("/api/watchlists/1/tickers/AAPL").status_code == 204

    with eng.connect() as conn:
        after_wl = conn.execute(text("SELECT COUNT(*) FROM watchlist WHERE list_id = 1 AND ticker = 'AAPL'")).scalar()
        after_holdings = conn.execute(text("SELECT COUNT(*) FROM holdings WHERE user_id = 'dev-bypass' AND ticker = 'AAPL'")).scalar()
    assert after_wl == 0       # ticker removed from watchlist
    assert after_holdings == 2  # holdings unchanged — the fix ensures we don't touch this table
    eng.dispose()
