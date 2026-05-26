"""Tests for GET /api/market/breadth — market-internals snapshot."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def breadth_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_breadth.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE daily_prices (
                date DATETIME, ticker TEXT,
                open FLOAT, high FLOAT, low FLOAT, close FLOAT,
                volume BIGINT
            )
        """))
        conn.execute(text("""
            CREATE TABLE companies (
                ticker TEXT PRIMARY KEY, company TEXT, domain TEXT,
                market TEXT, logo_url TEXT, industry TEXT,
                sector TEXT,
                description TEXT, description_short TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE company_health_history (
                ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT
            )
        """))
        conn.execute(text("CREATE TABLE fundamentals (ticker TEXT, market_cap REAL)"))
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


def test_breadth_from_live_snapshot(breadth_app):
    bm, client = breadth_app
    from sqlalchemy import text
    # Each ticker: older close 90 (05-21) + latest close 100 (05-22, volume 1000).
    # => prev_close=100, hi52=100, lo52=90, ma50=ma200=95.
    with bm.engine.begin() as conn:
        for t in ["AAA", "BBB", "CCC", "DDD"]:
            conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES (:t,:t,'US')"), {"t": t})
            conn.execute(text("INSERT INTO daily_prices (date,ticker,open,high,low,close,volume) VALUES ('2026-05-21 00:00:00',:t,90,90,90,90,0)"), {"t": t})
            conn.execute(text("INSERT INTO daily_prices (date,ticker,open,high,low,close,volume) VALUES ('2026-05-22 00:00:00',:t,100,100,100,100,1000)"), {"t": t})
    live = {"AAA": 110.0, "BBB": 105.0, "CCC": 95.0, "DDD": 100.0}  # 2 up, 1 down, 1 flat
    from unittest.mock import patch
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        body = client.get("/api/market/breadth").json()
    assert body["advancers"] == 2          # AAA, BBB
    assert body["decliners"] == 1          # CCC (DDD flat -> neither)
    assert body["up_volume"] == 2000       # AAA + BBB latest-session volume
    assert body["down_volume"] == 1000     # CCC
    assert body["week52_highs"] == 3       # AAA(110), BBB(105), DDD(100) all >= hi52 100
    assert body["week52_lows"] == 0        # none <= lo52 90
    assert body["above_50d_ma_pct"] == 0.75  # AAA,BBB,DDD > ma 95; CCC(95) not > 95


def test_breadth_idle_empty_snapshot(breadth_app):
    bm, client = breadth_app
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES ('AAA','AAA','US')"))
        conn.execute(text("INSERT INTO daily_prices (date,ticker,open,high,low,close,volume) VALUES ('2026-05-22 00:00:00','AAA',100,100,100,100,5)"))
    from unittest.mock import patch
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "idle", "prices": {}}):
        body = client.get("/api/market/breadth").json()
    # No live price -> price == prev_close -> change 0 -> neither advancer nor decliner
    assert body["advancers"] == 0
    assert body["decliners"] == 0
