"""Tests for backend.main._get_universe_baseline — daily-cached universe meta."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def base_app():
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "test_baseline.db")
    os.environ["BYPASS_AUTH"] = "1"
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}", poolclass=None)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
        conn.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT, industry TEXT, description TEXT, description_short TEXT, logo_url TEXT, domain TEXT)"))
        conn.execute(text("CREATE TABLE fundamentals (ticker TEXT, market_cap REAL)"))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        conn.execute(text("INSERT INTO companies (ticker, company, sector, market) VALUES ('AAA','AAA Corp','Tech','US')"))
        conn.execute(text("INSERT INTO fundamentals (ticker, market_cap) VALUES ('AAA', 1000)"))
        for d, c in [("2026-05-20 00:00:00", 120.0), ("2026-05-21 00:00:00", 90.0), ("2026-05-22 00:00:00", 100.0)]:
            conn.execute(text("INSERT INTO daily_prices (date,ticker,open,high,low,close,volume) VALUES (:d,'AAA',:c,:c,:c,:c,5)"), {"d": d, "c": c})
        conn.execute(text("INSERT INTO companies (ticker, company, sector, market) VALUES ('ZZZ','ZZZ Ltd','Tech','IL')"))
        conn.execute(text("INSERT INTO daily_prices (date,ticker,open,high,low,close,volume) VALUES ('2026-05-22 00:00:00','ZZZ',5,5,5,5,1)"))
    eng.dispose()
    import importlib, backend.main as bm
    importlib.reload(bm)
    yield bm, TestClient(bm.app)
    for f in os.listdir(tmpdir):
        try: os.remove(os.path.join(tmpdir, f))
        except OSError: pass
    os.rmdir(tmpdir)


def test_baseline_prev_close_is_latest_session_and_meta(base_app):
    bm, _ = base_app
    base = bm._get_universe_baseline()
    assert "AAA" in base
    assert "ZZZ" not in base
    assert base["AAA"]["prev_close"] == 100.0
    assert base["AAA"]["sector"] == "Tech"
    assert base["AAA"]["market_cap"] == 1000
    assert base["AAA"]["hi52"] == 120.0
    assert base["AAA"]["lo52"] == 90.0
