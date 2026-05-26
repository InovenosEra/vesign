"""Tests for GET /api/market/highs-lows — stocks at/near 52-week high or low."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def hl_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_hl.db")
    os.environ["DB_PATH"] = db_path
    os.environ["BYPASS_AUTH"] = "1"

    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
        conn.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT, industry TEXT, description TEXT, description_short TEXT, logo_url TEXT, domain TEXT)"))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        conn.execute(text("CREATE TABLE fundamentals (ticker TEXT, market_cap REAL)"))
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


def _company(bm, t):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES (:t, :t, 'US')"), {"t": t})


def _series(bm, t, closes):
    """closes is [(date, close), ...]; newest date should be 2026-05-22 (the last close)."""
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        for d, c in closes:
            conn.execute(text("INSERT INTO daily_prices (date, ticker, open, high, low, close, volume) VALUES (:d, :t, :c, :c, :c, :c, 0)"),
                         {"d": d, "t": t, "c": c})


D = ["2026-05-20 00:00:00", "2026-05-21 00:00:00", "2026-05-22 00:00:00"]


def test_high_list_includes_stocks_at_52w_high(hl_app):
    bm, client = hl_app
    _company(bm, "ATHIGH"); _company(bm, "MIDDLE")
    _series(bm, "ATHIGH", list(zip(D, [80.0, 90.0, 100.0])))   # hi52 = 100
    _series(bm, "MIDDLE", list(zip(D, [100.0, 60.0, 70.0])))   # hi52 = 100
    from unittest.mock import patch
    live = {"ATHIGH": 100.0, "MIDDLE": 70.0}                    # ATHIGH at high; MIDDLE 30% below
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        tk = [r["ticker"] for r in client.get("/api/market/highs-lows?type=high&limit=10").json()["movers"]]
    assert "ATHIGH" in tk
    assert "MIDDLE" not in tk


def test_low_list_includes_stocks_at_52w_low(hl_app):
    bm, client = hl_app
    _company(bm, "ATLOW"); _company(bm, "MIDDLE")
    _series(bm, "ATLOW", list(zip(D, [80.0, 60.0, 50.0])))     # lo52 = 50
    _series(bm, "MIDDLE", list(zip(D, [40.0, 100.0, 70.0])))   # lo52 = 40
    from unittest.mock import patch
    live = {"ATLOW": 50.0, "MIDDLE": 70.0}                      # ATLOW at low; MIDDLE 75% above
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        tk = [r["ticker"] for r in client.get("/api/market/highs-lows?type=low&limit=10").json()["movers"]]
    assert "ATLOW" in tk
    assert "MIDDLE" not in tk


def test_etf_excluded(hl_app):
    bm, client = hl_app
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO companies (ticker, company, sector, market) VALUES ('SPY','SPDR','ETF','US')"))
    _company(bm, "STK")
    _series(bm, "SPY", list(zip(D, [80.0, 90.0, 100.0])))
    _series(bm, "STK", list(zip(D, [80.0, 90.0, 100.0])))
    from unittest.mock import patch
    live = {"SPY": 100.0, "STK": 100.0}
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        tk = [r["ticker"] for r in client.get("/api/market/highs-lows?type=high&limit=10").json()["movers"]]
    assert "SPY" not in tk
    assert "STK" in tk


def test_live_price_makes_new_high(hl_app):
    """A live price above the prior 52w high registers as at-high even though the
    last daily close was below it."""
    bm, client = hl_app
    _company(bm, "GAPUP")
    _series(bm, "GAPUP", list(zip(D, [80.0, 90.0, 95.0])))     # hi52 = 95, last close 95
    from unittest.mock import patch
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": {"GAPUP": 99.0}}):
        tk = [r["ticker"] for r in client.get("/api/market/highs-lows?type=high&limit=10").json()["movers"]]
    assert "GAPUP" in tk     # 99 >= 95 * 0.98
