"""Tests for GET /api/stats — public marketing stats (no auth)."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def stats_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_stats.db")
    os.environ["DB_PATH"] = db_path
    # Intentionally NO BYPASS_AUTH — /api/stats must be public.
    os.environ.pop("BYPASS_AUTH", None)

    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE trade_log (
                ticker TEXT, buy_date TEXT, buy_price REAL,
                sell_date TEXT, sell_price REAL, return_pct REAL
            )
        """))
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


def _insert_trade(bm, *, ticker, buy_date, sell_date, return_pct):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO trade_log (ticker, buy_date, buy_price, sell_date, sell_price, return_pct)
            VALUES (:t, :bd, 100.0, :sd, :sp, :r)
        """), {"t": ticker, "bd": buy_date, "sd": sell_date,
               "sp": 100.0 * (1 + return_pct), "r": return_pct})


def _insert_company(bm, ticker, market="US"):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES (:t, :t, :m)"),
                     {"t": ticker, "m": market})


def test_stats_is_public_and_aggregates(stats_app):
    bm, client = stats_app
    # 4 trades: 3 wins, 1 loss → win_rate 75%
    _insert_trade(bm, ticker="AAA", buy_date="2025-01-01 00:00:00", sell_date="2025-01-11 00:00:00", return_pct=0.10)  # +10%, 10d
    _insert_trade(bm, ticker="BBB", buy_date="2025-01-01 00:00:00", sell_date="2025-01-21 00:00:00", return_pct=0.20)  # +20%, 20d
    _insert_trade(bm, ticker="CCC", buy_date="2025-01-01 00:00:00", sell_date="2025-01-31 00:00:00", return_pct=0.30)  # +30%, 30d
    _insert_trade(bm, ticker="DDD", buy_date="2025-01-01 00:00:00", sell_date="2025-01-21 00:00:00", return_pct=-0.10) # -10%, 20d
    for t in ["AAA", "BBB", "CCC", "DDD", "EEE"]:
        _insert_company(bm, t)

    resp = client.get("/api/stats")
    assert resp.status_code == 200          # public — no auth needed
    d = resp.json()
    assert d["closed_trades"] == 4
    assert d["win_rate"] == pytest.approx(75.0, abs=1e-6)
    # avg yield = (10+20+30-10)/4 = 12.5%
    assert d["avg_yield"] == pytest.approx(12.5, abs=1e-6)
    # avg hold = (10+20+30+20)/4 = 20 days
    assert d["avg_hold_days"] == pytest.approx(20, abs=0.5)
    assert d["tickers_tracked"] == 5


def test_tickers_tracked_excludes_non_us(stats_app):
    bm, client = stats_app
    _insert_company(bm, "USA1", market="US")
    _insert_company(bm, "TEVA.TA", market="TASE")
    d = client.get("/api/stats").json()
    assert d["tickers_tracked"] == 1


def test_empty_db_returns_zeros(stats_app):
    bm, client = stats_app
    d = client.get("/api/stats").json()
    assert d["closed_trades"] == 0
    assert d["win_rate"] is None
    assert d["tickers_tracked"] == 0
