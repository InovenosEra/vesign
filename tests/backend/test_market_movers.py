"""Tests for GET /api/market/movers — top gainers / losers / most-active."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def movers_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_movers.db")
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
                description TEXT, description_short TEXT
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


def _insert_company(bm, ticker, company, market="US"):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO companies (ticker, company, market)
            VALUES (:t, :c, :m)
        """), {"t": ticker, "c": company, "m": market})


def _insert_price(bm, *, date, ticker, close, volume=0):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO daily_prices (date, ticker, open, high, low, close, volume)
            VALUES (:date, :ticker, :close, :close, :close, :close, :volume)
        """), dict(date=date, ticker=ticker, close=close, volume=volume))


def _setup_two_day_universe(bm):
    """Insert prev/today closes for 6 tickers spanning gainers/losers."""
    # ticker, prev_close, today_close, today_volume
    rows = [
        ("AAA", 100.0, 110.0, 1_000_000),   # +10.00%
        ("BBB", 100.0, 105.0, 5_000_000),   #  +5.00%
        ("CCC", 100.0, 100.0, 9_000_000),   #   0.00%, biggest volume
        ("DDD", 100.0,  95.0, 2_000_000),   #  -5.00%
        ("EEE", 100.0,  90.0, 3_000_000),   # -10.00%
        ("FFF", 100.0,  80.0, 4_000_000),   # -20.00%
    ]
    for t, prev, today, vol in rows:
        _insert_company(bm, t, f"{t} Corp")
        _insert_price(bm, date="2026-05-21 00:00:00", ticker=t, close=prev, volume=0)
        _insert_price(bm, date="2026-05-22 00:00:00", ticker=t, close=today, volume=vol)


def test_gainers_sorted_by_change_pct_desc(movers_app):
    bm, client = movers_app
    _setup_two_day_universe(bm)

    body = client.get("/api/market/movers?type=gainers&limit=3").json()
    rows = body["movers"]
    assert len(rows) == 3
    assert [r["ticker"] for r in rows] == ["AAA", "BBB", "CCC"]
    assert rows[0]["change_pct"] == pytest.approx(10.0, abs=1e-3)
    assert rows[0]["close"] == pytest.approx(110.0)


def test_losers_sorted_by_change_pct_asc(movers_app):
    bm, client = movers_app
    _setup_two_day_universe(bm)

    body = client.get("/api/market/movers?type=losers&limit=3").json()
    rows = body["movers"]
    assert [r["ticker"] for r in rows] == ["FFF", "EEE", "DDD"]
    assert rows[0]["change_pct"] == pytest.approx(-20.0, abs=1e-3)


def test_active_sorted_by_volume_desc(movers_app):
    bm, client = movers_app
    _setup_two_day_universe(bm)

    body = client.get("/api/market/movers?type=active&limit=3").json()
    rows = body["movers"]
    # Top 3 by volume: CCC(9M), BBB(5M), FFF(4M)
    assert [r["ticker"] for r in rows] == ["CCC", "BBB", "FFF"]
    assert rows[0]["volume"] == 9_000_000


def test_excludes_spy_and_voo(movers_app):
    bm, client = movers_app
    _insert_company(bm, "SPY", "S&P 500 ETF")
    _insert_company(bm, "VOO", "Vanguard S&P 500 ETF")
    _insert_company(bm, "AAA", "Alpha")
    _insert_price(bm, date="2026-05-21 00:00:00", ticker="SPY", close=100.0)
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="SPY", close=130.0)  # +30%
    _insert_price(bm, date="2026-05-21 00:00:00", ticker="VOO", close=100.0)
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="VOO", close=125.0)  # +25%
    _insert_price(bm, date="2026-05-21 00:00:00", ticker="AAA", close=100.0)
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="AAA", close=101.0)  # +1%

    body = client.get("/api/market/movers?type=gainers&limit=5").json()
    tickers = [r["ticker"] for r in body["movers"]]
    assert "SPY" not in tickers
    assert "VOO" not in tickers
    assert tickers == ["AAA"]


def test_excludes_non_us_market(movers_app):
    bm, client = movers_app
    _insert_company(bm, "TEVA.TA", "Teva Israel", market="TASE")
    _insert_company(bm, "AAA", "Alpha", market="US")
    _insert_price(bm, date="2026-05-21 00:00:00", ticker="TEVA.TA", close=100.0)
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="TEVA.TA", close=200.0)  # +100%
    _insert_price(bm, date="2026-05-21 00:00:00", ticker="AAA", close=100.0)
    _insert_price(bm, date="2026-05-22 00:00:00", ticker="AAA", close=101.0)

    body = client.get("/api/market/movers?type=gainers&limit=5").json()
    assert [r["ticker"] for r in body["movers"]] == ["AAA"]


def test_default_limit_is_five(movers_app):
    bm, client = movers_app
    _setup_two_day_universe(bm)  # 6 tickers — should be capped at 5
    body = client.get("/api/market/movers?type=gainers").json()
    assert len(body["movers"]) == 5


def test_invalid_type_returns_422(movers_app):
    bm, client = movers_app
    resp = client.get("/api/market/movers?type=bogus")
    assert resp.status_code == 422
