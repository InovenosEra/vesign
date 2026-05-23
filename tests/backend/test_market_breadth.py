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


def _insert_company(bm, ticker, market="US"):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO companies (ticker, company, market) VALUES (:t, :t, :m)
        """), {"t": ticker, "m": market})


def _insert_series(bm, ticker, closes_by_date):
    """closes_by_date: dict[date_str, close]"""
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        for d, c in closes_by_date.items():
            conn.execute(text("""
                INSERT INTO daily_prices (date, ticker, open, high, low, close, volume)
                VALUES (:d, :t, :c, :c, :c, :c, 0)
            """), {"d": d, "t": ticker, "c": c})


def test_advancers_decliners_count_against_prior_day(breadth_app):
    bm, client = breadth_app
    # 3 advancers, 2 decliners, 1 flat
    for t, prev, today in [
        ("UP1", 100, 110),
        ("UP2", 100, 105),
        ("UP3", 100, 101),
        ("DN1", 100,  99),
        ("DN2", 100,  90),
        ("FLT", 100, 100),
    ]:
        _insert_company(bm, t)
        _insert_series(bm, t, {"2026-05-21 00:00:00": prev, "2026-05-22 00:00:00": today})

    body = client.get("/api/market/breadth").json()
    assert body["advancers"] == 3
    assert body["decliners"] == 2


def test_week52_highs_and_lows(breadth_app):
    bm, client = breadth_app
    # HI: today's close = max of the trailing year.
    _insert_company(bm, "HI")
    _insert_series(bm, "HI", {
        "2026-05-20 00:00:00": 150,
        "2026-05-21 00:00:00": 140,
        "2026-05-22 00:00:00": 160,  # new 52w high
    })
    # LO: today's close = min of the trailing year.
    _insert_company(bm, "LO")
    _insert_series(bm, "LO", {
        "2026-05-20 00:00:00": 50,
        "2026-05-21 00:00:00": 60,
        "2026-05-22 00:00:00": 40,  # new 52w low
    })
    # MID: today is neither hi nor lo.
    _insert_company(bm, "MID")
    _insert_series(bm, "MID", {
        "2026-05-20 00:00:00": 100,
        "2026-05-21 00:00:00": 80,
        "2026-05-22 00:00:00": 90,
    })

    body = client.get("/api/market/breadth").json()
    assert body["week52_highs"] == 1
    assert body["week52_lows"] == 1


def test_above_50d_ma_pct(breadth_app):
    bm, client = breadth_app
    # ABV: 50 days of 100, today = 110 → above MA
    # BLW: 50 days of 100, today =  90 → below MA
    for t, today_close in [("ABV", 110), ("BLW", 90)]:
        _insert_company(bm, t)
        series = {}
        for i in range(1, 51):
            series[f"2026-03-{i:02d} 00:00:00" if i <= 31 else f"2026-04-{i-31:02d} 00:00:00"] = 100.0
        series["2026-05-22 00:00:00"] = today_close
        _insert_series(bm, t, series)

    body = client.get("/api/market/breadth").json()
    # 1 of 2 US tickers above its 50d MA → 0.5
    assert body["above_50d_ma_pct"] == pytest.approx(0.5, abs=1e-6)


def test_us_only_filter(breadth_app):
    bm, client = breadth_app
    _insert_company(bm, "US1", market="US")
    _insert_series(bm, "US1", {"2026-05-21 00:00:00": 100, "2026-05-22 00:00:00": 110})
    _insert_company(bm, "TA1.TA", market="TASE")
    _insert_series(bm, "TA1.TA", {"2026-05-21 00:00:00": 100, "2026-05-22 00:00:00": 110})

    body = client.get("/api/market/breadth").json()
    assert body["advancers"] == 1  # TASE excluded
