"""Tests for GET /api/market/sectors — market-cap-weighted sector heatmap."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch


@pytest.fixture
def sectors_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_market_sectors.db")
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
                ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT,
                industry TEXT, description TEXT, description_short TEXT,
                logo_url TEXT, domain TEXT
            )
        """))
        conn.execute(text("""
            CREATE TABLE fundamentals (ticker TEXT, market_cap REAL)
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


def _insert_company(bm, ticker, sector, market="US"):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO companies (ticker, company, sector, market)
            VALUES (:t, :t, :s, :m)
        """), {"t": ticker, "s": sector, "m": market})


def _insert_mc(bm, ticker, market_cap):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO fundamentals (ticker, market_cap) VALUES (:t, :mc)
        """), {"t": ticker, "mc": market_cap})


def _insert_base(bm, ticker, close=100.0):
    """One daily row at the latest date -> baseline prev_close = close."""
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO daily_prices (date, ticker, open, high, low, close, volume)
            VALUES ('2026-05-22 00:00:00', :t, :c, :c, :c, :c, 0)
        """), {"t": ticker, "c": close})


def test_returns_one_row_per_sector_with_weighted_change_and_top_movers(sectors_app):
    bm, client = sectors_app
    # baseline prev_close=100 for all; live snapshot encodes the change% (live - 100)
    tech = [("NVDA", 5_000_000_000_000, 102.0), ("MSFT", 3_000_000_000_000, 99.0), ("AAPL", 4_000_000_000_000, 100.5)]
    energy = [("XOM", 500_000_000_000, 99.7), ("CVX", 300_000_000_000, 99.5)]
    for t, mc, _ in tech:
        _insert_company(bm, t, "Information Technology"); _insert_mc(bm, t, mc); _insert_base(bm, t)
    for t, mc, _ in energy:
        _insert_company(bm, t, "Energy"); _insert_mc(bm, t, mc); _insert_base(bm, t)
    live = {t: lv for t, _, lv in tech + energy}
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        body = client.get("/api/market/sectors").json()
    by_sector = {row["sector"]: row for row in body["sectors"]}
    assert set(by_sector.keys()) == {"Information Technology", "Energy"}
    # Tech weighted: (5T*2 + 3T*-1 + 4T*0.5)/12T = 9/12 = 0.75
    assert by_sector["Information Technology"]["change_pct"] == pytest.approx(0.75, abs=1e-3)
    # Energy weighted: (500B*-0.3 + 300B*-0.5)/800B = -0.375
    assert by_sector["Energy"]["change_pct"] == pytest.approx(-0.375, abs=1e-3)
    tech_movers = by_sector["Information Technology"]["top_movers"]
    assert [m["ticker"] for m in tech_movers] == ["NVDA", "MSFT", "AAPL"]
    assert tech_movers[0]["change_pct"] == pytest.approx(2.0, abs=1e-3)


def test_top_movers_capped_at_three(sectors_app):
    bm, client = sectors_app
    changes = [("A", 5.0), ("B", 4.0), ("C", 3.0), ("D", 2.0), ("E", 1.0)]
    for t, ch in changes:
        _insert_company(bm, t, "Industrials"); _insert_mc(bm, t, 100_000_000); _insert_base(bm, t)
    live = {t: 100.0 + ch for t, ch in changes}   # change% = (100+ch-100)/100*100 = ch
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        body = client.get("/api/market/sectors").json()
    movers = body["sectors"][0]["top_movers"]
    assert len(movers) == 3
    assert [m["ticker"] for m in movers] == ["A", "B", "C"]


def test_excludes_non_us_market(sectors_app):
    bm, client = sectors_app
    _insert_company(bm, "TEVA.TA", "Health Care", market="TASE"); _insert_mc(bm, "TEVA.TA", 100_000_000); _insert_base(bm, "TEVA.TA")
    _insert_company(bm, "JNJ", "Health Care", market="US"); _insert_mc(bm, "JNJ", 1_000_000_000); _insert_base(bm, "JNJ")
    live = {"TEVA.TA": 110.0, "JNJ": 101.0}   # TEVA excluded upstream (non-US baseline)
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        body = client.get("/api/market/sectors").json()
    hc = next(row for row in body["sectors"] if row["sector"] == "Health Care")
    assert hc["change_pct"] == pytest.approx(1.0, abs=1e-3)   # JNJ alone
    assert "TEVA.TA" not in [m["ticker"] for m in hc["top_movers"]]


def test_skips_sectors_with_null_label(sectors_app):
    """Companies whose sector is NULL shouldn't create a 'None' bucket."""
    bm, client = sectors_app
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO companies (ticker, company, sector, market) VALUES ('UNK','Unknown',NULL,'US')"))
    _insert_mc(bm, "UNK", 100_000_000); _insert_base(bm, "UNK")
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": {"UNK": 110.0}}):
        body = client.get("/api/market/sectors").json()
    assert body["sectors"] == []


def _seed_live(bm):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        for t, sector, mc in [("AAA", "Tech", 1000), ("BBB", "Tech", 1000), ("CCC", "Energy", 500)]:
            conn.execute(text("INSERT INTO companies (ticker,company,sector,market) VALUES (:t,:c,:s,'US')"),
                         {"t": t, "c": f"{t} Corp", "s": sector})
            conn.execute(text("INSERT INTO fundamentals (ticker, market_cap) VALUES (:t,:mc)"), {"t": t, "mc": mc})
            conn.execute(text("INSERT INTO daily_prices (date,ticker,open,high,low,close,volume) VALUES ('2026-05-22 00:00:00',:t,100,100,100,100,1)"),
                         {"t": t})


def test_sector_change_uses_live_prices(sectors_app):
    bm, client = sectors_app
    _seed_live(bm)
    live = {"AAA": 110.0, "BBB": 110.0, "CCC": 90.0}   # Tech +10%, Energy -10%
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        sectors = client.get("/api/market/sectors").json()["sectors"]
    by = {s["sector"]: s for s in sectors}
    assert round(by["Tech"]["change_pct"], 2) == 10.0
    assert round(by["Energy"]["change_pct"], 2) == -10.0
