"""Tests for GET /api/market/movers — top gainers / losers / most-active.

Movers are now ranked from the live snapshot (change% = live vs prev_close)
rather than two daily_prices rows. `active` still ranks by last-session volume
because intraday volume is not in the snapshot.
"""
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


def _company(bm, t, sector="Tech"):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO companies (ticker, company, market, sector) VALUES (:t,:c,'US',:s)"),
                     {"t": t, "c": f"{t} Corp", "s": sector})


def _close(bm, t, close, volume=0, date="2026-05-22 00:00:00"):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO daily_prices (date,ticker,open,high,low,close,volume) VALUES (:d,:t,:c,:c,:c,:c,:v)"),
                     {"d": date, "t": t, "c": close, "v": volume})


def _setup(bm):
    for t, vol in [("AAA", 1_000_000), ("BBB", 5_000_000), ("CCC", 9_000_000),
                   ("DDD", 2_000_000), ("EEE", 3_000_000), ("FFF", 4_000_000)]:
        _company(bm, t)
        _close(bm, t, 100.0, volume=vol)


def test_gainers_ranked_by_live_change(movers_app):
    bm, client = movers_app
    _setup(bm)
    live = {"AAA": 110.0, "BBB": 105.0, "CCC": 100.0, "DDD": 95.0, "EEE": 90.0, "FFF": 80.0}
    from unittest.mock import patch
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        rows = client.get("/api/market/movers?type=gainers&limit=3").json()["movers"]
    assert [r["ticker"] for r in rows] == ["AAA", "BBB", "CCC"]
    assert rows[0]["change_pct"] == 10.0


def test_losers_ranked_by_live_change(movers_app):
    bm, client = movers_app
    _setup(bm)
    live = {"AAA": 110.0, "FFF": 80.0, "EEE": 90.0, "DDD": 95.0, "CCC": 100.0, "BBB": 105.0}
    from unittest.mock import patch
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        rows = client.get("/api/market/movers?type=losers&limit=2").json()["movers"]
    assert [r["ticker"] for r in rows] == ["FFF", "EEE"]


def test_active_ranked_by_last_session_volume(movers_app):
    bm, client = movers_app
    _setup(bm)
    from unittest.mock import patch
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": {}}):
        rows = client.get("/api/market/movers?type=active&limit=2").json()["movers"]
    assert [r["ticker"] for r in rows] == ["CCC", "BBB"]   # 9M, 5M volume


def test_no_live_data_falls_back_to_zero(movers_app):
    bm, client = movers_app
    _setup(bm)
    from unittest.mock import patch
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "idle", "prices": {}}):
        rows = client.get("/api/market/movers?type=gainers&limit=3").json()["movers"]
    assert all(r["change_pct"] == 0.0 for r in rows)


def test_invalid_type_returns_422(movers_app):
    bm, client = movers_app
    resp = client.get("/api/market/movers?type=bogus")
    assert resp.status_code == 422


def test_excludes_spy_and_voo(movers_app):
    bm, client = movers_app
    _setup(bm)                       # AAA..FFF
    _company(bm, "SPY"); _close(bm, "SPY", 100.0, volume=0)
    _company(bm, "VOO"); _close(bm, "VOO", 100.0, volume=0)
    from unittest.mock import patch
    # SPY/VOO get the biggest live gain; must still be excluded from movers
    live = {"SPY": 200.0, "VOO": 200.0, "AAA": 110.0, "BBB": 105.0}
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        rows = client.get("/api/market/movers?type=gainers&limit=10").json()["movers"]
    tickers = [r["ticker"] for r in rows]
    assert "SPY" not in tickers
    assert "VOO" not in tickers
    assert "AAA" in tickers


def test_excludes_non_us_market(movers_app):
    bm, client = movers_app
    _setup(bm)
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO companies (ticker, company, market, sector) VALUES ('TEVA','Teva','IL','Health')"))
        conn.execute(text("INSERT INTO daily_prices (date,ticker,open,high,low,close,volume) VALUES ('2026-05-22 00:00:00','TEVA',100,100,100,100,1)"))
    from unittest.mock import patch
    live = {"TEVA": 200.0, "AAA": 110.0}
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        rows = client.get("/api/market/movers?type=gainers&limit=10").json()["movers"]
    assert "TEVA" not in [r["ticker"] for r in rows]


def test_default_limit_is_five(movers_app):
    bm, client = movers_app
    _setup(bm)                       # 6 tickers
    from unittest.mock import patch
    live = {"AAA": 110.0, "BBB": 109.0, "CCC": 108.0, "DDD": 107.0, "EEE": 106.0, "FFF": 105.0}
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": live}):
        rows = client.get("/api/market/movers?type=gainers").json()["movers"]
    assert len(rows) == 5
