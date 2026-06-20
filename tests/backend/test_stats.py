"""Tests for GET /api/stats — public marketing stats (no auth).

NOTE (2026-06-20): /api/stats was scoped to Prime (tier=1) signals only.
All existing tests now insert a matching tier-1 BUY signal alongside each
trade_log row so the JOIN in public_stats finds them.  The *semantic intent*
of every test is unchanged — they still verify aggregation arithmetic, DCA
avg-cost, auth, and empty-db behaviour — the only structural change is that
test data must now satisfy the Prime filter.  If a trade lacks a tier-1 signal
it is intentionally excluded (see test_tiers_api.py for that assertion).
"""
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
            CREATE TABLE trade_lots (
                ticker TEXT, buy_date TEXT, sell_date TEXT,
                lot_seq INTEGER, lot_date TEXT, lot_price REAL
            )
        """))
        conn.execute(text("""
            CREATE TABLE signals (
                date TEXT, ticker TEXT, signal TEXT, tier INTEGER
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


def _insert_trade(bm, *, ticker, buy_date, sell_date, return_pct, tier=1):
    """Insert a trade_log row and a matching BUY signal with the given tier.

    Defaults to tier=1 (Prime) so existing call-sites need no change; tests
    that want non-Prime trades can pass tier=2 or tier=3.
    """
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO trade_log (ticker, buy_date, buy_price, sell_date, sell_price, return_pct)
            VALUES (:t, :bd, 100.0, :sd, :sp, :r)
        """), {"t": ticker, "bd": buy_date, "sd": sell_date,
               "sp": 100.0 * (1 + return_pct), "r": return_pct})
        conn.execute(text("""
            INSERT INTO signals (date, ticker, signal, tier)
            VALUES (:bd, :t, 'BUY', :tier)
        """), {"bd": buy_date, "t": ticker, "tier": tier})


def _insert_company(bm, ticker, market="US"):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES (:t, :t, :m)"),
                     {"t": ticker, "m": market})


def _insert_lots(bm, *, ticker, buy_date, sell_date, lot_prices):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        for i, lp in enumerate(lot_prices):
            conn.execute(text("""
                INSERT INTO trade_lots (ticker, buy_date, sell_date, lot_seq, lot_date, lot_price)
                VALUES (:t, :bd, :sd, :seq, :bd, :lp)
            """), {"t": ticker, "bd": buy_date, "sd": sell_date, "seq": i, "lp": lp})


def test_stats_is_public_and_aggregates(stats_app):
    bm, client = stats_app
    # 4 Prime trades: 3 wins, 1 loss → win_rate 75% (yield-based)
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


def test_avg_yield_uses_dca_avg_cost_not_return_pct(stats_app):
    """Multi-lot (DCA) trades must yield against the harmonic-mean avg_cost,
    matching backend.yield_calcs, NOT trade_log.return_pct (which is vs the
    first lot only)."""
    bm, client = stats_app
    _insert_company(bm, "DCA")
    # Two lots at 100 and 50 → avg_cost = 2 / (1/100 + 1/50) = 66.667.
    # Sell at 120 → DCA yield = (120-66.667)/66.667 = 80.0%.
    # trade_log.return_pct is vs first lot (100): (120-100)/100 = 20% — the WRONG number.
    _insert_trade(bm, ticker="DCA", buy_date="2025-01-01 00:00:00",
                  sell_date="2025-01-11 00:00:00", return_pct=0.20)
    _insert_lots(bm, ticker="DCA", buy_date="2025-01-01 00:00:00",
                 sell_date="2025-01-11 00:00:00", lot_prices=[100.0, 50.0])
    # Override sell_price to exactly 120 for a clean assertion.
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("UPDATE trade_log SET sell_price = 120.0 WHERE ticker = 'DCA'"))

    d = client.get("/api/stats").json()
    assert d["closed_trades"] == 1
    assert d["avg_yield"] == pytest.approx(80.0, abs=0.1)   # DCA, not 20%


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
