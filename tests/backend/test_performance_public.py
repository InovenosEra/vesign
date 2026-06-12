"""Tests for the public /api/performance/* endpoints (no auth) — equity curve
and full trade ledger backing the public /performance page."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def perf_app():
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_perf.db")
    os.environ["DB_PATH"] = db_path
    # Intentionally NO BYPASS_AUTH — these endpoints must be public.
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
        conn.execute(text("""
            CREATE TABLE daily_prices (
                date TEXT, ticker TEXT, open REAL, high REAL, low REAL,
                close REAL, volume INTEGER
            )
        """))
        # Two closed US trades (one winner, one loser) + an excluded .TA trade.
        conn.execute(text("""
            INSERT INTO trade_log VALUES
                ('AAA', '2024-01-02', 100.0, '2024-03-01', 120.0, 0.20),
                ('BBB', '2024-02-01', 50.0,  '2024-04-01', 45.0, -0.10),
                ('XX.TA', '2024-01-02', 10.0, '2024-02-01', 12.0, 0.20)
        """))
        for t in ("AAA", "BBB"):
            conn.execute(text(
                "INSERT INTO companies (ticker, company, market) VALUES (:t, :t, 'US')"
            ), {"t": t})
        # Sparse daily prices: enough for MTM lookups + the SPY benchmark.
        for d, aaa, bbb, spy in [
            ("2024-01-02", 100.0, 48.0, 400.0),
            ("2024-02-01", 110.0, 50.0, 410.0),
            ("2024-03-01", 120.0, 47.0, 420.0),
            ("2024-04-01", 121.0, 45.0, 430.0),
        ]:
            conn.execute(text(
                "INSERT INTO daily_prices VALUES (:d, 'AAA', :a, :a, :a, :a, 0)"
            ), {"d": d, "a": aaa})
            conn.execute(text(
                "INSERT INTO daily_prices VALUES (:d, 'BBB', :b, :b, :b, :b, 0)"
            ), {"d": d, "b": bbb})
            conn.execute(text(
                "INSERT INTO daily_prices VALUES (:d, 'SPY', :s, :s, :s, :s, 0)"
            ), {"d": d, "s": spy})
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


def test_ledger_is_public_full_and_us_only(perf_app):
    _, client = perf_app
    res = client.get("/api/performance/ledger")
    assert res.status_code == 200
    body = res.json()
    assert body["total"] == 2  # .TA excluded
    tickers = [t["ticker"] for t in body["trades"]]
    assert tickers == ["BBB", "AAA"]  # sell_date DESC
    returns = [t["return_pct"] for t in body["trades"]]
    assert any(r > 0 for r in returns) and any(r < 0 for r in returns)
    # pagination
    res2 = client.get("/api/performance/ledger?limit=1&offset=1")
    assert res2.status_code == 200
    assert res2.json()["total"] == 2
    assert len(res2.json()["trades"]) == 1


def test_equity_curve_is_public_aligned_and_normalized(perf_app):
    _, client = perf_app
    res = client.get("/api/performance/equity-curve")
    assert res.status_code == 200
    body = res.json()
    assert body["start"] == "2024-01-02"
    pts = body["points"]
    assert len(pts) >= 2
    # both series normalized to 0 at the start
    assert pts[0]["model"] == 0.0
    assert pts[0]["spy"] == 0.0
    # SPY series follows daily_prices: 400 → 430 = +7.5% at the end
    spy_vals = [p["spy"] for p in pts if p["spy"] is not None]
    assert spy_vals[-1] == pytest.approx(7.5, abs=0.01)
    # model series exists and ends non-null (both trades closed inside range)
    model_vals = [p["model"] for p in pts if p["model"] is not None]
    assert len(model_vals) >= 1
