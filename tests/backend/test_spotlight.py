"""Tests for backend Spotlight endpoint."""
import os
import tempfile
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def spotlight_app():
    """Build an isolated FastAPI app over a temp SQLite DB.

    Each test gets a fresh `signals` + `companies` table so cache state and
    schema setup never leak between tests.
    """
    tmpdir = tempfile.mkdtemp()
    db_path = os.path.join(tmpdir, "test_spotlight.db")
    os.environ["DB_PATH"] = db_path

    # Create tables BEFORE importing backend.main (which calls _ensure_indexes at module load)
    from sqlalchemy import create_engine, text
    temp_engine = create_engine(f"sqlite:///{db_path}", poolclass=None)
    with temp_engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE signals (
                date TEXT, ticker TEXT, signal TEXT,
                close REAL, rsi REAL, rsi_3day_flag INTEGER,
                bb_condition INTEGER, analyst_condition INTEGER,
                volume_flag INTEGER, week52_condition INTEGER,
                health_condition INTEGER, ml_condition INTEGER,
                vqs INTEGER, pred_5d REAL, prediction_score REAL,
                lot_seq INTEGER, fair_value_upside REAL,
                target_mean_price REAL, target_low_price REAL, target_high_price REAL,
                health_score INTEGER
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

    # Now import backend.main with the temp DB set up
    import importlib
    import backend.main as bm
    importlib.reload(bm)

    yield bm, TestClient(bm.app)

    # cleanup
    for fname in os.listdir(tmpdir):
        try:
            os.remove(os.path.join(tmpdir, fname))
        except OSError:
            pass
    try:
        os.rmdir(tmpdir)
    except OSError:
        pass


def test_returns_null_when_no_signals(spotlight_app):
    bm, client = spotlight_app
    resp = client.get("/api/spotlight/today")
    assert resp.status_code == 200
    assert resp.json() is None


def _insert_company(bm, ticker, company, market="US", domain=None):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO companies (ticker, company, domain, market)
            VALUES (:t, :c, :d, :m)
        """), {"t": ticker, "c": company, "d": domain or f"{ticker.lower()}.com", "m": market})


def _insert_signal(bm, *, date, ticker, signal="HOLD", close=100.0,
                   rsi_3day_flag=0, bb_condition=0, analyst_condition=0,
                   volume_flag=0, week52_condition=0, health_condition=0,
                   ml_condition=0, vqs=0, pred_5d=0.0, prediction_score=0.0):
    from sqlalchemy import text
    with bm.engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO signals (
                date, ticker, signal, close,
                rsi_3day_flag, bb_condition, analyst_condition,
                volume_flag, week52_condition, health_condition, ml_condition,
                vqs, pred_5d, prediction_score
            ) VALUES (
                :date, :ticker, :signal, :close,
                :rsi_3day_flag, :bb_condition, :analyst_condition,
                :volume_flag, :week52_condition, :health_condition, :ml_condition,
                :vqs, :pred_5d, :prediction_score
            )
        """), dict(
            date=date, ticker=ticker, signal=signal, close=close,
            rsi_3day_flag=rsi_3day_flag, bb_condition=bb_condition,
            analyst_condition=analyst_condition, volume_flag=volume_flag,
            week52_condition=week52_condition, health_condition=health_condition,
            ml_condition=ml_condition, vqs=vqs, pred_5d=pred_5d,
            prediction_score=prediction_score,
        ))


def test_picks_ticker_with_most_v1_gates(spotlight_app):
    bm, client = spotlight_app
    _insert_company(bm, "AAA", "Alpha Inc")
    _insert_company(bm, "BBB", "Beta Corp")
    _insert_company(bm, "CCC", "Gamma Co")
    # AAA: 4 gates met
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="AAA",
                   bb_condition=1, analyst_condition=1, volume_flag=1, ml_condition=1)
    # BBB: 6 gates met — the winner
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="BBB",
                   bb_condition=1, analyst_condition=1, volume_flag=1,
                   week52_condition=1, health_condition=1, ml_condition=1)
    # CCC: 3 gates met
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="CCC",
                   bb_condition=1, analyst_condition=1, volume_flag=1)

    resp = client.get("/api/spotlight/today")
    assert resp.status_code == 200
    body = resp.json()
    assert body is not None
    assert body["ticker"] == "BBB"
    assert body["gates_met"] == 6
    assert body["gates_total"] == 7
