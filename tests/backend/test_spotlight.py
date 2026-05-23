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


def test_excludes_buy_and_sell_tickers(spotlight_app):
    bm, client = spotlight_app
    _insert_company(bm, "ZZZ", "Best Score But A BUY")
    _insert_company(bm, "YYY", "Second Best But A SELL")
    _insert_company(bm, "XXX", "Third Best HOLD — should win")
    # ZZZ: 7 gates met but signal=BUY (canonical signal handles it)
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="ZZZ", signal="BUY",
                   rsi_3day_flag=3, bb_condition=1, analyst_condition=1,
                   volume_flag=1, week52_condition=1, health_condition=1, ml_condition=1)
    # YYY: 6 gates met but signal=SELL
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="YYY", signal="SELL",
                   bb_condition=1, analyst_condition=1, volume_flag=1,
                   week52_condition=1, health_condition=1, ml_condition=1)
    # XXX: 5 gates met, signal=HOLD
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="XXX", signal="HOLD",
                   bb_condition=1, analyst_condition=1, volume_flag=1,
                   week52_condition=1, health_condition=1)

    resp = client.get("/api/spotlight/today")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ticker"] == "XXX"
    assert body["gates_met"] == 5


def test_tiebreak_vqs_then_pred_then_ticker(spotlight_app):
    bm, client = spotlight_app
    # Three tickers at 5/7 gates each; differentiate via vqs/pred/ticker.
    _insert_company(bm, "AAA", "Alpha")
    _insert_company(bm, "BBB", "Beta")
    _insert_company(bm, "CCC", "Gamma")
    # All have the same 5 gates; only vqs/pred/ticker differ.
    common = dict(bb_condition=1, analyst_condition=1, volume_flag=1,
                  week52_condition=1, health_condition=1)
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="AAA",
                   vqs=3, pred_5d=0.01, prediction_score=0.5, **common)
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="BBB",
                   vqs=5, pred_5d=0.01, prediction_score=0.2, **common)  # vqs winner
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="CCC",
                   vqs=3, pred_5d=0.02, prediction_score=0.9, **common)

    resp = client.get("/api/spotlight/today")
    assert resp.json()["ticker"] == "BBB"


def test_tiebreak_falls_through_to_ticker_when_all_equal(spotlight_app):
    bm, client = spotlight_app
    _insert_company(bm, "BBB", "Beta")
    _insert_company(bm, "AAA", "Alpha")  # inserted second to confirm order is alphabetical, not insertion
    common = dict(bb_condition=1, analyst_condition=1, volume_flag=1,
                  week52_condition=1, health_condition=1,
                  vqs=2, pred_5d=0.01, prediction_score=0.5)
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="BBB", **common)
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="AAA", **common)

    resp = client.get("/api/spotlight/today")
    assert resp.json()["ticker"] == "AAA"  # alphabetical fallback


def test_response_includes_seven_reasons_with_correct_met_flags(spotlight_app):
    bm, client = spotlight_app
    _insert_company(bm, "INTU", "Intuit", domain="intuit.com")
    # 6 of 7 gates met; rsi_3day_flag=1 (needs 3) is the only "not met"
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="INTU",
                   rsi_3day_flag=1,
                   bb_condition=1, analyst_condition=1, volume_flag=1,
                   week52_condition=1, health_condition=1, ml_condition=1,
                   close=612.34)

    body = client.get("/api/spotlight/today").json()
    assert body["ticker"] == "INTU"
    reasons = body["reasons"]
    assert len(reasons) == 7
    by_gate = {r["gate"]: r for r in reasons}
    assert by_gate["rsi_3day_flag"]["met"] is False
    assert by_gate["rsi_3day_flag"]["value"] == 1
    assert by_gate["rsi_3day_flag"]["needed"] == 3
    assert by_gate["rsi_3day_flag"]["label"] == "RSI<30 for 3 consecutive days"
    for g in ("bb_condition", "analyst_condition", "volume_flag",
              "week52_condition", "health_condition", "ml_condition"):
        assert by_gate[g]["met"] is True, f"{g} should be met"


def test_day_change_pct_from_prior_close(spotlight_app):
    bm, client = spotlight_app
    _insert_company(bm, "ABC", "Acme")
    # Prior day row, close=100
    _insert_signal(bm, date="2026-05-21 00:00:00", ticker="ABC",
                   close=100.0, bb_condition=1, analyst_condition=1, volume_flag=1)
    # Today row, close=102 → +2.00%
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="ABC",
                   close=102.0, bb_condition=1, analyst_condition=1, volume_flag=1)

    body = client.get("/api/spotlight/today").json()
    assert body["ticker"] == "ABC"
    assert body["close"] == 102.0
    assert body["day_change_pct"] == pytest.approx(2.0, rel=1e-6)


def test_day_change_pct_is_null_with_no_prior_row(spotlight_app):
    bm, client = spotlight_app
    _insert_company(bm, "NEW", "Newly Listed")
    _insert_signal(bm, date="2026-05-22 00:00:00", ticker="NEW",
                   close=50.0, bb_condition=1, analyst_condition=1, volume_flag=1)

    body = client.get("/api/spotlight/today").json()
    assert body["ticker"] == "NEW"
    assert body["day_change_pct"] is None
