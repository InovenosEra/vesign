"""Tests for backend.main._get_live_snapshot — TTL, single-flight, phase, idle."""
import os
import tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def snap_app():
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "test_snap.db")
    os.environ["BYPASS_AUTH"] = "1"
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}", poolclass=None)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
        conn.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT, industry TEXT, description TEXT, description_short TEXT, logo_url TEXT, domain TEXT)"))
        conn.execute(text("CREATE TABLE fundamentals (ticker TEXT, market_cap REAL)"))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
        conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES ('AAA','AAA Corp','US')"))
        conn.execute(text("INSERT INTO daily_prices (date,ticker,open,high,low,close,volume) VALUES ('2026-05-22 00:00:00','AAA',100,100,100,100,5)"))
    eng.dispose()
    import importlib, backend.main as bm
    importlib.reload(bm)
    yield bm, TestClient(bm.app)
    for f in os.listdir(tmpdir):
        try: os.remove(os.path.join(tmpdir, f))
        except OSError: pass
    os.rmdir(tmpdir)


def test_regular_phase_fetches_once_within_ttl(snap_app):
    bm, _ = snap_app
    calls = []
    def fake_live(tickers): calls.append(tickers); return {"AAA": 110.0}
    with patch.object(bm, "_phase_info", return_value={"phase": "regular"}), \
         patch.object(bm, "fetch_live_prices", side_effect=fake_live):
        a = bm._get_live_snapshot()
        b = bm._get_live_snapshot()
    assert a["prices"]["AAA"] == 110.0
    assert b["prices"]["AAA"] == 110.0
    assert len(calls) == 1


def test_pre_phase_uses_aftermarket_mid(snap_app):
    bm, _ = snap_app
    quotes = {"AAA": {"bidPrice": 110.0, "askPrice": 112.0}}
    with patch.object(bm, "_phase_info", return_value={"phase": "pre"}), \
         patch.object(bm, "fetch_aftermarket_quotes", return_value=quotes):
        out = bm._get_live_snapshot()
    assert out["phase"] == "pre"
    assert out["prices"]["AAA"] == 111.0


def test_idle_phase_no_fetch_empty_prices(snap_app):
    bm, _ = snap_app
    with patch.object(bm, "_phase_info", return_value={"phase": "idle"}), \
         patch.object(bm, "fetch_live_prices") as f:
        out = bm._get_live_snapshot()
    assert out == {"phase": "idle", "prices": {}}
    f.assert_not_called()


def test_phase_change_flushes(snap_app):
    bm, _ = snap_app
    with patch.object(bm, "_phase_info", return_value={"phase": "regular"}), \
         patch.object(bm, "fetch_live_prices", return_value={"AAA": 110.0}):
        bm._get_live_snapshot()
    with patch.object(bm, "_phase_info", return_value={"phase": "pre"}), \
         patch.object(bm, "fetch_aftermarket_quotes", return_value={"AAA": {"bidPrice": 200.0, "askPrice": 200.0}}):
        out = bm._get_live_snapshot()
    assert out["prices"]["AAA"] == 200.0


def test_fetch_failure_keeps_previous_snapshot(snap_app):
    bm, _ = snap_app
    with patch.object(bm, "_phase_info", return_value={"phase": "regular"}), \
         patch.object(bm, "fetch_live_prices", return_value={"AAA": 110.0}):
        bm._get_live_snapshot()
    bm._snapshot_ts = 0.0
    with patch.object(bm, "_phase_info", return_value={"phase": "regular"}), \
         patch.object(bm, "fetch_live_prices", side_effect=RuntimeError("FMP down")):
        out = bm._get_live_snapshot()
    assert out["prices"]["AAA"] == 110.0


def test_all_falsy_prices_still_honors_ttl(snap_app):
    bm, _ = snap_app
    calls = []
    def fake_live(tickers):
        calls.append(tickers)
        return {"AAA": None}            # filtered out -> empty cache, but ts set
    with patch.object(bm, "_phase_info", return_value={"phase": "regular"}), \
         patch.object(bm, "fetch_live_prices", side_effect=fake_live):
        bm._get_live_snapshot()
        bm._get_live_snapshot()         # within TTL -> must NOT refetch
        result = bm._get_live_snapshot()
        assert result["prices"] == {}
        assert len(calls) == 1          # TTL honored despite empty cache
