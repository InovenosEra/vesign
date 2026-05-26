"""Tests for backend.main._overlay_live — overlays live snapshot price on rows."""
import os
import tempfile
from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app():
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "test_overlay.db")
    os.environ["BYPASS_AUTH"] = "1"
    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}", poolclass=None)
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE daily_prices (date DATETIME, ticker TEXT, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)"))
        conn.execute(text("CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT, sector TEXT, market TEXT, industry TEXT, description TEXT, description_short TEXT, logo_url TEXT, domain TEXT)"))
        conn.execute(text("CREATE TABLE fundamentals (ticker TEXT, market_cap REAL)"))
        conn.execute(text("CREATE TABLE company_health_history (ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"))
    eng.dispose()
    import importlib, backend.main as bm
    importlib.reload(bm)
    yield bm
    for f in os.listdir(tmpdir):
        try: os.remove(os.path.join(tmpdir, f))
        except OSError: pass
    os.rmdir(tmpdir)


def test_overlay_replaces_close_when_live_present(app):
    bm = app
    rows = [{"ticker": "AAA", "close": 100.0}, {"ticker": "BBB", "close": 50.0}]
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": {"AAA": 123.0}}):
        out = bm._overlay_live(rows)
    assert out[0]["close"] == 123.0      # AAA overlaid
    assert out[1]["close"] == 50.0       # BBB unchanged (absent from snapshot)


def test_overlay_does_not_mutate_input_rows(app):
    bm = app
    rows = [{"ticker": "AAA", "close": 100.0}]
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": {"AAA": 123.0}}):
        out = bm._overlay_live(rows)
    assert rows[0]["close"] == 100.0     # original (cached) row NOT mutated
    assert out[0]["close"] == 123.0
    assert out[0] is not rows[0]         # changed row is a copy


def test_overlay_ignores_falsy_live_price(app):
    bm = app
    rows = [{"ticker": "AAA", "close": 100.0}]
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "regular", "prices": {"AAA": 0}}):
        out = bm._overlay_live(rows)
    assert out[0]["close"] == 100.0      # falsy live -> keep close


def test_overlay_empty_snapshot_returns_equivalent_rows(app):
    bm = app
    rows = [{"ticker": "AAA", "close": 100.0}]
    with patch.object(bm, "_get_live_snapshot", return_value={"phase": "idle", "prices": {}}):
        out = bm._overlay_live(rows)
    assert out[0]["close"] == 100.0
