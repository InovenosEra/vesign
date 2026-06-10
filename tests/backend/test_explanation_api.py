"""Endpoint tests for the BUY-signal explainer: gating + success + errors."""
import os
import tempfile
import importlib
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def api():
    saved = {k: os.environ.get(k) for k in
             ("DB_PATH", "BYPASS_AUTH", "DEV_PLAN", "DEV_WALLET_CENTS", "BYPASS_USER_ID")}
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "expl_api.db")
    os.environ["BYPASS_AUTH"] = "1"
    # Unset dev overrides so set_plan (DB) is authoritative for tier switching.
    os.environ.pop("DEV_PLAN", None)
    os.environ.pop("DEV_WALLET_CENTS", None)

    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}")
    with eng.begin() as conn:
        conn.execute(text("""CREATE TABLE signals (date TEXT, ticker TEXT, close REAL,
            prediction_score REAL, vqs INTEGER, signal TEXT, health_score INTEGER,
            target_mean_price REAL, lot_seq INTEGER, rsi REAL, fair_value_upside REAL,
            target_low_price REAL, target_high_price REAL)"""))
        conn.execute(text("""CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT,
            market TEXT, industry TEXT, domain TEXT, description TEXT,
            description_short TEXT, logo_url TEXT, sector TEXT)"""))
        conn.execute(text("""CREATE TABLE company_health_history (
            ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"""))
        conn.execute(text("""CREATE TABLE fundamentals (ticker TEXT, market_cap REAL,
            pe_ttm REAL, gross_margin REAL, op_margin REAL, net_margin REAL,
            roe REAL, de_ratio REAL)"""))
        conn.execute(text("""CREATE TABLE company_health (
            ticker TEXT PRIMARY KEY, score INTEGER, reason TEXT)"""))
        conn.execute(text("""CREATE TABLE daily_prices (
            ticker TEXT, date TEXT, close REAL, open REAL, high REAL, low REAL,
            volume REAL)"""))
        conn.execute(text("""CREATE TABLE analyst_expectations (
            ticker TEXT PRIMARY KEY, target_mean_price REAL, target_low_price REAL,
            target_high_price REAL)"""))
        conn.execute(text("""CREATE TABLE market_cap_history (
            ticker TEXT, date TEXT, shares_outstanding REAL)"""))
        conn.execute(text("""CREATE TABLE predictions (
            ticker TEXT, date TEXT, pred_5d REAL, pred_20d REAL, prediction_score REAL)"""))
        conn.execute(text("""INSERT INTO companies (ticker, company, market) VALUES
            ('AAPL','Apple Inc.','US')"""))
        conn.execute(text("""INSERT INTO signals (date, ticker, close, prediction_score,
            vqs, signal, health_score, target_mean_price) VALUES
            ('2026-05-26 00:00:00','AAPL',100.0,0.30,9,'BUY',4,120.0)"""))
        conn.execute(text("""INSERT INTO fundamentals (ticker, market_cap, pe_ttm,
            gross_margin, op_margin, net_margin, roe, de_ratio) VALUES
            ('AAPL',3e12,28.0,0.44,0.30,0.25,0.15,1.5)"""))
    eng.dispose()

    import data.loaders as loaders; importlib.reload(loaders)
    import data.explanations as expl; importlib.reload(expl)
    import backend.entitlements as ent; importlib.reload(ent)
    import backend.main as bm; importlib.reload(bm)
    os.environ.pop("BYPASS_USER_ID", None)
    yield bm, TestClient(bm.app)

    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    import shutil; shutil.rmtree(tmpdir, ignore_errors=True)


def _set_plan(bm, plan):
    # Synthetic bypass user id used by get_current_user when BYPASS_AUTH=1.
    bm.ent.set_plan("dev-bypass", plan)


_PAYLOAD = {"headline": "h", "strengths": ["a"], "risks": ["x"],
            "key_numbers": [{"label": "P/E", "value": "28.0"}]}


def test_free_user_gets_403(api):
    bm, client = api
    _set_plan(bm, "free")
    r = client.get("/api/signals/AAPL/explanation?date=2026-05-26")
    assert r.status_code == 403


def test_pro_user_gets_explanation(api, monkeypatch):
    bm, client = api
    _set_plan(bm, "pro")
    monkeypatch.setattr(bm.explanations, "get_or_create", lambda t, d: dict(_PAYLOAD))
    r = client.get("/api/signals/AAPL/explanation?date=2026-05-26")
    assert r.status_code == 200
    body = r.json()
    assert body["headline"] == "h"
    assert body["model"] == bm.explanations.MODEL


def test_bad_date_returns_400(api):
    bm, client = api
    _set_plan(bm, "max")
    r = client.get("/api/signals/AAPL/explanation?date=05-26-2026")
    assert r.status_code == 400


def test_unknown_signal_returns_404(api, monkeypatch):
    bm, client = api
    _set_plan(bm, "max")
    monkeypatch.setattr(bm.explanations, "get_or_create", lambda t, d: None)
    r = client.get("/api/signals/ZZZZ/explanation?date=2026-05-26")
    assert r.status_code == 404


def test_model_failure_returns_503(api, monkeypatch):
    bm, client = api
    _set_plan(bm, "max")
    def boom(t, d): raise RuntimeError("api down")
    monkeypatch.setattr(bm.explanations, "get_or_create", boom)
    r = client.get("/api/signals/AAPL/explanation?date=2026-05-26")
    assert r.status_code == 503
