"""Integration tests: redaction through the real endpoints + the new ones."""
import os
import tempfile
import importlib
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def api():
    saved = {k: os.environ.get(k) for k in ("DB_PATH", "BYPASS_AUTH", "DEV_PLAN", "DEV_WALLET_CENTS")}
    tmpdir = tempfile.mkdtemp()
    os.environ["DB_PATH"] = os.path.join(tmpdir, "ent_api.db")
    os.environ["BYPASS_AUTH"] = "1"
    os.environ.pop("DEV_PLAN", None)
    os.environ.pop("DEV_WALLET_CENTS", None)

    from sqlalchemy import create_engine, text
    eng = create_engine(f"sqlite:///{os.environ['DB_PATH']}")
    with eng.begin() as conn:
        conn.execute(text("""CREATE TABLE signals (date TEXT, ticker TEXT, close REAL,
            rsi REAL, target_mean_price REAL, target_low_price REAL, target_high_price REAL,
            prediction_score REAL, vqs INTEGER, signal TEXT, lot_seq INTEGER,
            health_score INTEGER, fair_value_upside REAL)"""))
        conn.execute(text("""CREATE TABLE companies (ticker TEXT PRIMARY KEY, company TEXT,
            market TEXT, industry TEXT, domain TEXT, description TEXT, description_short TEXT,
            logo_url TEXT, sector TEXT)"""))
        # Tables required by _ensure_indexes() at module load time
        conn.execute(text("""CREATE TABLE company_health_history (
            ticker TEXT, recorded_at TEXT, score INTEGER, reason TEXT)"""))
        # Tables required by _MARKET_CAP_JOIN (LEFT JOINs in _build_signals_today)
        conn.execute(text("""CREATE TABLE fundamentals (
            ticker TEXT, market_cap REAL, pe_ttm REAL)"""))
        conn.execute(text("""CREATE TABLE company_health (
            ticker TEXT PRIMARY KEY, score INTEGER, reason TEXT)"""))
        conn.execute(text("""CREATE TABLE daily_prices (
            ticker TEXT, date TEXT, close REAL, open REAL, high REAL, low REAL,
            volume REAL)"""))
        conn.execute(text("""CREATE TABLE analyst_expectations (
            ticker TEXT PRIMARY KEY, target_mean_price REAL, target_low_price REAL,
            target_high_price REAL)"""))
        for i in range(3):
            conn.execute(text("INSERT INTO companies (ticker, company, market) VALUES (:t,:t,'US')"),
                         {"t": f"T{i}"})
            conn.execute(text("""INSERT INTO signals (date, ticker, close, vqs, prediction_score,
                signal, health_score) VALUES ('2026-05-26 00:00:00', :t, 100, 9, 0.3, 'BUY', 4)"""),
                         {"t": f"T{i}"})
    eng.dispose()

    import data.loaders as loaders; importlib.reload(loaders)
    import backend.entitlements as ent; importlib.reload(ent)
    import backend.main as bm; importlib.reload(bm)
    yield bm, TestClient(bm.app)
    import shutil; shutil.rmtree(tmpdir, ignore_errors=True)
    for k, v in saved.items():
        if v is None: os.environ.pop(k, None)
        else: os.environ[k] = v


def test_free_buy_signals_are_redacted(api):
    bm, client = api
    os.environ["DEV_PLAN"] = "free"
    rows = client.get("/api/signals/today?signal=BUY&market=US").json()
    assert rows and all(r.get("locked") for r in rows)
    assert all("ticker" not in r for r in rows)        # SECURITY INVARIANT over HTTP
    del os.environ["DEV_PLAN"]


def test_max_buy_signals_are_full(api):
    bm, client = api
    os.environ["DEV_PLAN"] = "max"
    rows = client.get("/api/signals/today?signal=BUY&market=US").json()
    assert rows and all(r.get("ticker") for r in rows)
    del os.environ["DEV_PLAN"]


def test_me_reports_plan_balance_and_prices(api):
    bm, client = api
    os.environ["DEV_PLAN"] = "pro"
    os.environ["DEV_WALLET_CENTS"] = "250"
    d = client.get("/api/me").json()
    assert d["plan"] == "pro"
    assert d["balance_cents"] == 250
    assert d["per_row_price_cents"] == 10
    assert d["see_all_price_cents"] == 50
    del os.environ["DEV_PLAN"]; del os.environ["DEV_WALLET_CENTS"]
