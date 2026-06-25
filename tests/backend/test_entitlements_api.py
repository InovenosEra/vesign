"""Integration tests: redaction through the real endpoints + the new ones."""
import os
import tempfile
import importlib
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def api():
    saved = {k: os.environ.get(k) for k in ("DB_PATH", "BYPASS_AUTH", "DEV_PLAN", "DEV_WALLET_CENTS", "BYPASS_USER_ID")}
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
            prediction_score REAL, vqs INTEGER, tier INTEGER, signal TEXT, lot_seq INTEGER,
            health_score INTEGER, fair_value_upside REAL,
            rsi_3day_flag INTEGER, bb_condition INTEGER, analyst_condition INTEGER,
            volume_flag INTEGER, week52_condition INTEGER, health_condition INTEGER,
            ml_condition INTEGER, open REAL, high REAL, low REAL, volume REAL,
            bb_high REAL, bb_low REAL, macd REAL, rsi_factor REAL, bb_factor REAL,
            macd_factor REAL, trend_factor REAL, volume_sma_20 REAL, volume_ratio REAL,
            week52_high REAL, pct_from_52w_high REAL, number_of_analysts INTEGER,
            bb_pct_b REAL, score REAL, vesign_score REAL, news_block_reason TEXT)"""))
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
        # Required by /api/signals subquery: (s.close * shares_outstanding) AS market_cap
        conn.execute(text("""CREATE TABLE market_cap_history (
            ticker TEXT, date TEXT, shares_outstanding REAL)"""))
        # Required by _build_open_trades: ML prediction fields at buy date
        conn.execute(text("""CREATE TABLE predictions (
            ticker TEXT, date TEXT, pred_5d REAL, pred_20d REAL, prediction_score REAL)"""))
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
    # load_dotenv() inside bm may re-set BYPASS_USER_ID from .env — remove it
    # so get_current_user falls back to the default "dev-bypass" synthetic id.
    os.environ.pop("BYPASS_USER_ID", None)
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
    assert d["per_row_price_cents"] == {"buy": 20, "sell": 10}
    assert d["see_all_price_cents"] == 50      # static legacy field; live see-all is dynamic
    del os.environ["DEV_PLAN"]; del os.environ["DEV_WALLET_CENTS"]


def _seed_pro(bm, cents):
    import backend.entitlements as e
    e.set_plan("dev-bypass", "pro")
    e.credit("dev-bypass", cents, reason="test-seed")


def test_set_plan_switches_plan(api):
    bm, client = api
    assert client.get("/api/me").json()["plan"] == "free"
    r = client.post("/api/me/plan", json={"plan": "pro"})
    assert r.status_code == 200
    assert r.json()["plan"] == "pro"
    assert client.get("/api/me").json()["plan"] == "pro"
    assert client.post("/api/me/plan", json={"plan": "max"}).json()["plan"] == "max"
    assert client.get("/api/me").json()["plan"] == "max"
    # cancel == back to free
    assert client.post("/api/me/plan", json={"plan": "free"}).json()["plan"] == "free"
    assert client.get("/api/me").json()["plan"] == "free"


def test_set_plan_rejects_invalid(api):
    bm, client = api
    r = client.post("/api/me/plan", json={"plan": "ultra"})
    assert r.status_code == 400
    assert client.get("/api/me").json()["plan"] == "free"      # unchanged


def test_unlock_buy_tier_deducts_and_reveals(api):
    bm, client = api
    _seed_pro(bm, 100)
    import backend.entitlements as e
    # 3 seeded BUYs are untiered → Promising (tier 3); unlock that tier.
    resp = client.post("/api/signals/unlock",
                       json={"kind": "buy", "scope": "tier", "tier": 3, "market": "US"})
    assert resp.status_code == 200
    assert resp.json()["balance_cents"] == 100 - e.tier_unlock_price_cents(3, 3)  # 100-30
    rows = client.get("/api/signals/today?signal=BUY&market=US").json()
    assert all(not r.get("locked") for r in rows)


def test_unlock_tier_idempotent_no_double_charge(api):
    bm, client = api
    _seed_pro(bm, 100)
    body = {"kind": "buy", "scope": "tier", "tier": 3, "market": "US"}
    client.post("/api/signals/unlock", json=body)
    second = client.post("/api/signals/unlock", json=body)
    assert second.json()["balance_cents"] == 70      # unchanged on re-purchase


def test_unlock_insufficient_funds(api):
    bm, client = api
    _seed_pro(bm, 5)
    resp = client.post("/api/signals/unlock",
                       json={"kind": "buy", "scope": "tier", "tier": 3, "market": "US"})
    assert resp.status_code == 402


def test_unlock_rejected_for_free_plan(api):
    bm, client = api
    import backend.entitlements as e
    e.set_plan("dev-bypass", "free")
    resp = client.post("/api/signals/unlock",
                       json={"kind": "buy", "scope": "tier", "tier": 3, "market": "US"})
    assert resp.status_code == 402


def test_unlock_buy_all_charges_value_weighted_price(api):
    bm, client = api
    _seed_pro(bm, 100)
    import backend.entitlements as e
    resp = client.post("/api/signals/unlock",
                       json={"kind": "buy", "scope": "all", "market": "US"})
    assert resp.status_code == 200
    assert resp.json()["balance_cents"] == 100 - e.all_tiers_price_cents({1: 0, 2: 0, 3: 3})  # 100-30
    rows = client.get("/api/signals/today?signal=BUY&market=US").json()
    assert all(not r.get("locked") for r in rows)


def test_unlock_error_codes_are_distinct(api):
    bm, client = api
    import backend.entitlements as e
    e.set_plan("dev-bypass", "free")
    r1 = client.post("/api/signals/unlock", json={"kind": "buy", "scope": "tier", "tier": 3, "market": "US"})
    assert r1.status_code == 402 and r1.json()["detail"]["code"] == "UPGRADE_REQUIRED"
    e.set_plan("dev-bypass", "pro")
    r2 = client.post("/api/signals/unlock", json={"kind": "buy", "scope": "tier", "tier": 3, "market": "US"})
    assert r2.status_code == 402 and r2.json()["detail"]["code"] == "INSUFFICIENT_FUNDS"


def test_unlock_tier_requires_buy_and_valid_tier(api):
    bm, client = api
    _seed_pro(bm, 100)
    assert client.post("/api/signals/unlock", json={"kind": "sell", "scope": "tier", "tier": 3, "market": "US"}).status_code == 400
    assert client.post("/api/signals/unlock", json={"kind": "buy", "scope": "tier", "tier": 9, "market": "US"}).status_code == 400
    assert client.post("/api/signals/unlock", json={"kind": "buy", "scope": "tier", "market": "US"}).status_code == 400


def test_unlock_bad_kind_and_scope_rejected(api):
    bm, client = api
    _seed_pro(bm, 100)
    assert client.post("/api/signals/unlock", json={"kind": "bogus", "scope": "all", "market": "US"}).status_code == 400
    assert client.post("/api/signals/unlock", json={"kind": "buy", "scope": "bogus", "market": "US"}).status_code == 400


def test_unlock_all_idempotent(api):
    bm, client = api
    _seed_pro(bm, 100)
    import backend.entitlements as e
    client.post("/api/signals/unlock", json={"kind": "buy", "scope": "all", "market": "US"})
    second = client.post("/api/signals/unlock", json={"kind": "buy", "scope": "all", "market": "US"})
    assert second.json()["balance_cents"] == 100 - e.all_tiers_price_cents({1: 0, 2: 0, 3: 3})


def test_api_signals_list_redacts_today_buys_for_free(api):
    bm, client = api
    os.environ["DEV_PLAN"] = "free"
    resp = client.get("/api/signals?signal=BUY&market=US&months=120").json()
    rows = resp["data"]
    assert rows and all(r.get("locked") for r in rows)
    assert all("ticker" not in r for r in rows)     # enumeration closed
    del os.environ["DEV_PLAN"]


def test_api_signals_list_full_for_max(api):
    bm, client = api
    os.environ["DEV_PLAN"] = "max"
    rows = client.get("/api/signals?signal=BUY&market=US&months=120").json()["data"]
    assert rows and all(r.get("ticker") for r in rows)
    del os.environ["DEV_PLAN"]


def test_signals_export_excludes_today_buys_for_nonmax(api):
    bm, client = api
    os.environ["DEV_PLAN"] = "free"
    free_csv = client.get("/api/signals/export?market=US&format=csv").content
    assert b"T0" not in free_csv and b"T1" not in free_csv  # today's BUYs excluded
    os.environ["DEV_PLAN"] = "max"
    max_csv = client.get("/api/signals/export?market=US&format=csv").content
    assert b"T0" in max_csv                                  # max gets everything
    del os.environ["DEV_PLAN"]


def test_tiers_endpoint_shape_counts_and_rates(api):
    bm, client = api
    os.environ["DEV_PLAN"] = "pro"
    d = client.get("/api/signals/today/tiers?market=US").json()
    assert d["total"] == 3                      # 3 BUYs seeded (untiered → Promising)
    assert d["all_discount_pct"] == 15
    assert d["tiers"]["prime"] == {"tier": 1, "count": 0, "rate_cents": 30}
    assert d["tiers"]["strong"] == {"tier": 2, "count": 0, "rate_cents": 20}
    assert d["tiers"]["promising"] == {"tier": 3, "count": 3, "rate_cents": 10}
    del os.environ["DEV_PLAN"]


def test_open_trades_export_is_max_only(api):
    bm, client = api
    os.environ["DEV_PLAN"] = "free"
    assert client.get("/api/trades/open/export?market=US&format=csv").status_code == 402
    os.environ["DEV_PLAN"] = "pro"
    assert client.get("/api/trades/open/export?market=US&format=csv").status_code == 402
    os.environ["DEV_PLAN"] = "max"
    assert client.get("/api/trades/open/export?market=US&format=csv").status_code != 402
    del os.environ["DEV_PLAN"]
