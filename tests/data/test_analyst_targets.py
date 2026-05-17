"""Tests for data.analyst_targets — yfinance + FMP fallback orchestrator."""
import os
from unittest.mock import patch
from data import analyst_targets


def test_yfinance_success_no_fallback_call():
    """When yfinance returns data, FMP is NOT called for that ticker."""
    yfinance_out = {"AAPL": {"target_mean_price": 250.0, "target_high_price": 290.0,
                              "target_low_price": 210.0, "number_of_analysts": 35}}
    fmp_called = []

    def fake_fmp(t):
        fmp_called.append(t)
        return {"targetConsensus": 999.0, "targetHigh": 1000.0,
                "targetLow": 1.0, "numberOfAnalysts": 1}

    with patch("data.analyst_targets.yfinance_analyst.get_targets_batch",
               return_value=yfinance_out), \
         patch("data.analyst_targets.fmp.price_target_consensus",
               side_effect=fake_fmp):
        out = analyst_targets.fetch_with_fallback(["AAPL"])

    assert out["AAPL"]["source"] == "yfinance"
    assert out["AAPL"]["target_mean_price"] == 250.0
    assert fmp_called == []  # FMP not called


def test_yfinance_empty_falls_back_to_fmp():
    """When yfinance returns None for a ticker, FMP fills in."""
    yfinance_out = {"NGG": None}
    fmp_data = {"targetConsensus": 92.4, "targetHigh": 105.2,
                "targetLow": 78.0, "numberOfAnalysts": 8}

    with patch("data.analyst_targets.yfinance_analyst.get_targets_batch",
               return_value=yfinance_out), \
         patch("data.analyst_targets.fmp.price_target_consensus",
               return_value=fmp_data):
        out = analyst_targets.fetch_with_fallback(["NGG"])

    assert out["NGG"]["source"] == "fmp"
    assert out["NGG"]["target_mean_price"] == 92.4
    assert out["NGG"]["target_high_price"] == 105.2
    assert out["NGG"]["target_low_price"]  == 78.0


def test_both_empty_returns_source_none():
    """When yfinance AND FMP both return empty, source='none' with NULL values."""
    with patch("data.analyst_targets.yfinance_analyst.get_targets_batch",
               return_value={"WEIRD": None}), \
         patch("data.analyst_targets.fmp.price_target_consensus",
               return_value=None):
        out = analyst_targets.fetch_with_fallback(["WEIRD"])

    assert out["WEIRD"]["source"] == "none"
    assert out["WEIRD"]["target_mean_price"] is None
    assert out["WEIRD"]["number_of_analysts"] is None


def test_env_flag_yfinance_calls_orchestrator(monkeypatch):
    """When ANALYST_SOURCE=yfinance, the analyst routing path calls our
    orchestrator (not raw FMP)."""
    monkeypatch.setenv("ANALYST_SOURCE", "yfinance")
    called = {"orchestrator": 0, "raw_fmp": 0}

    def fake_orch(tickers):
        called["orchestrator"] += 1
        return {t: {"target_mean_price": 100, "target_high_price": 110,
                    "target_low_price": 90, "number_of_analysts": 5,
                    "source": "yfinance"} for t in tickers}

    def fake_fmp_consensus(t):
        called["raw_fmp"] += 1
        return {"targetConsensus": 50}

    with patch("data.analyst_targets.fetch_with_fallback", side_effect=fake_orch), \
         patch("data.analyst_targets.fmp.price_target_consensus", side_effect=fake_fmp_consensus):
        rows = analyst_targets.fetch_analyst_targets_routed(["AAPL", "MSFT"])

    assert called["orchestrator"] == 1
    assert called["raw_fmp"] == 0
    assert rows["AAPL"]["source"] == "yfinance"


def test_env_flag_fmp_uses_legacy_path(monkeypatch):
    """When ANALYST_SOURCE=fmp (or unset), the raw-FMP path is used."""
    monkeypatch.setenv("ANALYST_SOURCE", "fmp")
    called = {"orchestrator": 0, "raw_fmp": 0}

    def fake_orch(tickers):
        called["orchestrator"] += 1
        return {}

    def fake_fmp_consensus(t):
        called["raw_fmp"] += 1
        return {"targetConsensus": 100, "targetHigh": 110, "targetLow": 90,
                "numberOfAnalysts": 3}

    with patch("data.analyst_targets.fetch_with_fallback", side_effect=fake_orch), \
         patch("data.analyst_targets.fmp.price_target_consensus", side_effect=fake_fmp_consensus):
        rows = analyst_targets.fetch_analyst_targets_routed(["AAPL", "MSFT"])

    assert called["orchestrator"] == 0
    assert called["raw_fmp"] == 2
    assert rows["AAPL"]["source"] == "fmp"


def test_update_company_info_writes_source_column(monkeypatch):
    """When ANALYST_SOURCE=yfinance, the source column is populated on write."""
    monkeypatch.setenv("ANALYST_SOURCE", "yfinance")
    from sqlalchemy import create_engine, text
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        conn.execute(text("""CREATE TABLE companies (ticker TEXT, industry TEXT, description TEXT)"""))
        conn.execute(text("""CREATE TABLE fundamentals (ticker TEXT, market_cap REAL)"""))
        conn.execute(text("""CREATE TABLE pipeline_control (
            step_name TEXT PRIMARY KEY, last_run TEXT
        )"""))
        conn.execute(text("INSERT INTO companies (ticker) VALUES ('AAPL')"))

    import data.market_data as md, data.loaders as loaders, data.analyst_targets as at
    monkeypatch.setattr(loaders, "engine", eng)
    monkeypatch.setattr(md, "engine", eng)
    loaders._ensure_analyst_source_column()

    def fake_routed(tickers):
        return {t: {"target_mean_price": 250.0, "target_high_price": 290.0,
                    "target_low_price": 210.0, "number_of_analysts": 5,
                    "source": "yfinance"} for t in tickers}

    monkeypatch.setattr(at, "fetch_analyst_targets_routed", fake_routed)
    monkeypatch.setattr(md, "should_run", lambda *a, **k: True)
    monkeypatch.setattr(md.fmp, "company_profile", lambda t: {"marketCap": 1e12})

    md.update_company_info()

    src = eng.connect().execute(text("SELECT source FROM analyst_expectations WHERE ticker='AAPL'")).fetchone()
    assert src is not None and src[0] == "yfinance"


def test_snapshot_carries_source_to_history(monkeypatch):
    """snapshot_analyst_targets() persists the source column to analyst_targets_history."""
    from sqlalchemy import create_engine, text
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE analyst_expectations (
                ticker TEXT PRIMARY KEY, target_mean_price REAL,
                target_high_price REAL, target_low_price REAL,
                number_of_analysts REAL, last_update TEXT, source TEXT
            )
        """))
        conn.execute(text("""
            INSERT INTO analyst_expectations VALUES
              ('AAPL', 250, 290, 210, 35, '2026-05-18', 'yfinance'),
              ('NGG',  92,  105, 78, 8,  '2026-05-18', 'yfinance')
        """))

    import data.market_data as md, data.loaders as loaders
    monkeypatch.setattr(loaders, "engine", eng)
    monkeypatch.setattr(md, "engine", eng)

    md.snapshot_analyst_targets("2026-05-18")

    rows = eng.connect().execute(text(
        "SELECT ticker, source FROM analyst_targets_history WHERE date='2026-05-18'"
    )).fetchall()
    assert {(r[0], r[1]) for r in rows} == {("AAPL", "yfinance"), ("NGG", "yfinance")}


def test_fmp_fallback_pulls_analyst_count_from_summary():
    """In FMP fallback path, _fmp_analyst_count fills the analyst count
    from price_target_summary (consensus doesn't have it)."""
    fmp_consensus = {"targetConsensus": 100.0, "targetHigh": 110.0, "targetLow": 90.0}
    fmp_summary = {"lastQuarterCount": 5, "lastMonthCount": 3, "lastYearCount": 12}

    with patch("data.analyst_targets.yfinance_analyst.get_targets_batch",
               return_value={"X": None}), \
         patch("data.analyst_targets.fmp.price_target_consensus",
               return_value=fmp_consensus), \
         patch("data.analyst_targets.fmp.price_target_summary",
               return_value=fmp_summary):
        out = analyst_targets.fetch_with_fallback(["X"])

    assert out["X"]["source"] == "fmp"
    assert out["X"]["target_mean_price"] == 100.0
    assert out["X"]["number_of_analysts"] == 5  # lastQuarterCount preferred
