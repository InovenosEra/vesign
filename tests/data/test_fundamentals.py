"""Tests for data.fundamentals — TTM fundamentals extraction + storage."""
import sqlite3

import pytest
from sqlalchemy import create_engine, text

from data import fundamentals

FIELDS = {"pe_ttm", "eps_ttm", "revenue_ttm", "revenue_growth",
          "gross_margin", "op_margin", "net_margin", "roe", "de_ratio"}


def test_extract_maps_all_fields():
    """Each of the 9 fields maps from the correct FMP key (real AAPL keys)."""
    ratios = {
        "priceToEarningsRatioTTM": 37.06,
        "netIncomePerShareTTM": 8.33,
        "grossProfitMarginTTM": 0.4786,
        "operatingProfitMarginTTM": 0.3264,
        "netProfitMarginTTM": 0.2715,
        "debtToEquityRatioTTM": 0.7955,
    }
    km = {"returnOnEquityTTM": 1.4669}
    growth = {"revenueGrowth": 0.0502}
    stmts = [{"revenue": 391035000000, "fiscalYear": "2024"}]

    f = fundamentals.extract_fundamentals_ttm(ratios, km, growth, stmts)
    assert f == {
        "pe_ttm": 37.06,
        "eps_ttm": 8.33,
        "revenue_ttm": 391035000000,
        "revenue_growth": 0.0502,
        "gross_margin": 0.4786,
        "op_margin": 0.3264,
        "net_margin": 0.2715,
        "roe": 1.4669,
        "de_ratio": 0.7955,
    }


def test_extract_handles_empty_payloads():
    f = fundamentals.extract_fundamentals_ttm({}, {}, {}, [])
    assert set(f) == FIELDS
    assert all(v is None for v in f.values())


def test_extract_handles_none_payloads():
    """Defensive: FMP helpers can return None on rate-limit/error."""
    f = fundamentals.extract_fundamentals_ttm(None, None, None, None)
    assert set(f) == FIELDS
    assert all(v is None for v in f.values())


def test_extract_revenue_uses_most_recent_statement():
    stmts = [{"revenue": 100, "date": "2025-09-30"}, {"revenue": 90, "date": "2024-09-30"}]
    f = fundamentals.extract_fundamentals_ttm({}, {}, {}, stmts)
    assert f["revenue_ttm"] == 100


def _fresh_db(tmp_path):
    db = tmp_path / "t.db"
    raw = sqlite3.connect(db)
    raw.execute("CREATE TABLE fundamentals (ticker TEXT, market_cap FLOAT)")
    for c in ("pe_ttm", "eps_ttm", "revenue_ttm", "revenue_growth", "gross_margin",
              "op_margin", "net_margin", "roe", "de_ratio"):
        raw.execute(f"ALTER TABLE fundamentals ADD COLUMN {c} FLOAT")
    raw.execute("ALTER TABLE fundamentals ADD COLUMN fundamentals_updated TEXT")
    raw.execute("INSERT INTO fundamentals (ticker, market_cap) VALUES ('AAPL', 3.4e12)")
    raw.commit()
    raw.close()
    return create_engine(f"sqlite:///{db}")


def test_store_updates_existing_row_preserving_market_cap(tmp_path):
    """UPSERT must UPDATE the existing row (no UNIQUE constraint on ticker)
    and must NOT clobber market_cap."""
    eng = _fresh_db(tmp_path)
    fund = {k: 1.0 for k in FIELDS}
    fundamentals.store_fundamentals("AAPL", fund, eng, updated="2026-05-24")

    with eng.connect() as c:
        rows = c.execute(text("SELECT ticker, market_cap, pe_ttm, fundamentals_updated "
                              "FROM fundamentals WHERE ticker='AAPL'")).fetchall()
    assert len(rows) == 1, "must not create a duplicate row"
    assert rows[0][1] == 3.4e12, "market_cap preserved"
    assert rows[0][2] == 1.0
    assert rows[0][3] == "2026-05-24"


def test_store_inserts_when_ticker_absent(tmp_path):
    eng = _fresh_db(tmp_path)
    fund = {k: 2.0 for k in FIELDS}
    fundamentals.store_fundamentals("NVDA", fund, eng, updated="2026-05-24")

    with eng.connect() as c:
        row = c.execute(text("SELECT ticker, market_cap, net_margin FROM fundamentals "
                             "WHERE ticker='NVDA'")).fetchone()
    assert row is not None
    assert row[0] == "NVDA"
    assert row[1] is None       # no market_cap known yet
    assert row[2] == 2.0
