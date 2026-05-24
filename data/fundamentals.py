"""TTM fundamentals — extraction from FMP payloads + storage.

The `fundamentals` table historically held only (ticker, market_cap). This
module adds the TTM valuation/profitability fields the UI needs (P/E, EPS,
revenue, revenue growth, gross/op/net margin, ROE, D/E).

Design notes:
- `extract_fundamentals_ttm` is a pure mapping (no I/O) so it unit-tests
  trivially and can be reused by the daily pipeline and the one-time backfill.
- The `fundamentals` table has NO UNIQUE constraint on ticker, so storage
  follows the same UPDATE-then-INSERT pattern as the market-cap repair in
  production/run_daily.py, and never clobbers market_cap.
- Margins / ROE / revenue_growth are stored as raw fractions (0.27 = 27%),
  matching how the health pipeline already treats them; the frontend formats
  them as percentages.
"""
from __future__ import annotations

from datetime import date as _date
from typing import Optional

from sqlalchemy import text

from data import fmp

# Field → FMP source key. Kept as data so the mapping is auditable in one place.
_FIELDS = (
    "pe_ttm", "eps_ttm", "revenue_ttm", "revenue_growth",
    "gross_margin", "op_margin", "net_margin", "roe", "de_ratio",
)


def extract_fundamentals_ttm(ratios, km, growth, stmts) -> dict:
    """Map FMP payloads → the 9 fundamentals fields. Missing/None → None.

    ratios: fmp.ratios_ttm(t), km: fmp.key_metrics_ttm(t),
    growth: fmp.financial_growth(t), stmts: fmp.income_statement(t, limit>=1).
    """
    ratios = ratios or {}
    km = km or {}
    growth = growth or {}
    stmts = stmts or []
    inc0 = stmts[0] if stmts else {}

    return {
        "pe_ttm":         ratios.get("priceToEarningsRatioTTM"),
        "eps_ttm":        ratios.get("netIncomePerShareTTM"),
        "revenue_ttm":    inc0.get("revenue"),
        "revenue_growth": growth.get("revenueGrowth"),
        "gross_margin":   ratios.get("grossProfitMarginTTM"),
        "op_margin":      ratios.get("operatingProfitMarginTTM"),
        "net_margin":     ratios.get("netProfitMarginTTM"),
        "roe":            km.get("returnOnEquityTTM"),
        "de_ratio":       ratios.get("debtToEquityRatioTTM"),
    }


def fetch_fundamentals(ticker: str) -> dict:
    """Fetch the FMP payloads for one ticker and extract the 9 fields.

    Returns a dict of the 9 fields (values may be None). Makes ~4 FMP calls.
    """
    ratios = fmp.ratios_ttm(ticker) or {}
    km = fmp.key_metrics_ttm(ticker) or {}
    growth = fmp.financial_growth(ticker) or {}
    stmts = fmp.income_statement(ticker, limit=1) or []
    return extract_fundamentals_ttm(ratios, km, growth, stmts)


def store_fundamentals(ticker: str, fund: dict, engine, updated: Optional[str] = None) -> None:
    """UPSERT the fundamentals for `ticker` without a UNIQUE constraint:
    UPDATE the row; if it didn't exist, INSERT. Preserves market_cap.
    """
    if updated is None:
        updated = _date.today().isoformat()
    params = {f: fund.get(f) for f in _FIELDS}
    params["ticker"] = ticker
    params["updated"] = updated

    set_clause = ", ".join(f"{f} = :{f}" for f in _FIELDS)
    with engine.begin() as conn:
        res = conn.execute(
            text(f"UPDATE fundamentals SET {set_clause}, fundamentals_updated = :updated "
                 f"WHERE ticker = :ticker"),
            params,
        )
        if res.rowcount == 0:
            cols = ", ".join(_FIELDS)
            vals = ", ".join(f":{f}" for f in _FIELDS)
            conn.execute(
                text(f"INSERT INTO fundamentals (ticker, {cols}, fundamentals_updated) "
                     f"VALUES (:ticker, {vals}, :updated)"),
                params,
            )
