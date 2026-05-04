"""Sector taxonomy normalization.

The DB used to mix two parallel naming conventions because the universe came
from Wikipedia (GICS-style: 'Information Technology', 'Health Care', 'Materials')
and from FMP (Yahoo-style: 'Technology', 'Healthcare', 'Basic Materials'). The
allocator and any downstream sector grouping double-counted. We standardize on
GICS — the industry-standard taxonomy used by S&P and institutional research.

Apply normalize_sector() at every point where a sector enters the DB:
  - utils.universe_loader.load_universe (FMP constituent feeds + ETF holdings)
  - data.market_data.update_company_info (FMP company_profile refresh)

Run normalize_existing() once to fix legacy rows.
"""
from typing import Optional

# Map: any source name (lower-cased, stripped) -> canonical GICS sector.
# Keep keys lower-cased for case-insensitive lookup.
_GICS_MAP = {
    # Yahoo / FMP variants → GICS
    "technology":             "Information Technology",
    "information technology": "Information Technology",

    "financial services": "Financials",
    "financials":         "Financials",

    "healthcare":  "Health Care",
    "health care": "Health Care",

    "consumer cyclical":      "Consumer Discretionary",
    "consumer discretionary": "Consumer Discretionary",

    "basic materials": "Materials",
    "materials":       "Materials",

    "consumer defensive": "Consumer Staples",
    "consumer staples":   "Consumer Staples",

    # Sectors with consistent naming across both sources
    "industrials":            "Industrials",
    "energy":                 "Energy",
    "utilities":              "Utilities",
    "real estate":            "Real Estate",
    "communication services": "Communication Services",

    # Special non-GICS bucket we use for ETFs we track (SPY/VOO).
    # Keep as-is so the engine can filter on `sector='ETF'`.
    "etf": "ETF",
}


def normalize_sector(value: Optional[str]) -> Optional[str]:
    """Map any incoming sector string to its canonical GICS form.

    Returns None for empty/null input (allows DB NULL to propagate).
    Returns the input unchanged if it's not in the map — better to surface
    an unknown sector than silently squash it.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return _GICS_MAP.get(s.lower(), s)


def normalize_existing(engine) -> dict:
    """One-shot: normalize every sector currently in the companies table.

    Returns a dict {old_sector: new_sector} of the changes applied.
    """
    import pandas as pd
    from sqlalchemy import text

    df = pd.read_sql("SELECT DISTINCT sector FROM companies WHERE sector IS NOT NULL", engine)
    changes = {}
    with engine.begin() as conn:
        for old in df["sector"]:
            new = normalize_sector(old)
            if new != old:
                conn.execute(
                    text("UPDATE companies SET sector = :new WHERE sector = :old"),
                    {"new": new, "old": old},
                )
                changes[old] = new
    return changes
