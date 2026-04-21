"""Manual logo URL overrides for tickers where FMP returns a placeholder or 404.

Applied in two places:
  1. utils/universe_loader.py — right after load_universe() rewrites companies
     (so the daily pipeline never loses overrides even if company_info is throttled).
  2. data/market_data.py update_company_info — defense in depth, re-applies after
     FMP has had its turn to set logo_url from the profile endpoint.
"""

LOGO_OVERRIDES = {
    "PENG": "https://cdn.prod.website-files.com/6764579f0a24e5a0083f25bb/67bb88245ce879aaca499ddb_schema--penguin-logo.jpg",
    "HWKN": "https://www.hawkinsinc.com/wp-content/uploads/2025/10/Hawkins-logo-300-x-300.jpg",
    "GTM":  "https://img.logo.dev/zoominfo.com?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "AAMI": "https://img.logo.dev/ticker/AAMI?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "FLG":  "https://img.logo.dev/ticker/FLG?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
    "VSNT": "https://img.logo.dev/ticker/VSNT?token=pk_X-1ZO13GSgeOoUrIuJ6GMQ",
}


def apply_logo_overrides(engine):
    """UPDATE companies.logo_url for each override. Safe to call repeatedly."""
    from sqlalchemy import text
    with engine.begin() as conn:
        for ticker, url in LOGO_OVERRIDES.items():
            conn.execute(text("UPDATE companies SET logo_url = :url WHERE ticker = :t"),
                         {"url": url, "t": ticker})
