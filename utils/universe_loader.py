"""Build the daily US ticker universe from FMP.

Sources (all FMP, no Wikipedia scraping):
  - S&P 500    →  /stable/sp500-constituent              (dedicated, has sector)
  - S&P 400    →  /stable/etf/holdings?symbol=IJH        (iShares mid-cap ETF)
  - S&P 600    →  /stable/etf/holdings?symbol=IJR        (iShares small-cap ETF)
  - NASDAQ-100 →  /stable/nasdaq-constituent             (dedicated, has sector)

The two ETF-derived sources (IJH, IJR) don't expose a sector field. We
preserve sectors from the existing `companies` row when the new source
returns blank, so we never lose sector data that the daily company_profile
fetch has already populated.

Safety net: if any source returns 0 rows we abort with an exception rather
than wiping the companies table with a partial universe.
"""
import pandas as pd

from data.loaders import engine
from data.fmp import (
    sp500_constituents,
    nasdaq100_constituents,
    etf_holdings,
)
from utils.sectors import normalize_sector


# Tickers permanently excluded from the universe even if present in the source feeds.
# Reason kept inline so anyone reading load_universe() can see why.
EXCLUDED_TICKERS = {
    "HNI",  # 2026-04-23: 52/53-week fiscal calendar breaks _build_ttm_snapshot;
            #  no historical health scores can be generated → BUY gate waived
            #  inappropriately. Remove until fiscal-calendar parsing is fixed.
}


# Defensive ETF blocklist — applied to FMP-sourced rows only, not to the custom
# rows already in the `companies` table. The four index endpoints we use
# (sp500-constituent, nasdaq-constituent, IJH/IJR ETF holdings) shouldn't ever
# return ETFs by definition, but this guard catches any future data anomalies
# from FMP without us noticing.
#
# Two ETFs are explicitly kept (added as custom rows by other tooling, not by
# load_universe): SPY and VOO. They live in `companies` with sector='ETF' and
# survive every reload because `custom` rows are appended after the index
# replace.
ALLOWED_ETFS = {"SPY", "VOO"}
BLOCKED_ETFS = {
    # Broad-market index ETFs that could plausibly be misclassified into a
    # constituent feed. Not exhaustive — defense-in-depth, not a whitelist.
    "QQQ", "IVV", "IJH", "IJR", "VTI", "VXUS", "IWM", "IWB", "DIA",
    "SCHB", "SCHG", "SCHV", "ITOT", "XLF", "XLK", "XLE", "XLY", "XLI",
    "XLP", "XLV", "XLU", "XLB", "XLRE", "XLC",
    "ARKK", "ARKW", "ARKG", "ARKQ", "ARKX",
    "EFA", "EEM", "AGG", "BND", "TLT", "SHY", "GLD", "SLV",
}


# Columns preserved across reloads — owned by other parts of the pipeline.
# Without preservation, replacing the companies table would clobber these.
#
# `domain` was previously rebuilt every reload from Wikipedia's "Website" column,
# but the FMP-based feeds don't carry it. We preserve whatever's already there
# (legacy domain hints used by production/download_logos.py); new tickers get
# NULL and download_logos.py falls back to ticker-only lookup strategies.
PRESERVED_COLS = ["industry", "description", "description_short", "logo_url", "domain"]


def _is_clean_stock_ticker(t: str) -> bool:
    """ETF holdings include cash sleeves, futures, and the ETF itself.
    A real stock ticker is 1-5 uppercase letters (we re-map dots to dashes
    just like the dedicated constituent endpoints do)."""
    if not t or not isinstance(t, str):
        return False
    s = t.replace("-", "").replace(".", "")
    return s.isalpha() and s.isupper() and 1 <= len(s) <= 5


def _etf_holdings_as_constituents(etf_symbol: str) -> list:
    """Filter raw ETF holdings down to clean stock tickers.

    Drops cash sleeves (e.g. BLK CSH FND), the ETF itself, and any oddly-formed
    asset codes. Returns [{ticker, company, sector}] with sector left blank
    (FMP's ETF-holdings endpoint doesn't carry sector — the loader merges in
    the existing sector from `companies` when available)."""
    raw = etf_holdings(etf_symbol)
    out = []
    for h in raw:
        ticker = h.get("asset")
        if ticker == etf_symbol:
            continue
        if not _is_clean_stock_ticker(ticker):
            continue
        company = (h.get("name") or "").strip()
        # FMP holdings names are uppercase ("APPLE INC") — title-case for display
        company = company.title() if company else ticker
        out.append({
            "ticker":  ticker.replace(".", "-"),
            "company": company,
            "sector":  "",
        })
    return out


def _to_companies_df(rows: list) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["ticker", "company", "sector"]) if rows else pd.DataFrame(columns=["ticker", "company", "sector"])
    df["market"] = "US"
    # Normalize FMP's sector names to GICS so sp500-constituent ('Technology')
    # and S&P 500's actual GICS classification ('Information Technology')
    # don't both exist in the DB.
    df["sector"] = df["sector"].apply(normalize_sector)
    return df


def load_universe():
    """Load the US ticker universe from FMP. Aborts (raises) if any source is
    empty so we never replace the companies table with a partial universe."""

    print("Loading S&P 500 from FMP /sp500-constituent ...")
    sp500 = _to_companies_df(sp500_constituents())
    print(f"  → {len(sp500)} tickers")

    print("Loading S&P 400 from FMP IJH ETF holdings ...")
    sp400 = _to_companies_df(_etf_holdings_as_constituents("IJH"))
    print(f"  → {len(sp400)} tickers")

    print("Loading S&P 600 from FMP IJR ETF holdings ...")
    sp600 = _to_companies_df(_etf_holdings_as_constituents("IJR"))
    print(f"  → {len(sp600)} tickers")

    print("Loading NASDAQ-100 from FMP /nasdaq-constituent ...")
    ndx = _to_companies_df(nasdaq100_constituents())
    print(f"  → {len(ndx)} tickers")

    # Safety net: any feed returning 0 means FMP failed or we hit a rate limit
    # spiral. Don't replace the companies table with a partial universe — that
    # would mass-orphan existing rows and break the daily pipeline.
    for name, df in [("S&P 500", sp500), ("S&P 400", sp400), ("S&P 600", sp600), ("NASDAQ-100", ndx)]:
        if len(df) == 0:
            raise RuntimeError(
                f"Aborting universe rebuild: FMP returned 0 rows for {name}. "
                f"Refusing to wipe the companies table with a partial universe."
            )

    # Concat + dedup by ticker. Earlier sources win when a ticker appears in
    # multiple indexes (S&P 500 takes precedence over S&P 400/600/NDX, since
    # S&P 500 carries an authoritative sector that ETF-derived rows don't).
    companies = (
        pd.concat([sp500, sp400, sp600, ndx], ignore_index=True)
          .drop_duplicates(subset=["ticker"], keep="first")
    )

    if EXCLUDED_TICKERS:
        before = len(companies)
        companies = companies[~companies["ticker"].isin(EXCLUDED_TICKERS)]
        excluded_n = before - len(companies)
        if excluded_n:
            print(f"Excluded {excluded_n} ticker(s) per EXCLUDED_TICKERS: "
                  f"{sorted(EXCLUDED_TICKERS)}")

    # Defensive ETF blocklist — index feeds shouldn't return these, but guard
    # against future FMP data anomalies. SPY/VOO are kept as custom rows.
    blocked_present = companies[companies["ticker"].isin(BLOCKED_ETFS)]["ticker"].tolist()
    if blocked_present:
        companies = companies[~companies["ticker"].isin(BLOCKED_ETFS)]
        print(f"Blocked {len(blocked_present)} ETF(s) that appeared in an index feed: "
              f"{sorted(blocked_present)}")

    # ─── Preserve columns owned by other pipeline stages ───
    # logo_url is owned by production/download_logos.py — never clobber it.
    # industry / description / description_short are populated from
    # company_profile by the daily pipeline.
    # sector is special: when our new source has blank sector (ETF-derived
    # rows for S&P 400 / S&P 600), fall back to the existing companies row
    # so we don't temporarily lose sector data after a universe refresh.
    try:
        existing = pd.read_sql("SELECT * FROM companies", engine)
        index_tickers = set(companies["ticker"])
        custom = existing[~existing["ticker"].isin(index_tickers)].copy()
        if "market" not in custom.columns:
            custom["market"] = "US"

        preserved_cols = [c for c in PRESERVED_COLS if c in existing.columns]
        if preserved_cols:
            preserved = existing[["ticker"] + preserved_cols]
            companies = companies.merge(preserved, on="ticker", how="left")
            for col in preserved_cols:
                if col not in custom.columns:
                    custom[col] = None

        # Sector fallback: only fill blanks from existing data.
        # Bug fix (2026-05-04): the previous predicate did
        #   companies["sector"].astype(str).str.strip() != ""
        # which converted None values to the string "None" — never empty after
        # strip — so .where() KEPT None instead of falling back to existing.
        # Result: every update_prices() call (which invokes load_universe) was
        # silently nulling sector for ~1000 S&P 400/600 tickers, since IJH/IJR
        # ETF holdings don't carry a sector field. Use isna() + empty-string
        # check as the proper "blank" predicate.
        if "sector" in existing.columns:
            existing_sector = existing[["ticker", "sector"]].rename(
                columns={"sector": "_existing_sector"}
            )
            companies = companies.merge(existing_sector, on="ticker", how="left")
            sec = companies["sector"]
            is_blank = sec.isna() | (sec.astype(str).str.strip() == "")
            companies["sector"] = sec.mask(is_blank, companies["_existing_sector"])
            companies = companies.drop(columns=["_existing_sector"])
    except Exception as e:
        print(f"  (no existing companies to merge from — fresh DB? {e})")
        custom = pd.DataFrame()

    for col in PRESERVED_COLS:
        if col not in companies.columns:
            companies[col] = None

    companies.to_sql("companies", engine, if_exists="replace", index=False)

    if not custom.empty:
        existing_cols = pd.read_sql("SELECT * FROM companies LIMIT 0", engine).columns.tolist()
        custom = custom[[c for c in custom.columns if c in existing_cols]]
        custom.to_sql("companies", engine, if_exists="append", index=False)

    tickers = companies["ticker"].tolist()
    print(f"\nLoaded {len(tickers)} index tickers from FMP")

    # Always include custom (non-index) companies in the daily universe — these
    # are typically ETFs we track (SPY, VOO, …). Without this they'd live in
    # the companies table but get skipped by the pipeline that calls
    # load_universe(), so prices/fundamentals would never refresh.
    if not custom.empty:
        custom_tickers = [t for t in custom["ticker"].tolist() if t not in set(tickers)]
        if custom_tickers:
            print(f"Adding {len(custom_tickers)} custom ticker(s) to universe: {custom_tickers}")
            tickers = tickers + custom_tickers

    try:
        watchlist_tickers = pd.read_sql("SELECT DISTINCT ticker FROM watchlist", engine)["ticker"].tolist()
        extra = [t for t in watchlist_tickers if t not in set(tickers)]
        if extra:
            print(f"Adding {len(extra)} watchlist ticker(s) to universe: {extra}")
            existing_co = set(pd.read_sql("SELECT ticker FROM companies", engine)["ticker"])
            new_co = [t for t in extra if t not in existing_co]
            if new_co:
                pd.DataFrame({
                    "ticker":   new_co,
                    "company":  new_co,
                    "sector":   [""] * len(new_co),
                    "market":   ["US"] * len(new_co),
                    # logo_url left NULL — production/download_logos.py
                    # (called from run_daily) will populate it.
                }).to_sql("companies", engine, if_exists="append", index=False)
            tickers = tickers + extra
    except Exception as e:
        print(f"Could not add watchlist tickers to universe: {e}")

    print(f"Total universe: {len(tickers)} tickers (US-only, FMP-sourced)")
    return tickers
