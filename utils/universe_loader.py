import pandas as pd
import requests
from io import StringIO
from data.loaders import engine


def _fetch_index_table(url: str) -> pd.DataFrame:
    """Fetch a Wikipedia S&P index table and return a normalised companies DataFrame."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    html = StringIO(response.text)
    table = pd.read_html(html)[0]

    table["Symbol"] = table["Symbol"].str.replace(".", "-", regex=False)

    companies = table[["Symbol", "Security", "GICS Sector"]].rename(
        columns={"Symbol": "ticker", "Security": "company", "GICS Sector": "sector"}
    )

    if "Website" in table.columns:
        websites = table[["Symbol", "Website"]].rename(
            columns={"Symbol": "ticker", "Website": "website"}
        )
        companies = companies.merge(websites, on="ticker", how="left")
        companies["domain"] = (
            companies["website"]
            .astype(str)
            .str.replace("https://", "", regex=False)
            .str.replace("http://", "", regex=False)
            .str.split("/")
            .str[0]
        )
    else:
        companies["domain"] = (
            companies["company"]
            .str.lower()
            .str.replace(r"[^a-z0-9 ]", "", regex=True)
            .str.replace(" ", "") + ".com"
        )

    companies["market"] = "US"

    return companies


# Tickers permanently excluded from the universe even if present in S&P indexes.
# Reason kept inline so anyone reading load_universe() can see why.
EXCLUDED_TICKERS = {
    "HNI",  # 2026-04-23: 52/53-week fiscal calendar breaks _build_ttm_snapshot;
            #  no historical health scores can be generated → BUY gate waived
            #  inappropriately. Remove until fiscal-calendar parsing is fixed.
}


def load_universe():
    """Load the US ticker universe (S&P 500 + 400 + 600). TASE removed 2026-04-23."""

    print("Loading S&P 500 universe...")
    sp500 = _fetch_index_table("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    print(f"Loaded {len(sp500)} S&P 500 tickers")

    print("Loading S&P 400 universe...")
    sp400 = _fetch_index_table("https://en.wikipedia.org/wiki/List_of_S%26P_400_companies")
    print(f"Loaded {len(sp400)} S&P 400 tickers")

    print("Loading S&P 600 universe...")
    sp600 = _fetch_index_table("https://en.wikipedia.org/wiki/List_of_S%26P_600_companies")
    print(f"Loaded {len(sp600)} S&P 600 tickers")

    companies = pd.concat([sp500, sp400, sp600], ignore_index=True).drop_duplicates(subset=["ticker"])

    if EXCLUDED_TICKERS:
        before = len(companies)
        companies = companies[~companies["ticker"].isin(EXCLUDED_TICKERS)]
        excluded_n = before - len(companies)
        if excluded_n:
            print(f"Excluded {excluded_n} ticker(s) per EXCLUDED_TICKERS: "
                  f"{sorted(EXCLUDED_TICKERS)}")

    # logo_url is preserved across reloads — owned by production/download_logos.py.
    # Without preservation, this table replace would clobber the self-hosted
    # /logos/{T}.png paths back to NULL (or worse, an arbitrary URL).
    EXTRA_COLS = ["industry", "description", "description_short", "logo_url"]
    try:
        existing = pd.read_sql("SELECT * FROM companies", engine)
        index_tickers = set(companies["ticker"])
        custom = existing[~existing["ticker"].isin(index_tickers)].copy()
        if "market" not in custom.columns:
            custom["market"] = "US"
        preserved_cols = [c for c in EXTRA_COLS if c in existing.columns]
        if preserved_cols:
            preserved = existing[["ticker"] + preserved_cols]
            companies = companies.merge(preserved, on="ticker", how="left")
            for col in preserved_cols:
                if col not in custom.columns:
                    custom[col] = None
    except Exception:
        custom = pd.DataFrame()

    for col in EXTRA_COLS:
        if col not in companies.columns:
            companies[col] = None

    companies.to_sql("companies", engine, if_exists="replace", index=False)

    if not custom.empty:
        existing_cols = pd.read_sql("SELECT * FROM companies LIMIT 0", engine).columns.tolist()
        custom = custom[[c for c in custom.columns if c in existing_cols]]
        custom.to_sql("companies", engine, if_exists="append", index=False)

    tickers = companies["ticker"].tolist()
    print(f"Loaded {len(tickers)} total US tickers")

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

    print(f"Total universe: {len(tickers)} tickers (US-only)")
    return tickers
