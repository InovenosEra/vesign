import pandas as pd
import requests
from io import StringIO
from data.loaders import engine
from utils.logo_overrides import apply_logo_overrides


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

    companies["logo_url"] = (
        "https://financialmodelingprep.com/image-stock/" + companies["ticker"] + ".png"
    )
    companies["market"] = "US"

    return companies


def _fetch_ta_index(url: str) -> pd.DataFrame:
    """Fetch a Wikipedia TASE index table and return a normalised companies DataFrame.

    Israeli Wikipedia tables use Name/Symbol/Sector column names.
    Tickers get a .TA suffix for yfinance (e.g. TEVA.TA).
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    html = StringIO(response.text)
    tables = pd.read_html(html)

    # Find the table that has a Symbol/Ticker column (flat or MultiIndex)
    def _has_symbol_col(t):
        if isinstance(t.columns, pd.MultiIndex):
            return any("Symbol" in str(c) or "Ticker" in str(c) for c in t.columns)
        return any(k in t.columns for k in ("Symbol", "Ticker"))

    table = next((t for t in tables if _has_symbol_col(t)), None)
    if table is None:
        raise ValueError(f"No table with 'Symbol' column found at {url}")

    # Flatten MultiIndex columns if present
    if isinstance(table.columns, pd.MultiIndex):
        table.columns = [" ".join(str(c) for c in col).strip() for col in table.columns]
        # Re-find columns after flattening
        sym_col = next((c for c in table.columns if "Symbol" in c or "Ticker" in c), None)
        name_col = next((c for c in table.columns if "Name" in c or "Corporation" in c), None)
        sec_col  = next((c for c in table.columns if "Sector" in c or "Industry" in c), None)
    else:
        sym_col  = next((c for c in ("Symbol", "Ticker") if c in table.columns), None) or "Symbol"
        name_col = next((c for c in ("Name", "Company", "Security") if c in table.columns), table.columns[0])
        sec_col  = next((c for c in ("Sector", "GICS Sector", "Industry") if c in table.columns), None)

    if sym_col is None:
        raise ValueError(f"Could not locate Symbol column in table from {url}")

    tickers = (
        table[sym_col]
        .astype(str)
        .str.strip()
        .apply(lambda t: t if t.endswith(".TA") else t + ".TA")
    )

    companies = pd.DataFrame({
        "ticker":  tickers,
        "company": table[name_col].astype(str).str.strip() if name_col else tickers,
        "sector":  table[sec_col].astype(str).str.strip() if sec_col else "",
        "market":  "IL",
    })

    companies["logo_url"] = (
        "https://financialmodelingprep.com/image-stock/" + companies["ticker"] + ".png"
    )

    return companies


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

    EXTRA_COLS = ["industry", "description", "description_short"]
    try:
        existing = pd.read_sql("SELECT * FROM companies", engine)
        index_tickers = set(companies["ticker"])
        custom = existing[~existing["ticker"].isin(index_tickers)].copy()
        # Drop any legacy TASE rows from the custom carryover
        custom = custom[~custom["ticker"].str.endswith(".TA", na=False)]
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

    try:
        watchlist_tickers = pd.read_sql("SELECT DISTINCT ticker FROM watchlist", engine)["ticker"].tolist()
        extra = [t for t in watchlist_tickers if t not in set(tickers) and not t.endswith(".TA")]
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
                    "logo_url": [
                        f"https://financialmodelingprep.com/image-stock/{t}.png"
                        for t in new_co
                    ],
                }).to_sql("companies", engine, if_exists="append", index=False)
            tickers = tickers + extra
    except Exception as e:
        print(f"Could not add watchlist tickers to universe: {e}")

    apply_logo_overrides(engine)
    print(f"Total universe: {len(tickers)} tickers (US-only)")
    return tickers
