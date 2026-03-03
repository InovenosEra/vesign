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

    companies["logo_url"] = (
        "https://financialmodelingprep.com/image-stock/" + companies["ticker"] + ".png"
    )

    return companies


def load_universe():

    headers = {"User-Agent": "Mozilla/5.0"}

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

    # Save companies table, preserving any manually added custom tickers
    # (tickers not in S&P 500/400/600 that were inserted outside this function).
    try:
        existing = pd.read_sql("SELECT * FROM companies", engine)
        index_tickers = set(companies["ticker"])
        custom = existing[~existing["ticker"].isin(index_tickers)]
    except Exception:
        custom = pd.DataFrame()

    companies.to_sql("companies", engine, if_exists="replace", index=False)

    if not custom.empty:
        custom.to_sql("companies", engine, if_exists="append", index=False)

    tickers = companies["ticker"].tolist()

    print(f"Loaded {len(tickers)} S&P 500 + S&P 400 + S&P 600 tickers")

    # ── Extend universe with any watchlist tickers outside the S&P 500 ──
    try:
        watchlist_tickers = pd.read_sql(
            "SELECT DISTINCT ticker FROM watchlist", engine
        )["ticker"].tolist()

        extra = [t for t in watchlist_tickers if t not in set(tickers)]

        if extra:
            print(f"Adding {len(extra)} watchlist ticker(s) to universe: {extra}")

            # Ensure each extra ticker has a row in the companies table
            existing_co = set(
                pd.read_sql("SELECT ticker FROM companies", engine)["ticker"]
            )
            new_co = [t for t in extra if t not in existing_co]
            if new_co:
                pd.DataFrame({
                    "ticker":   new_co,
                    "company":  new_co,
                    "sector":   [""] * len(new_co),
                    "logo_url": [
                        f"https://financialmodelingprep.com/image-stock/{t}.png"
                        for t in new_co
                    ],
                }).to_sql("companies", engine, if_exists="append", index=False)

            tickers = tickers + extra

    except Exception as e:
        print(f"Could not add watchlist tickers to universe: {e}")

    print(f"Total universe: {len(tickers)} tickers")

    return tickers
