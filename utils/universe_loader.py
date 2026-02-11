import pandas as pd
import requests
from io import StringIO
from database.db_connection import engine


def load_universe():

    print("Loading S&P 500 universe...")

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, headers=headers)
    html = StringIO(response.text)

    table = pd.read_html(html)[0]

    # Fix ticker formatting for Yahoo Finance
    table["Symbol"] = table["Symbol"].str.replace(".", "-", regex=False)

    # Base company table
    companies = table[["Symbol", "Security", "GICS Sector"]].rename(
        columns={
            "Symbol": "ticker",
            "Security": "company",
            "GICS Sector": "sector"
        }
    )

    # ---------- Website extraction if available ----------
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
        # Fallback domain guess
        companies["domain"] = (
            companies["company"]
            .str.lower()
            .str.replace(r"[^a-z0-9 ]", "", regex=True)
            .str.replace(" ", "") + ".com"
        )

    # Logo URL
    # Logo URL based on ticker (reliable)
    companies["logo_url"] = (
            "https://financialmodelingprep.com/image-stock/" +
            companies["ticker"] + ".png"
    )

    # Save companies table
    companies.to_sql("companies", engine, if_exists="replace", index=False)

    tickers = companies["ticker"].tolist()

    print(f"Loaded {len(tickers)} tickers")

    return tickers
