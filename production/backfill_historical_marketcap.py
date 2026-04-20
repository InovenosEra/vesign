"""Fetch quarterly shares_outstanding for all US tickers and populate
`market_cap_history` table. Backend queries then compute per-date market cap
as: close(date) × shares_outstanding(latest quarter <= date).

Uses FMP `/stable/enterprise-values?period=quarter&limit=30` (~7 years of
quarterly data per ticker on Premium plan).
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from data import fmp
from data.loaders import engine


def fetch_enterprise_values(ticker: str) -> list:
    data = fmp._get("enterprise-values", {"symbol": ticker, "period": "quarter", "limit": 30})
    if not isinstance(data, list):
        return []
    rows = []
    for item in data:
        date_str = item.get("date")
        shares = item.get("numberOfShares")
        mcap = item.get("marketCapitalization")
        if not date_str or shares is None:
            continue
        rows.append({
            "ticker": ticker,
            "date": date_str[:10],
            "shares_outstanding": float(shares),
            "market_cap": float(mcap) if mcap is not None else None,
        })
    return rows


def main():
    started = datetime.now()
    tickers = pd.read_sql(
        "SELECT ticker FROM companies WHERE COALESCE(market,'US') = 'US'",
        engine,
    )["ticker"].tolist()
    print(f"Tickers: {len(tickers)}")

    print("Fetching enterprise-values (quarterly) for all tickers…")
    all_rows = []
    done = 0
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_enterprise_values, t): t for t in tickers}
        for fut in as_completed(futures):
            all_rows.extend(fut.result())
            done += 1
            if done % 200 == 0:
                print(f"  {done}/{len(tickers)} fetched, {len(all_rows):,} rows total")
    print(f"  total rows: {len(all_rows):,}")

    if not all_rows:
        print("No data — aborting.")
        return

    df = pd.DataFrame(all_rows)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS market_cap_history"))
    df.to_sql("market_cap_history", engine, if_exists="replace", index=False)
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX idx_mch_ticker_date ON market_cap_history(ticker, date)"))

    with engine.connect() as conn:
        per_ticker = conn.execute(text("SELECT COUNT(DISTINCT ticker) FROM market_cap_history")).scalar()
        date_min = conn.execute(text("SELECT MIN(date) FROM market_cap_history")).scalar()
        date_max = conn.execute(text("SELECT MAX(date) FROM market_cap_history")).scalar()

    print(f"\nSaved: {per_ticker} tickers, dates {date_min} → {date_max}")
    print(f"Elapsed: {datetime.now() - started}")


if __name__ == "__main__":
    main()
