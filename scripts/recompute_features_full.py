"""Wipe and rebuild the features table from 2018-01-01 onwards.

Processes one ticker at a time to avoid concatenating ~3M rows in memory
(droplet is 4GB RAM + 2GB swap).
"""
import gc
import pandas as pd
from sqlalchemy import text
from data.loaders import engine
from features.technical_indicators import add_indicators


def main():
    with engine.begin() as c:
        c.execute(text("DELETE FROM features"))
    with engine.connect() as c:
        tickers = [r[0] for r in c.execute(text(
            "SELECT DISTINCT ticker FROM daily_prices WHERE ticker NOT LIKE '%.TA' ORDER BY ticker"
        ))]
    print(f"Recomputing features for {len(tickers)} tickers", flush=True)

    written = 0
    for i, t in enumerate(tickers, 1):
        prices = pd.read_sql(
            text("SELECT date,ticker,open,high,low,close,volume FROM daily_prices "
                 "WHERE ticker=:t AND date >= '2018-01-01' ORDER BY date"),
            engine, params={"t": t},
        )
        if prices.empty:
            continue
        feat = add_indicators(prices.copy())
        feat.to_sql("features", engine, if_exists="append", index=False)
        written += len(feat)
        del prices, feat
        if i % 100 == 0:
            gc.collect()
            print(f"  [{i}/{len(tickers)}] rows written: {written:,}", flush=True)
    print(f"DONE: {written:,} feature rows written", flush=True)


if __name__ == "__main__":
    main()
