"""Pre-deploy comparison: yfinance vs current FMP analyst data.

NO DB WRITES. Reads current FMP values from analyst_expectations, fetches
fresh yfinance values, and prints a report. Run this BEFORE flipping
ANALYST_SOURCE=yfinance to confirm coverage is reasonable and signal-shift
expectations are sane.

Usage:
    venv/bin/python scripts/dry_run_yfinance_analyst.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from sqlalchemy import text
from data.loaders import engine
from data.yfinance_analyst import get_targets_batch


def main():
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT ae.ticker,
                   ae.target_mean_price AS fmp_mean,
                   ae.target_high_price AS fmp_high,
                   ae.target_low_price  AS fmp_low,
                   ae.number_of_analysts AS fmp_n,
                   fund.market_cap
            FROM analyst_expectations ae
            LEFT JOIN fundamentals fund USING(ticker)
            WHERE ae.ticker NOT LIKE '%.TA'
        """), conn)

    tickers = df["ticker"].tolist()
    print(f"Fetching yfinance for {len(tickers)} tickers (~{len(tickers)/4/60:.1f} min)…")
    yf_out = get_targets_batch(tickers, max_workers=4, sleep_sec=1.0)

    df["yf_mean"]  = [yf_out.get(t, {}).get("target_mean_price")  if yf_out.get(t) else None for t in df["ticker"]]
    df["yf_high"]  = [yf_out.get(t, {}).get("target_high_price")  if yf_out.get(t) else None for t in df["ticker"]]
    df["yf_low"]   = [yf_out.get(t, {}).get("target_low_price")   if yf_out.get(t) else None for t in df["ticker"]]
    df["yf_n"]     = [yf_out.get(t, {}).get("number_of_analysts") if yf_out.get(t) else None for t in df["ticker"]]

    n_total      = len(df)
    n_yf         = df["yf_mean"].notna().sum()
    n_fmp_only   = ((df["fmp_mean"].notna()) & (df["yf_mean"].isna())).sum()
    n_yf_only    = ((df["yf_mean"].notna())  & (df["fmp_mean"].isna())).sum()
    n_both_empty = ((df["fmp_mean"].isna())  & (df["yf_mean"].isna())).sum()

    print()
    print("=== yfinance vs FMP coverage comparison ===")
    print(f"Total tickers:                {n_total}")
    print(f"yfinance returned data:       {n_yf}  ({n_yf/n_total*100:.1f}%)")
    print(f"FMP-only (yfinance empty):    {n_fmp_only}")
    print(f"yfinance-only (FMP empty):    {n_yf_only}")
    print(f"Both empty (true gap):        {n_both_empty}")

    df["delta_mean_pct"] = (df["yf_mean"] - df["fmp_mean"]) / df["fmp_mean"] * 100
    print()
    print("=== Largest delta_mean changes (yfinance vs FMP) ===")
    print(df.sort_values("delta_mean_pct", key=lambda s: s.abs(), na_position='last')[[
        "ticker", "fmp_mean", "yf_mean", "delta_mean_pct", "fmp_n", "yf_n"
    ]].head(20).to_string(index=False))

    print()
    print("=== Tickers FMP would still be needed for (top 30 by market cap) ===")
    fmp_needed = df[df["yf_mean"].isna() & df["fmp_mean"].notna()].sort_values("market_cap", ascending=False).head(30)
    print(fmp_needed[["ticker", "market_cap", "fmp_mean", "fmp_n"]].to_string(index=False))


if __name__ == "__main__":
    main()
