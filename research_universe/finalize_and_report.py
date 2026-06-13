"""STEP 3 — Concatenate per-symbol parquets into universe_prices.parquet and
print the dataset report. Writes ONLY research_universe/universe_prices.parquet.
"""
import os
import glob
import csv

import pandas as pd

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_CSV = os.path.join(OUT_DIR, "universe_master.csv")
PRICE_DIR = os.path.join(OUT_DIR, "prices")
PRICES_PARQUET = os.path.join(OUT_DIR, "universe_prices.parquet")
FAIL_CSV = os.path.join(OUT_DIR, "failures.csv")
FETCH_LOG = os.path.join(OUT_DIR, "fetch.log")


def human(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def main():
    master = pd.read_csv(MASTER_CSV, dtype=str).fillna("")
    n_active = (master.status == "active").sum()
    n_delisted = (master.status == "delisted").sum()

    files = sorted(glob.glob(os.path.join(PRICE_DIR, "*.parquet")))
    print(f"Concatenating {len(files)} per-symbol parquet files…")
    frames = [pd.read_parquet(f) for f in files]
    prices = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["symbol", "date", "open", "high", "low", "close", "adjclose", "volume"])
    prices.to_parquet(PRICES_PARQUET, index=False)

    saved_syms = set(prices["symbol"].unique())
    delisted_syms = set(master.loc[master.status == "delisted", "symbol"])
    delisted_saved = sorted(saved_syms & delisted_syms)

    # delisted series whose last saved date sits exactly at delistedDate cutoff
    dd_map = dict(zip(master.symbol, master.delistedDate))
    truncated_at_cutoff = 0
    for s in delisted_saved:
        last = prices.loc[prices.symbol == s, "date"].max()
        if dd_map.get(s) and str(last) <= dd_map[s] and str(last) >= dd_map[s][:7]:
            truncated_at_cutoff += 1

    failed = 0
    if os.path.exists(FAIL_CSV):
        with open(FAIL_CSV) as f:
            failed = max(0, sum(1 for _ in f) - 1)

    size = os.path.getsize(PRICES_PARQUET)

    print("\n" + "=" * 64)
    print("RESEARCH UNIVERSE — survivorship-bias-free Nasdaq common stock")
    print("=" * 64)
    print("\n— MASTER —")
    print(f"  active             : {n_active}")
    print(f"  delisted (>=2020)  : {n_delisted}")
    print(f"  total unique       : {len(master)}")
    print("\n— PRICES —")
    print(f"  symbols with data  : {prices['symbol'].nunique()}")
    print(f"  total price rows   : {len(prices):,}")
    if len(prices):
        print(f"  date range         : {prices['date'].min()} -> {prices['date'].max()}")
        print(f"  adjclose populated : {prices['adjclose'].notna().sum():,} / {len(prices):,}")
    print(f"  symbols failed/empty (in failures.csv): {failed}")
    print(f"  delisted series ending at delistedDate cutoff: {truncated_at_cutoff}")
    print(f"\n  parquet on disk    : {human(size)}  ({PRICES_PARQUET})")

    print("\n— SAMPLE: universe_master (5 rows) —")
    print(master.head(5).to_string(index=False))
    print("\n— SAMPLE: universe_prices (5 rows) —")
    print(prices.head(5).to_string(index=False))

    # surface the fetcher's own DONE summary if present (authoritative truncation count)
    if os.path.exists(FETCH_LOG):
        with open(FETCH_LOG) as f:
            done = [ln for ln in f if ln.startswith("DONE")]
        if done:
            print("\n— fetcher summary —\n  " + done[-1].strip())


if __name__ == "__main__":
    main()
