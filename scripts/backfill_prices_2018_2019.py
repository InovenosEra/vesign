"""Fetch 2018-01-01 → 2019-12-31 prices for current US universe via FMP.

Idempotent: re-running skips tickers that already have 2018-01 rows.
Memory-conscious: appends per-ticker, no cross-ticker concat in pandas.
"""
import sys, time
import pandas as pd
from sqlalchemy import text
from data.loaders import engine
from data.fmp import historical_prices

START = "2018-01-01"
END = "2019-12-31"


def already_has_2018(ticker: str) -> bool:
    with engine.connect() as c:
        n = c.execute(
            text("SELECT COUNT(*) FROM daily_prices WHERE ticker=:t AND date < '2019-01-01' AND date >= '2018-01-01'"),
            {"t": ticker},
        ).scalar()
    return n > 0


def main():
    with engine.connect() as c:
        tickers = [r[0] for r in c.execute(text(
            "SELECT DISTINCT ticker FROM companies WHERE ticker NOT LIKE '%.TA' ORDER BY ticker"
        ))]
    print(f"Backfilling {len(tickers)} tickers for {START}..{END}", flush=True)

    ok, skipped, missing, errors = 0, 0, 0, 0
    for i, t in enumerate(tickers, 1):
        if already_has_2018(t):
            skipped += 1
            continue
        try:
            df = historical_prices(t, START, END)
        except Exception as e:
            print(f"  [{i}/{len(tickers)}] {t}: ERROR {e}", flush=True)
            errors += 1
            time.sleep(1)
            continue
        if df is None or df.empty:
            missing += 1
            continue
        try:
            df.to_sql("daily_prices", engine, if_exists="append", index=False)
            ok += 1
        except Exception as e:
            print(f"  [{i}/{len(tickers)}] {t}: WRITE-ERROR {e}", flush=True)
            errors += 1
        if i % 50 == 0:
            print(f"  [{i}/{len(tickers)}] ok={ok} skipped={skipped} missing={missing} errors={errors}", flush=True)
    print(f"DONE: ok={ok} skipped={skipped} missing={missing} errors={errors}", flush=True)


if __name__ == "__main__":
    main()
