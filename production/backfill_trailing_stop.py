"""One-shot backfill: re-score every historical signal row using the current
trailing_stop_pct in config/settings.yaml, then rebuild trade_log.

Run with vesign STOPPED so memory is free (1500+ run_scoring calls).
Idempotent — calling it twice produces the same output.

Maintains open_positions in memory across iterations so each call avoids the
expensive SQL self-join on the signals table (~11s near end of history).
"""
import sys
import os
import gc
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from sqlalchemy import text

from data.loaders import engine
from signals.engine import run_scoring
from backtesting.engine import build_trade_log


def _next_open_positions(open_positions: dict, target_date: str) -> dict:
    """Update the in-memory dict from the signals just written for `target_date`.

    Adds any new BUY rows; removes tickers whose row is now SELL.
    """
    df = pd.read_sql(
        text(
            "SELECT ticker, signal, close, date FROM signals "
            "WHERE DATE(date) = :d AND signal IN ('BUY','SELL')"
        ),
        engine, params={"d": target_date},
    )
    new = dict(open_positions)
    for r in df.itertuples(index=False):
        if r.signal == "BUY" and r.ticker not in new:
            new[r.ticker] = (r.close, r.date)
        elif r.signal == "SELL":
            new.pop(r.ticker, None)
    return new


def backfill_all_signals():
    dates = pd.read_sql(
        "SELECT DISTINCT DATE(date) AS d FROM features ORDER BY d ASC",
        engine,
    )["d"].tolist()

    print(f"Re-scoring {len(dates)} dates: {dates[0]} → {dates[-1]}")
    open_positions: dict = {}      # {ticker: (entry_price, buy_date)}

    t0 = time.time()
    for i, d in enumerate(dates, start=1):
        run_scoring(target_date=d, open_positions=open_positions)
        open_positions = _next_open_positions(open_positions, d)
        if i % 50 == 0:
            elapsed = time.time() - t0
            eta = elapsed / i * (len(dates) - i)
            print(
                f"  [{i}/{len(dates)}] elapsed={elapsed/60:.1f}m  "
                f"eta={eta/60:.1f}m  open_positions={len(open_positions)}"
            )
        gc.collect()

    print(f"Done in {(time.time() - t0)/60:.1f} minutes.")


def rebuild_trade_log():
    print("Rebuilding trade_log...")
    build_trade_log()
    n = pd.read_sql(
        "SELECT COUNT(*) c FROM trade_log WHERE sell_date IS NOT NULL",
        engine,
    ).iloc[0]["c"]
    print(f"trade_log: {n} closed trades")


if __name__ == "__main__":
    backfill_all_signals()
    rebuild_trade_log()
