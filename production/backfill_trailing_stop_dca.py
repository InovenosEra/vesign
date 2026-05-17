"""DCA-aware variant of backfill_trailing_stop.py — re-scores every historical
signal row using the current trailing_stop_pct from config/settings.yaml, then
rebuilds trade_log. Maintains 4-tuple open_positions so the post-DCA Path B
engine (commit 4371c6e, 2026-05-12) writes correct lot_seq for add-on lots.

The original backfill_trailing_stop.py used 2-tuple (entry_price, buy_date)
which made the engine fall back to last_lot_price = entry_price → DCA add-on
gate compared against first-lot price instead of most-recent lot, corrupting
lot_seq history.

Run with vesign STOPPED so memory is free (2000+ run_scoring calls).
Idempotent — calling it twice produces the same output.

Tested 2026-05-17: 391 minutes to re-score 2104 dates (2018-01-02 → 2026-05-15)
on the prod droplet. Produces 1,770 closed trades when trailing_stop_pct=0.25.
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
    """Update open_positions from today's freshly-written signals.

    Returns 4-tuples (entry_price, buy_date, last_lot_price, lot_count) so the
    engine's DCA add-on gate (close ≤ 0.90 × last_lot_price) compares against
    the most-recent lot price instead of the original entry. Without the 4th
    element, engine.py:599-604 falls back to last_lot_price = entry_price.

    On fresh BUY (ticker not in dict): (close, date, close, 1).
    On add-on BUY (ticker already in dict): keep entry_price + buy_date,
        update last_lot_price = close, increment lot_count.
    On SELL: pop ticker.
    """
    df = pd.read_sql(
        text(
            "SELECT ticker, signal, close, date, lot_seq FROM signals "
            "WHERE DATE(date) = :d AND signal IN ('BUY','SELL')"
        ),
        engine, params={"d": target_date},
    )
    new = dict(open_positions)
    for r in df.itertuples(index=False):
        if r.signal == "BUY":
            if r.ticker in new:
                ep, bd, _, lc = new[r.ticker]
                new[r.ticker] = (ep, bd, r.close, lc + 1)
            else:
                new[r.ticker] = (r.close, r.date, r.close, 1)
        elif r.signal == "SELL":
            new.pop(r.ticker, None)
    return new


def backfill_all_signals():
    dates = pd.read_sql(
        "SELECT DISTINCT DATE(date) AS d FROM features ORDER BY d ASC",
        engine,
    )["d"].tolist()

    print(f"Re-scoring {len(dates)} dates: {dates[0]} → {dates[-1]}", flush=True)
    open_positions: dict = {}

    t0 = time.time()
    for i, d in enumerate(dates, start=1):
        run_scoring(target_date=d, open_positions=open_positions)
        open_positions = _next_open_positions(open_positions, d)
        if i % 50 == 0:
            elapsed = time.time() - t0
            eta = elapsed / i * (len(dates) - i)
            multilot = sum(1 for v in open_positions.values() if v[3] > 1)
            print(
                f"  [{i}/{len(dates)}] {d}  elapsed={elapsed/60:.1f}m  "
                f"eta={eta/60:.1f}m  open={len(open_positions)}  multi_lot={multilot}",
                flush=True,
            )
            # WAL checkpoint every 50 dates to prevent unbounded -wal growth
            with engine.connect() as conn:
                conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))
        gc.collect()

    print(f"\nRe-score done in {(time.time() - t0)/60:.1f} minutes.", flush=True)
    with engine.connect() as conn:
        conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))


def rebuild_trade_log():
    print("Rebuilding trade_log...", flush=True)
    build_trade_log()
    n = pd.read_sql(
        "SELECT COUNT(*) c FROM trade_log WHERE sell_date IS NOT NULL",
        engine,
    ).iloc[0]["c"]
    print(f"trade_log: {n} closed trades", flush=True)


if __name__ == "__main__":
    backfill_all_signals()
    rebuild_trade_log()
