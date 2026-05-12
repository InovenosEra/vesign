"""V2 strategy backfill: re-run signal scoring for every historical date,
then rebuild trade_log from scratch.

USAGE:
  python -m production.v2_backfill                # all dates
  python -m production.v2_backfill --start 2024-01-01

Idempotent: safe to re-run. Each date is recomputed in place. Existing
signals rows for that date are deleted before insert (handled inside
run_scoring).

Walks _get_open_positions() forward in memory to avoid the ~14s SQL
self-join on every iteration (saves ~6 hours on a full backfill).
"""
import argparse
import time
import pandas as pd
from sqlalchemy import text

from data.loaders import engine
from signals.engine import run_scoring, _get_open_positions, _ensure_signals_columns
from backtesting.engine import build_trade_log


def _list_target_dates(start: str | None, end: str | None) -> list[str]:
    """Distinct dates in features table, optionally filtered to [start, end)."""
    sql = "SELECT DISTINCT DATE(date) AS d FROM features"
    where_clauses = []
    if start:
        where_clauses.append(f"date >= '{start}'")
    if end:
        where_clauses.append(f"date < '{end}'")
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    sql += " ORDER BY d ASC"
    with engine.connect() as c:
        rows = pd.read_sql(sql, c)
    return [str(d)[:10] for d in rows["d"].tolist()]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", help="YYYY-MM-DD inclusive")
    p.add_argument("--end",   help="YYYY-MM-DD exclusive")
    p.add_argument("--skip-scoring", action="store_true",
                   help="Skip per-date scoring; only rebuild trade_log")
    p.add_argument("--skip-trade-log", action="store_true",
                   help="Skip trade_log rebuild; only rescore signals")
    p.add_argument("--refresh-positions-every", type=int, default=20,
                   help="Re-pull open_positions from DB every N dates (default 20)")
    args = p.parse_args()

    _ensure_signals_columns()  # makes sure vqs column exists

    if not args.skip_scoring:
        dates = _list_target_dates(args.start, args.end)
        print(f"V2 backfill — {len(dates):,} dates from {dates[0]} to {dates[-1]}")
        t0 = time.time()
        # warm the open-positions dict from BEFORE the start date
        open_positions = _get_open_positions(as_of_date=args.start) if args.start else {}
        for i, d in enumerate(dates, 1):
            run_scoring(target_date=d, open_positions=open_positions, fast_v2=True)
            # Incrementally sync open_positions with this date's BUY/SELL emissions
            # so the next iteration's run_scoring sees fresh state. Without this,
            # a BUY on day D wouldn't appear as "open" until the next bulk refresh,
            # and any RSI>70-profitable SELL between D and the refresh would be missed.
            with engine.connect() as conn:
                new_signals = conn.execute(text(
                    "SELECT ticker, signal, close FROM signals "
                    "WHERE DATE(date) = DATE(:d) AND signal IN ('BUY', 'SELL')"
                ), {"d": d}).fetchall()
            for ticker, sig, close in new_signals:
                if sig == "BUY":
                    # Path B 4-tuple: (entry_price, buy_date, last_lot_price, lot_count).
                    # Fresh entry → all four from this BUY. Add-on → keep entry_price+buy_date,
                    # update last_lot_price+lot_count to reflect the new lot.
                    if ticker in open_positions:
                        prev = open_positions[ticker]
                        entry_p, buy_d = prev[0], prev[1]
                        prev_count = int(prev[3]) if len(prev) > 3 else 1
                        open_positions[ticker] = (entry_p, buy_d, float(close), prev_count + 1)
                    else:
                        open_positions[ticker] = (float(close), d, float(close), 1)
                elif sig == "SELL" and ticker in open_positions:
                    del open_positions[ticker]
            # Periodic full refresh as drift insurance + to log progress.
            if i % args.refresh_positions_every == 0:
                next_d = (pd.Timestamp(d) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                open_positions = _get_open_positions(as_of_date=next_d)
                elapsed = time.time() - t0
                eta = elapsed / i * (len(dates) - i)
                print(f"  [{i}/{len(dates)}] {d}  "
                      f"({elapsed/60:.1f}min elapsed, ETA {eta/60:.0f}min)  "
                      f"open positions: {len(open_positions):,}")
        print(f"\nScoring complete in {(time.time() - t0)/60:.1f}min")

    if not args.skip_trade_log:
        print("\nRebuilding trade_log from V2 signals...")
        build_trade_log()
        # quick sanity print
        import sqlite3
        from pathlib import Path
        db = Path(__file__).resolve().parents[1] / "vesign.db"
        con = sqlite3.connect(db)
        n = con.execute("SELECT COUNT(*) FROM trade_log").fetchone()[0]
        if n > 0:
            stats = con.execute(
                "SELECT AVG(return_pct), AVG(CASE WHEN return_pct > 0 THEN 1.0 ELSE 0.0 END) "
                "FROM trade_log WHERE return_pct IS NOT NULL"
            ).fetchone()
            print(f"trade_log: N={n:,}  mean={stats[0]*100:+.2f}%  WR={stats[1]:.3f}")
        con.close()


if __name__ == "__main__":
    main()
