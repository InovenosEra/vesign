"""Re-run scoring for a date range. Designed for 2-process parallel execution.

Usage:
    python scripts/rebuild_signals_range.py --start 2020-01-02 --end 2023-06-30
    python scripts/rebuild_signals_range.py --start 2023-07-01 --end 2026-05-05

Each process:
- DELETEs signals in its OWN range only (so two processes don't clobber each other)
- Iterates the date list ascending
- Calls run_scoring(target_date=d) for each date
- Logs progress every 50 dates with timestamps

Caller must stop vesign first if running parallel; both processes will write to
signals concurrently. SQLite WAL mode handles concurrent readers + serialized
writers (per-date writes are short).
"""
import argparse
import sys
import time
from datetime import datetime

from sqlalchemy import text

from data.loaders import engine
from signals.engine import run_scoring


def log(msg: str) -> None:
    ts = datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end",   required=True, help="YYYY-MM-DD inclusive")
    args = ap.parse_args()

    log(f"START rebuild range [{args.start}, {args.end}]")

    with engine.begin() as c:
        c.execute(
            text("DELETE FROM signals WHERE DATE(date) >= :s AND DATE(date) <= :e"),
            {"s": args.start, "e": args.end},
        )
    log(f"DELETE complete for range")

    with engine.connect() as c:
        dates = [r[0] for r in c.execute(
            text(
                "SELECT DISTINCT DATE(date) FROM features "
                "WHERE DATE(date) >= :s AND DATE(date) <= :e ORDER BY date"
            ),
            {"s": args.start, "e": args.end},
        )]
    log(f"Re-scoring {len(dates)} dates from {dates[0] if dates else 'NONE'} to {dates[-1] if dates else 'NONE'}")

    t0 = time.time()
    for i, d in enumerate(dates, 1):
        run_scoring(target_date=d)
        if i % 50 == 0 or i == len(dates):
            dt = time.time() - t0
            rate = i / dt if dt > 0 else 0
            eta_min = (len(dates) - i) / rate / 60 if rate > 0 else 0
            log(f"  [{i}/{len(dates)}] {d}  rate={rate:.2f} dates/s  eta={eta_min:.1f} min")
    log(f"DONE range [{args.start}, {args.end}] in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
