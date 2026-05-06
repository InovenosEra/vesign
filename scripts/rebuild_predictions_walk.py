"""Wipe and rebuild predictions_walk from 2020-01-02 to today using walk-forward.

Quarterly cadence: for each Q in [2020-Q1, ..., this-quarter):
    - train models with train_end_date = Q.start
    - predict every date in [Q.start, Q+1.start) using those models
For the current open quarter:
    - train with cutoff = current-quarter.start
    - predict from current-quarter.start to today

After this script finishes, scripts/verify_no_leak.py predictions_walk MUST pass.
"""
from datetime import date, timedelta

from sqlalchemy import text

from data.loaders import engine
from models.walk_forward import (
    quarterly_cutoffs, train_for_cutoff, predict_period,
    ensure_predictions_walk_table, PREDICTIONS_TABLE,
)

WALK_START = date(2020, 1, 1)


def main():
    today = date.today()
    ensure_predictions_walk_table()

    with engine.begin() as c:
        c.execute(text(f"DELETE FROM {PREDICTIONS_TABLE}"))

    cuts = quarterly_cutoffs(WALK_START, today)
    print(f"Cutoffs: {len(cuts)}  ({cuts[0]} ... {cuts[-1]})", flush=True)

    total = 0
    for i, cutoff in enumerate(cuts):
        period_end = cuts[i + 1] if i + 1 < len(cuts) else (today + timedelta(days=1))
        print(f"[{i+1}/{len(cuts)}] cutoff={cutoff} period=[{cutoff},{period_end})", flush=True)
        train_for_cutoff(cutoff)
        n = predict_period(cutoff, cutoff, period_end)
        total += n
        print(f"  wrote {n:,} rows  (running total: {total:,})", flush=True)
    print(f"DONE: {total:,} rows written to {PREDICTIONS_TABLE}", flush=True)


if __name__ == "__main__":
    main()
