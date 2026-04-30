"""Full-history features rebuild — fixes the 280-day chunk bug that left
~1.1M signal rows with NULL `week52_high` (and therefore corrupted the
`week52_condition` BUY gate via the "pass through if NULL" rule).

Run with vesign stopped. Calls compute_and_save_features_chunked with
`days` set wider than the entire price history so every row is recomputed
with proper rolling-window warmup.
"""
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from data.loaders import engine
from features.technical import compute_and_save_features_chunked


def main():
    with engine.connect() as conn:
        total_dates = conn.execute(text("SELECT COUNT(DISTINCT date) FROM daily_prices")).scalar()
    print(f"daily_prices has {total_dates} distinct dates — rebuilding all of them")

    # `days` larger than total_dates makes the function recompute the whole
    # feature table; warmup of 252 ensures rolling(252) is fully warm.
    t0 = time.time()
    compute_and_save_features_chunked(engine, days=total_dates + 1, chunk_size=50, warmup=252)
    print(f"features rebuild done in {(time.time() - t0)/60:.1f} min")


if __name__ == "__main__":
    main()
