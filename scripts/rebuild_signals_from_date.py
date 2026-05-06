"""Wipe signals from 2020-01-02 onwards and re-run scoring chronologically.

Uses the engine's existing target_date mode. Slow but correct: ~3s/date * 1500
trading days ≈ 75 minutes. Caller must stop vesign before running.
"""
from sqlalchemy import text

from data.loaders import engine
from signals.engine import run_scoring

START = "2020-01-02"


def main():
    with engine.begin() as c:
        c.execute(text("DELETE FROM signals WHERE DATE(date) >= :s"), {"s": START})
    with engine.connect() as c:
        dates = [r[0] for r in c.execute(text(
            "SELECT DISTINCT DATE(date) FROM features WHERE DATE(date) >= :s ORDER BY date"
        ), {"s": START})]
    print(f"re-scoring {len(dates)} dates", flush=True)

    for i, d in enumerate(dates, 1):
        run_scoring(target_date=d)
        if i % 50 == 0:
            print(f"  [{i}/{len(dates)}] {d}", flush=True)
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
