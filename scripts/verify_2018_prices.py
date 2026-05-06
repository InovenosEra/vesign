"""Asserts the 2018-2019 backfill is in place."""
import sqlite3, sys

DB = "/opt/vesign/vesign.db"


def main():
    c = sqlite3.connect(DB)
    n_2018 = c.execute("SELECT COUNT(*) FROM daily_prices WHERE date >= '2018-01-01' AND date < '2019-01-01'").fetchone()[0]
    n_2019 = c.execute("SELECT COUNT(*) FROM daily_prices WHERE date >= '2019-01-01' AND date < '2020-01-01'").fetchone()[0]
    tickers_with_2018 = c.execute("SELECT COUNT(DISTINCT ticker) FROM daily_prices WHERE date >= '2018-01-01' AND date < '2019-01-01'").fetchone()[0]
    print(f"2018 rows: {n_2018:,}  2019 rows: {n_2019:,}  tickers w/ 2018 history: {tickers_with_2018}")
    if n_2018 < 200_000 or n_2019 < 200_000 or tickers_with_2018 < 800:
        print("FAIL: 2018-2019 backfill incomplete")
        sys.exit(1)
    print("OK")


if __name__ == "__main__":
    main()
