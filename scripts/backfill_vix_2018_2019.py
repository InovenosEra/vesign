"""Fetch ^VIX for 2018-01-01 → 2019-12-31 via yfinance and append to vix table.

Schema: vix(date TEXT, close FLOAT). Only the close is stored.
"""
import yfinance as yf
import pandas as pd
from sqlalchemy import text
from data.loaders import engine

START = "2018-01-01"
END = "2020-01-01"  # yfinance end is exclusive


def main():
    with engine.connect() as c:
        n = c.execute(text(
            "SELECT COUNT(*) FROM vix WHERE date >= :s AND date < :e"
        ), {"s": START, "e": END}).scalar()
    if n > 100:
        print(f"VIX already populated for {START}..{END} ({n} rows). Skipping.", flush=True)
        return

    data = yf.download("^VIX", start=START, end=END, auto_adjust=False, progress=False)
    if data is None or data.empty:
        print("ERROR: VIX yfinance returned empty data", flush=True)
        return

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.droplevel(1)

    data = data.reset_index()
    out = pd.DataFrame({
        "date":  pd.to_datetime(data["Date"]),
        "close": data["Close"].astype(float),
    })
    out.to_sql("vix", engine, if_exists="append", index=False)
    print(f"Inserted {len(out)} VIX rows for {START}..{END}", flush=True)


if __name__ == "__main__":
    main()
