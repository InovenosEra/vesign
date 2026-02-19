import pandas as pd
import yfinance as yf
from data.loaders import engine
from utils.update_guard import should_run, mark_run


def update_fundamentals():

    # ---------- run only if needed ----------
    if not should_run("fundamentals_update", 24):
        return

    print("Updating fundamentals...")

    tickers = pd.read_sql(
        "SELECT ticker FROM companies",
        engine
    )["ticker"].tolist()

    rows = []

    for t in tickers:
        try:
            info = yf.Ticker(t).info

            rows.append({
                "ticker": t,
                "market_cap": info.get("marketCap")
            })

        except Exception:
            continue

    if len(rows) == 0:
        print("No fundamentals downloaded")
        return

    df = pd.DataFrame(rows)

    df.to_sql(
        "fundamentals",
        engine,
        if_exists="replace",
        index=False
    )

    # ---------- mark job completion ----------
    mark_run("fundamentals_update")

    print("Fundamentals updated successfully")
