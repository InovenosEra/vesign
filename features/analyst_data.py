import pandas as pd
import yfinance as yf
from datetime import datetime, UTC
from data.loaders import engine
from utils.update_guard import should_run, mark_run


def update_analyst_data():

    # ---------- run only once per day ----------
    if not should_run("analyst_update", 24):
        return

    print("Downloading analyst expectations...")

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
                "target_mean_price": info.get("targetMeanPrice"),
                "target_high_price": info.get("targetHighPrice"),
                "target_low_price": info.get("targetLowPrice"),
                "number_of_analysts": info.get("numberOfAnalystOpinions"),
                "last_update": datetime.now(UTC)
            })

        except Exception:
            continue

    if len(rows) == 0:
        print("No analyst data downloaded")
        return

    df = pd.DataFrame(rows)

    df.to_sql(
        "analyst_expectations",
        engine,
        if_exists="replace",
        index=False
    )

    # ---------- mark job completion ----------
    mark_run("analyst_update")

    print("Analyst expectations updated successfully")
