import yfinance as yf
import pandas as pd
from database.db_connection import engine


def update_analyst_data():

    print("Downloading analyst expectations...")

    tickers = pd.read_sql(
        "SELECT ticker FROM companies",
        engine
    )["ticker"].tolist()
    records = []

    for t in tickers:
        try:
            tk = yf.Ticker(t)
            info = tk.info

            records.append({
                "ticker": t,
                "target_mean_price": info.get("targetMeanPrice"),
                "target_high_price": info.get("targetHighPrice"),
                "target_low_price": info.get("targetLowPrice"),
                "recommendation_mean": info.get("recommendationMean"),
                "num_analysts": info.get("numberOfAnalystOpinions")
            })

        except Exception:
            continue

    df = pd.DataFrame(records)

    df.to_sql(
        "analyst_expectations",
        engine,
        if_exists="replace",
        index=False
    )

    print("Analyst expectations updated")
