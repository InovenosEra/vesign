import pandas as pd
from sqlalchemy import text
from data.loaders import engine


def compute_forward_returns():

    prices = pd.read_sql("SELECT date,ticker,close FROM daily_prices", engine)

    prices = prices.sort_values(["ticker", "date"])

    prices["fwd_5d"] = (
        prices.groupby("ticker")["close"].shift(-5) / prices["close"] - 1
    )

    prices["fwd_20d"] = (
        prices.groupby("ticker")["close"].shift(-20) / prices["close"] - 1
    )

    # Drop rows where either forward return is unknown (last 20 trading days
    # per ticker). These rows cannot serve as valid training labels.
    prices = prices.dropna(subset=["fwd_5d", "fwd_20d"])

    # Write to a staging table first, then atomically rename to avoid leaving
    # forward_returns in a corrupt state if the write fails mid-way.
    prices.to_sql("forward_returns_new", engine, if_exists="replace", index=False)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS forward_returns"))
        conn.execute(text("ALTER TABLE forward_returns_new RENAME TO forward_returns"))
