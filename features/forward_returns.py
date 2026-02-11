import pandas as pd
from database.db_connection import engine


def compute_forward_returns():

    prices = pd.read_sql("SELECT date,ticker,close FROM daily_prices", engine)

    prices = prices.sort_values(["ticker", "date"])

    prices["fwd_5d"] = (
        prices.groupby("ticker")["close"].shift(-5) / prices["close"] - 1
    )

    prices["fwd_20d"] = (
        prices.groupby("ticker")["close"].shift(-20) / prices["close"] - 1
    )

    prices.to_sql("forward_returns", engine, if_exists="replace", index=False)
