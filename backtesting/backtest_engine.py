import pandas as pd
from data.loaders import engine


def run_backtest():

    print("Running backtest...")

    prices = pd.read_sql("SELECT date, ticker, close FROM daily_prices", engine)
    signals = pd.read_sql("SELECT date, ticker, signal FROM signals", engine)

    merged = signals.merge(prices, on=["date", "ticker"])
    merged = merged.sort_values(["ticker", "date"])

    merged["next_close"] = merged.groupby("ticker")["close"].shift(-1)

    merged["return"] = (merged["next_close"] - merged["close"]) / merged["close"]

    # Strategy return only when BUY
    merged["strategy_return"] = merged.apply(
        lambda x: x["return"] if x["signal"] == "BUY" else 0,
        axis=1
    )

    results = merged["strategy_return"].mean()

    print(f"Average daily strategy return: {results:.6f}")

    merged.to_sql("backtest_results", engine, if_exists="replace", index=False)

    print("Backtest completed")
