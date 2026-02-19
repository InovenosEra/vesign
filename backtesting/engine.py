import pandas as pd
from data.loaders import engine


def build_trade_log():

    print("Building trade log...")

    signals = pd.read_sql(
        "SELECT date, ticker, signal, close FROM signals",
        engine
    )

    signals = signals.sort_values(["ticker", "date"])

    trades = []

    for ticker, df in signals.groupby("ticker"):

        open_trade = None

        for _, row in df.iterrows():

            if row["signal"] == "BUY" and open_trade is None:
                open_trade = {
                    "ticker": ticker,
                    "buy_date": row["date"],
                    "buy_price": row["close"]
                }

            elif row["signal"] == "SELL" and open_trade is not None:

                trade = {
                    **open_trade,
                    "sell_date": row["date"],
                    "sell_price": row["close"],
                    "return_pct":
                        (row["close"] - open_trade["buy_price"])
                        / open_trade["buy_price"]
                }

                trades.append(trade)
                open_trade = None

    trades_df = pd.DataFrame(trades)

    trades_df.to_sql("trade_log", engine,
                     if_exists="replace",
                     index=False)

    print("Trade log created")


def run_backtest():

    print("Running backtest...")

    prices = pd.read_sql("SELECT date, ticker, close FROM daily_prices", engine)
    signals = pd.read_sql("SELECT date, ticker, signal FROM signals", engine)

    merged = signals.merge(prices, on=["date", "ticker"])
    merged = merged.sort_values(["ticker", "date"])

    merged["next_close"] = merged.groupby("ticker")["close"].shift(-1)

    merged["return"] = (
        (merged["next_close"] - merged["close"])
        / merged["close"]
    )

    merged["strategy_return"] = merged.apply(
        lambda x: x["return"] if x["signal"] == "BUY" else 0,
        axis=1
    )

    results = merged["strategy_return"].mean()

    print(f"Average daily strategy return: {results:.6f}")

    merged.to_sql("backtest_results",
                  engine,
                  if_exists="replace",
                  index=False)

    print("Backtest completed")
