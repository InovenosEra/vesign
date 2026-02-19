import pandas as pd
from datetime import datetime, timedelta
from data.loaders import engine


def compute_signal_success_rate():

    print("Computing BUY→SELL success rate (12 months)...")

    signals = pd.read_sql("SELECT date, ticker, signal, close FROM signals", engine)

    signals["date"] = pd.to_datetime(signals["date"])

    cutoff = datetime.today() - timedelta(days=365)
    signals = signals[signals["date"] >= cutoff]

    signals = signals.sort_values(["ticker", "date"])

    trades = []

    for ticker, df in signals.groupby("ticker"):

        open_trade = None

        for _, row in df.iterrows():

            if row["signal"] == "BUY" and open_trade is None:
                open_trade = row

            elif row["signal"] == "SELL" and open_trade is not None:

                ret = (row["close"] - open_trade["close"]) / open_trade["close"]

                trades.append({
                    "ticker": ticker,
                    "buy_date": open_trade["date"],
                    "sell_date": row["date"],
                    "return": ret
                })

                open_trade = None

    trades_df = pd.DataFrame(trades)

    if trades_df.empty:
        print("No completed trades found")
        return

    success_rate = (trades_df["return"] > 0).mean()

    avg_return = trades_df["return"].mean()

    summary = pd.DataFrame([{
        "success_rate": success_rate,
        "avg_return": avg_return,
        "num_trades": len(trades_df)
    }])

    summary.to_sql("signal_success_metrics",
                   engine,
                   if_exists="replace",
                   index=False)

    trades_df.to_sql("signal_trades",
                     engine,
                     if_exists="replace",
                     index=False)

    # ---------- Company level summary ----------
    company_summary = (
        trades_df.groupby("ticker")
        .agg(
            trades=("return", "count"),
            success_rate=("return", lambda x: (x > 0).mean()),
            avg_return=("return", "mean")
        )
        .reset_index()
    )

    company_summary.to_sql(
        "signal_success_by_company",
        engine,
        if_exists="replace",
        index=False
    )

    print(f"Success rate: {success_rate:.2%}")
    print(f"Average return: {avg_return:.2%}")
    print(f"Trades analyzed: {len(trades_df)}")
