import os
import yaml
import pandas as pd
from data.loaders import engine


def build_trade_log():

    print("Building trade log...")

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(BASE_DIR, "config", "settings.yaml")) as f:
        config = yaml.safe_load(f)
    stop_pct = config.get("trailing_stop_pct", 0.15)

    # Merge signals + ML prediction into daily_prices so every day is iterated.
    # SELL (stop or RSI>=70) now requires ML prediction_score < 0 (MASTER gate).
    prices = pd.read_sql(
        "SELECT date, ticker, close FROM daily_prices",
        engine,
    )
    signals = pd.read_sql(
        "SELECT date, ticker, signal, prediction_score FROM signals",
        engine,
    )
    merged = prices.merge(signals, on=["date", "ticker"], how="left")
    merged = merged.sort_values(["ticker", "date"])

    def _ml_allows_sell(pred, ticker):
        # Waived when prediction is missing (new ticker without enough history).
        if pred is None or pd.isna(pred):
            return True
        return pred < 0

    trades = []
    stop_sells = []  # (ticker, sell_date) — stop-triggered exits to write back to signals
    time_sells = []  # (ticker, sell_date) — 365-day time-based exits to write back

    for ticker, df in merged.groupby("ticker"):

        open_trade = None
        stop_price = None

        for _, row in df.iterrows():

            close = row["close"]
            sig = row["signal"]
            pred = row.get("prediction_score")

            if open_trade is None:
                if sig == "BUY":
                    open_trade = {
                        "ticker": ticker,
                        "buy_date": row["date"],
                        "buy_price": close,
                    }
                    stop_price = close * (1 - stop_pct)
            else:
                stop_hit = close <= stop_price
                rsi_sell = sig == "SELL"
                ml_ok = _ml_allows_sell(pred, ticker)

                # 365-day time-based exit — hard cap regardless of yield (bypasses ML gate)
                days_held = (pd.to_datetime(row["date"]) - pd.to_datetime(open_trade["buy_date"])).days
                time_exit = days_held >= 365

                fires = ((stop_hit or rsi_sell) and ml_ok) or time_exit

                if fires:
                    trade = {
                        **open_trade,
                        "sell_date": row["date"],
                        "sell_price": close,
                        "return_pct": (close - open_trade["buy_price"]) / open_trade["buy_price"],
                    }
                    trades.append(trade)
                    # Tag the exit type for back-writing signal markers.
                    # Priority: stop > rsi > time (if signal=SELL already, don't need back-write)
                    if stop_hit and not rsi_sell:
                        stop_sells.append((ticker, row["date"]))
                    elif time_exit and not rsi_sell and not stop_hit:
                        time_sells.append((ticker, row["date"]))
                    open_trade = None
                    stop_price = None

    trades_df = pd.DataFrame(trades)

    # Write stop-triggered + time-based SELL markers back to signals so they show in UI
    from sqlalchemy import text as _text
    if stop_sells or time_sells:
        with engine.begin() as conn:
            for ticker, sell_date in stop_sells + time_sells:
                conn.execute(_text("""
                    UPDATE signals
                    SET signal = 'SELL'
                    WHERE ticker = :t AND date = :d AND signal != 'BUY'
                """), {"t": ticker, "d": sell_date})
        if stop_sells:
            print(f"Back-wrote {len(stop_sells):,} stop-triggered SELL markers")
        if time_sells:
            print(f"Back-wrote {len(time_sells):,} 365-day time-exit SELL markers")

    if trades_df.empty:
        # Ensure table exists with a proper schema even when no closed trades yet
        # (fresh DB with too little history for BUY→SELL pairs to form).
        from sqlalchemy import text as _text
        with engine.begin() as conn:
            conn.execute(_text("""
                CREATE TABLE IF NOT EXISTS trade_log (
                    ticker     TEXT,
                    buy_date   TEXT,
                    buy_price  REAL,
                    sell_date  TEXT,
                    sell_price REAL,
                    return_pct REAL
                )
            """))
        print("Trade log: 0 closed trades (table ensured)")
        return

    trades_df.to_sql("trade_log", engine,
                     if_exists="replace",
                     index=False)

    print(f"Trade log created: {len(trades_df):,} closed trades")


def run_backtest(eval_start_date=None):
    """
    Evaluate strategy returns on historical signals.

    Parameters
    ----------
    eval_start_date : str or None
        Only evaluate signals on or after this date (format "YYYY-MM-DD").
        Pass this in the research pipeline so the backtest covers only the
        out-of-sample period — dates the model was never trained on.
        Defaults to None, which evaluates all available signals.
    """

    print("Running backtest...")

    prices = pd.read_sql("SELECT date, ticker, close FROM daily_prices", engine)
    signals = pd.read_sql("SELECT date, ticker, signal FROM signals", engine)

    merged = signals.merge(prices, on=["date", "ticker"])
    merged = merged.sort_values(["ticker", "date"])

    if eval_start_date is not None:
        eval_start_date = pd.Timestamp(eval_start_date)
        merged = merged[merged["date"] >= eval_start_date]
        print(f"Evaluating signals from {eval_start_date.date()} onwards (out-of-sample)")

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

    print("Backtest completed")
