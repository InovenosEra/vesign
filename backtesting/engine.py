import os
import yaml
import pandas as pd
from data.loaders import engine


def build_trade_log():
    """V2 trade log: pair each BUY signal with the next SELL signal for the
    same ticker. SELL fires either via signals.engine (RSI>=70 AND profitable)
    or via 175-calendar-day expiration. No trailing stop in V2.
    """
    print("Building trade log (V2 — no trailing stop, 175-day cap)...")

    prices = pd.read_sql("SELECT date, ticker, close FROM daily_prices", engine)
    signals = pd.read_sql("SELECT date, ticker, signal FROM signals", engine)
    merged = prices.merge(signals, on=["date", "ticker"], how="left")
    merged = merged.sort_values(["ticker", "date"])

    trades = []
    time_sells = []  # (ticker, sell_date) — back-write SELL marker for time exits

    for ticker, df in merged.groupby("ticker"):
        open_trade = None
        for _, row in df.iterrows():
            close = row["close"]
            sig = row["signal"]

            if open_trade is None:
                if sig == "BUY":
                    open_trade = {
                        "ticker": ticker,
                        "buy_date": row["date"],
                        "buy_price": close,
                    }
            else:
                # V2 exit: signal=='SELL' (which signals.engine emits when RSI>=70
                # AND profitable) OR 175 calendar days held (~120 trading days).
                days_held = (
                    pd.to_datetime(row["date"]) - pd.to_datetime(open_trade["buy_date"])
                ).days
                rsi_sell = sig == "SELL"
                time_exit = days_held >= 175

                if rsi_sell or time_exit:
                    trades.append({
                        **open_trade,
                        "sell_date": row["date"],
                        "sell_price": close,
                        "return_pct": (close - open_trade["buy_price"]) / open_trade["buy_price"],
                    })
                    if time_exit and not rsi_sell:
                        time_sells.append((ticker, row["date"]))
                    open_trade = None

    trades_df = pd.DataFrame(trades)

    from sqlalchemy import text as _text
    if time_sells:
        with engine.begin() as conn:
            for ticker, sell_date in time_sells:
                conn.execute(_text("""
                    UPDATE signals SET signal = 'SELL'
                    WHERE ticker = :t AND date = :d AND signal != 'BUY'
                """), {"t": ticker, "d": sell_date})
        print(f"Back-wrote {len(time_sells):,} 175-day time-exit SELL markers")

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
