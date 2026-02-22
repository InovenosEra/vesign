import pandas as pd
import yaml
import os
from data.loaders import engine
from sqlalchemy import text, inspect


def run_scoring():

    print("Running hybrid scoring engine...")

    # ---------- Load config ----------
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(BASE_DIR, "config", "settings.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # ---------- Load data ----------
    features = pd.read_sql(
        "SELECT * FROM features ORDER BY ticker, date",
        engine
    )

    analyst = pd.read_sql("SELECT * FROM analyst_expectations", engine)

    df = features.merge(analyst, on="ticker", how="left")

    # ---------- Analyst upside ----------
    df["fair_value_upside"] = (
        df["target_mean_price"] - df["close"]
    ) / df["close"]

    df["analyst_condition"] = df["fair_value_upside"] >= 0.05

    # ---------- Bollinger condition ----------
    # BB %B: where the closing price sits within the band range.
    #   0 = at the lower band, 0.5 = middle, 1 = upper band.
    # For a BUY signal we want the price in the bottom 20% of the range,
    # confirming the stock is genuinely oversold relative to its own volatility.
    # The old check (bb_low / bb_high > 0.8) measured band WIDTH, which is
    # inversely correlated with sell-offs and was always blocking BUY signals.
    df["bb_pct_b"] = (df["close"] - df["bb_low"]) / (df["bb_high"] - df["bb_low"])
    df["bb_condition"] = df["bb_pct_b"] < 0.2

    # ensure strict ordering for rolling windows
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # ---------- RSI consecutive condition ----------
    df["rsi_below_30"] = df["rsi"] < 30

    df["rsi_3day_flag"] = (
        df.groupby("ticker")["rsi_below_30"]
        .rolling(3, min_periods=3)
        .sum()
        .reset_index(level=0, drop=True)
    )

    # ---------- Signal logic ----------
    def assign_signal(row):

        if (
            row["rsi_3day_flag"] == 3
            and row["bb_condition"]
            and row["analyst_condition"]
        ):
            return "BUY"

        elif row["rsi"] >= 70:
            return "SELL"

        elif 30 <= row["rsi"] < 70:
            return "HOLD"

        else:
            return "HOLD"

    df["signal"] = df.apply(assign_signal, axis=1)

    # RSI-based fallback score (always defined, always positive for BUY signals)
    df["score"] = 50 - df["rsi"]

    # ---------- Merge ML prediction scores ----------
    # predictions table is populated by run_prediction_engine() earlier in
    # the daily pipeline, so it is available here. Left-join so the table
    # degrades gracefully if the model hasn't been trained yet.
    if "predictions" in inspect(engine).get_table_names():
        predictions = pd.read_sql(
            "SELECT date, ticker, prediction_score FROM predictions",
            engine
        )
        df = df.merge(predictions, on=["date", "ticker"], how="left")
    else:
        df["prediction_score"] = float("nan")

    # ---------- Write to DB ----------
    today = df["date"].max()

    inspector = inspect(engine)

    if "signals" in inspector.get_table_names():

        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM signals WHERE date = :date "),
                {"date": today}
            )

    df.to_sql("signals", engine, if_exists="append", index=False)

    print("Hybrid signals generated successfully")
