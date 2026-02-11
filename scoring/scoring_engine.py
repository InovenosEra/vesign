import pandas as pd
import yaml
import os
from database.db_connection import engine


def run_scoring():

    print("Running hybrid scoring engine...")

    # ---------- Load config ----------
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(BASE_DIR, "config", "settings.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # buy_threshold = config["scoring"]["buy_threshold"]
    # sell_threshold = config["scoring"]["sell_threshold"]

    # ---------- Load data ----------
    features = pd.read_sql("SELECT * FROM features", engine)

    features = pd.read_sql("SELECT * FROM features", engine)
    analyst = pd.read_sql("SELECT * FROM analyst_expectations", engine)

    df = features.merge(analyst, on="ticker", how="left")

    df["fair_value_upside"] = (
                                      df["target_mean_price"] - df["close"]
                              ) / df["close"]

    df["analyst_condition"] = df["fair_value_upside"] >= 0.10

    # preds = pd.read_sql("SELECT * FROM predictions", engine)

    # df = features.merge(preds, on=["date", "ticker"], how="left")


    df["bb_ratio"] = df["bb_low"] / df["bb_high"]
    df["bb_condition"] = df["bb_ratio"] > 0.75

    df = df.sort_values(["ticker", "date"])

    # ---------- RSI consecutive condition ----------
    df["rsi_below_30"] = df["rsi"] < 30

    df["rsi_3day_flag"] = (
        df.groupby("ticker")["rsi_below_30"]
        .rolling(3)
        .sum()
        .reset_index(level=0, drop=True)
    )

    # ---------- Regime condition ----------
    # df["regime_pass"] = df["rsi"] < 40   # flexible regime entry filter

    # ---------- Signal logic ----------
    # def assign_signal(row):
    #
    #     if (
    #         row["regime_pass"]
    #         and row["prediction_score"] > buy_threshold
    #         and row["bb_condition"]
    #     ):
    #         return "BUY"
    #
    #     elif row["rsi"] > 70 or row["prediction_score"] < -sell_threshold:
    #         return "SELL"
    #
    #     else:
    #         return "HOLD"

    def assign_signal(row):

        # BUY: RSI<30 for 3 days AND Bollinger compression
        if (
            row["rsi_3day_flag"] == 3
            and row["bb_condition"]
            and row["analyst_condition"]
        ):
            return "BUY"

        # SELL: RSI >= 70
        elif row["rsi"] >= 70:
            return "SELL"

        # HOLD: RSI between 30–70
        elif 30 <= row["rsi"] < 70:
            return "HOLD"

        else:
            return "HOLD"

    df["signal"] = df.apply(assign_signal, axis=1)

    # ranking score driven by prediction
    # df["score"] = df["prediction_score"]

    df["score"] = 50 - df["rsi"]
    df.to_sql("signals", engine, if_exists="replace", index=False)

    print("Hybrid signals generated successfully")
