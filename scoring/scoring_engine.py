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

    buy_threshold = config["scoring"]["buy_threshold"]
    sell_threshold = config["scoring"]["sell_threshold"]

    # ---------- Load data ----------
    features = pd.read_sql("SELECT * FROM features", engine)
    preds = pd.read_sql("SELECT * FROM predictions", engine)

    df = features.merge(preds, on=["date", "ticker"], how="left")

    df = df.sort_values(["ticker", "date"])

    # ---------- Regime condition ----------
    df["regime_pass"] = df["rsi"] < 40   # flexible regime entry filter

    # ---------- Signal logic ----------
    def assign_signal(row):

        if row["regime_pass"] and row["prediction_score"] > buy_threshold:
            return "BUY"

        elif row["rsi"] > 70 or row["prediction_score"] < -sell_threshold:
            return "SELL"

        else:
            return "HOLD"

    df["signal"] = df.apply(assign_signal, axis=1)

    # ranking score driven by prediction
    df["score"] = df["prediction_score"]

    df.to_sql("signals", engine, if_exists="replace", index=False)

    print("Hybrid signals generated successfully")
