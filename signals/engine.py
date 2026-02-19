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
    df["bb_ratio"] = df["bb_low"] / df["bb_high"]
    df["bb_condition"] = df["bb_ratio"] > 0.8

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

    # ranking score
    df["score"] = 50 - df["rsi"]

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
