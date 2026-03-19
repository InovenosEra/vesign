"""
One-time backfill: generate historical signals for tickers that only have
today's signal row (i.e. newly added tickers like the S&P 400 batch).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import yaml
from sqlalchemy import text
from data.loaders import engine


def backfill_signals():

    # ---------- Config ----------
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(BASE_DIR, "config", "settings.yaml")
    with open(config_path) as f:
        config = yaml.safe_load(f)

    trailing_stop_pct    = config.get("trailing_stop_pct", 0.07)
    volume_threshold     = config.get("volume_ratio_threshold", 1.5)
    pct_52w_min          = config.get("pct_from_52w_high_min", 0.10)

    # ---------- Find tickers that need backfilling ----------
    counts = pd.read_sql(
        "SELECT ticker, COUNT(*) as cnt FROM signals GROUP BY ticker",
        engine
    )
    # Tickers with only 1 signal row are the newly added ones
    backfill_tickers = counts[counts["cnt"] == 1]["ticker"].tolist()

    if not backfill_tickers:
        print("Nothing to backfill — all tickers already have signal history.")
        return

    print(f"Backfilling {len(backfill_tickers)} tickers...")

    # ---------- Load all features for those tickers ----------
    placeholders = ",".join(f"'{t}'" for t in backfill_tickers)
    features = pd.read_sql(
        f"SELECT * FROM features WHERE ticker IN ({placeholders}) ORDER BY ticker, date",
        engine
    )
    print(f"Loaded {len(features):,} feature rows")

    # ---------- Merge analyst data (use current targets as best approximation) ----------
    analyst = pd.read_sql("SELECT * FROM analyst_expectations", engine)
    df = features.merge(analyst, on="ticker", how="left")

    # ---------- Compute all derived columns ----------
    df["fair_value_upside"] = (df["target_mean_price"] - df["close"]) / df["close"]
    df["analyst_condition"] = df["fair_value_upside"] >= 0.05

    df["bb_pct_b"] = (df["close"] - df["bb_low"]) / (df["bb_high"] - df["bb_low"])
    df["bb_condition"] = df["bb_pct_b"] < config.get("bb_pct_b_max", 0.1)

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    df["rsi_below_30"] = df["rsi"] < 30
    df["rsi_3day_flag"] = (
        df.groupby("ticker")["rsi_below_30"]
        .rolling(3, min_periods=3)
        .sum()
        .reset_index(level=0, drop=True)
    )

    df["volume_flag"] = (
        df.groupby("ticker")["volume_ratio"]
        .rolling(3, min_periods=1)
        .max()
        .reset_index(level=0, drop=True)
        >= volume_threshold
    )

    df["week52_condition"] = df["pct_from_52w_high"] <= -pct_52w_min

    # ---------- Signal logic (no trailing stop for historical backfill) ----------
    buy_cond = (
        (df["rsi_3day_flag"] == 3)
        & df["bb_condition"]
        & df["analyst_condition"]
        & df["volume_flag"]
        & df["week52_condition"]
    )
    sell_cond = df["rsi"] >= 70

    df["signal"] = np.select(
        [buy_cond, sell_cond],
        ["BUY", "SELL"],
        default="HOLD"
    )
    df["score"] = 50 - df["rsi"]
    df["prediction_score"] = float("nan")

    # ---------- Delete existing (today-only) signals and replace with full history ----------
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM signals WHERE ticker IN ({placeholders})"))

    df.to_sql("signals", engine, if_exists="append", index=False)

    buy_count  = (df["signal"] == "BUY").sum()
    sell_count = (df["signal"] == "SELL").sum()
    print(f"Inserted {len(df):,} signal rows — {buy_count:,} BUY, {sell_count:,} SELL, {len(df)-buy_count-sell_count:,} HOLD")
    print("Backfill complete.")


if __name__ == "__main__":
    backfill_signals()
