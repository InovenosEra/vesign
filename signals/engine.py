import pandas as pd
import yaml
import os
from data.loaders import engine
from sqlalchemy import text, inspect


def _ensure_signals_columns():
    new_cols = {
        "volume_sma_20":    "REAL",
        "volume_ratio":     "REAL",
        "week52_high":      "REAL",
        "pct_from_52w_high":"REAL",
        "volume_flag":      "INTEGER",
        "week52_condition": "INTEGER",
    }
    inspector = inspect(engine)
    if "signals" in inspector.get_table_names():
        with engine.begin() as conn:
            existing = pd.read_sql("PRAGMA table_info(signals)", conn)
            for col, dtype in new_cols.items():
                if col not in existing["name"].values:
                    conn.execute(text(f"ALTER TABLE signals ADD COLUMN {col} {dtype}"))


def _get_open_positions():
    """Return {ticker: entry_price} for tickers with an open BUY (no subsequent SELL)."""
    inspector = inspect(engine)
    if "signals" not in inspector.get_table_names():
        return {}

    sql = """
        WITH last_buy AS (
            SELECT ticker, MAX(date) AS buy_date
            FROM signals WHERE signal = 'BUY'
            GROUP BY ticker
        ),
        last_sell AS (
            SELECT ticker, MAX(date) AS sell_date
            FROM signals WHERE signal = 'SELL'
            GROUP BY ticker
        )
        SELECT s.ticker, s.close AS entry_price
        FROM signals s
        JOIN last_buy lb ON s.ticker = lb.ticker AND s.date = lb.buy_date
        LEFT JOIN last_sell ls ON s.ticker = ls.ticker
        WHERE ls.sell_date IS NULL OR ls.sell_date < lb.buy_date
    """
    try:
        df = pd.read_sql(sql, engine)
        return dict(zip(df["ticker"], df["entry_price"]))
    except Exception:
        return {}


def run_scoring():

    print("Running hybrid scoring engine...")

    # ---------- Load config ----------
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(BASE_DIR, "config", "settings.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    trailing_stop_pct = config.get("trailing_stop_pct", 0.07)

    # ---------- Schema migration ----------
    _ensure_signals_columns()

    # ---------- Load open positions for trailing stop ----------
    open_positions = _get_open_positions()

    # ---------- Load data ----------
    # Only the last 5 trading days are needed: the rolling conditions
    # (rsi_3day_flag, volume_flag) look back at most 3 rows. All other
    # indicator values (rsi, bb_*, volume_ratio, pct_from_52w_high) are
    # already stored in the features table so no additional history is needed.
    features = pd.read_sql(
        """
        SELECT * FROM features
        WHERE date >= (
            SELECT date FROM (
                SELECT DISTINCT date FROM features ORDER BY date DESC LIMIT 5
            ) ORDER BY date ASC LIMIT 1
        )
        ORDER BY ticker, date
        """,
        engine
    )

    analyst = pd.read_sql("SELECT * FROM analyst_expectations", engine)

    df = features.merge(analyst, on="ticker", how="left")

    # ---------- Analyst upside ----------
    df["fair_value_upside"] = (
        df["target_mean_price"] - df["close"]
    ) / df["close"]

    df["analyst_condition"] = df["fair_value_upside"] >= config.get("analyst_upside_min", 0.30)

    # ---------- Bollinger condition ----------
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

    # ---------- Volume confirmation ----------
    df["volume_flag"] = (
        df.groupby("ticker")["volume_ratio"]
        .rolling(3, min_periods=1)
        .max()
        .reset_index(level=0, drop=True)
        >= config.get("volume_ratio_threshold", 1.5)
    )

    # ---------- 52-week high distance ----------
    df["week52_condition"] = (
        df["pct_from_52w_high"] <= -config.get("pct_from_52w_high_min", 0.10)
    )

    # ---------- Filter to today before signal assignment ----------
    # Rolling windows already computed above using full history.
    # Signal assignment only touches today's rows — much faster.
    today = df["date"].max()
    today_df = df[df["date"] == today].copy()

    # ---------- Trailing stop (vectorized) ----------
    if open_positions:
        op_series = pd.Series(open_positions, name="entry_price")
        today_df = today_df.join(op_series, on="ticker")
        stop_hit = (
            today_df["entry_price"].notna()
            & (today_df["close"] < today_df["entry_price"] * (1 - trailing_stop_pct))
        )
        today_df.drop(columns=["entry_price"], inplace=True)
    else:
        stop_hit = pd.Series(False, index=today_df.index)

    # ---------- Vectorized signal logic ----------
    import numpy as np

    buy_cond = (
        (today_df["rsi_3day_flag"] == 3)
        & today_df["bb_condition"]
        & today_df["analyst_condition"]
        & today_df["volume_flag"]
        & today_df["week52_condition"]
    )
    sell_cond = stop_hit | (today_df["rsi"] >= 70)

    today_df["signal"] = np.select(
        [stop_hit, buy_cond, sell_cond],
        ["SELL", "BUY", "SELL"],
        default="HOLD"
    )

    # RSI-based fallback score (always defined, always positive for BUY signals)
    today_df["score"] = 50 - today_df["rsi"]

    # ---------- Merge ML prediction scores ----------
    if "predictions" in inspect(engine).get_table_names():
        predictions = pd.read_sql(
            "SELECT date, ticker, prediction_score FROM predictions",
            engine
        )
        today_df = today_df.merge(predictions, on=["date", "ticker"], how="left")
    else:
        today_df["prediction_score"] = float("nan")

    if "signals" in inspect(engine).get_table_names():
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM signals WHERE date = :date"),
                {"date": today}
            )

    today_df.to_sql("signals", engine, if_exists="append", index=False)

    print("Hybrid signals generated successfully")
