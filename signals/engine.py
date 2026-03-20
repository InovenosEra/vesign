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
        "health_score":     "REAL",
        "health_condition": "INTEGER",
        "prediction_score": "REAL",
        "ml_condition":     "INTEGER",
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
        WITH last_sell AS (
            SELECT ticker, MAX(date) AS sell_date
            FROM signals WHERE signal = 'SELL'
            GROUP BY ticker
        ),
        first_open_buy AS (
            SELECT b.ticker, MIN(b.date) AS buy_date
            FROM signals b
            LEFT JOIN last_sell ls ON b.ticker = ls.ticker
            WHERE b.signal = 'BUY'
            AND (ls.sell_date IS NULL OR b.date > ls.sell_date)
            GROUP BY b.ticker
        )
        SELECT s.ticker, s.close AS entry_price
        FROM signals s
        JOIN first_open_buy fob ON s.ticker = fob.ticker AND s.date = fob.buy_date
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

    analyst  = pd.read_sql("SELECT * FROM analyst_expectations", engine)
    health   = pd.read_sql("SELECT ticker, score AS health_score FROM company_health", engine)

    df = features.merge(analyst, on="ticker", how="left")
    df = df.merge(health, on="ticker", how="left")

    # ---------- ML prediction scores (loaded early — used as BUY gate) ----------
    if "predictions" in inspect(engine).get_table_names():
        predictions = pd.read_sql(
            "SELECT date, ticker, prediction_score FROM predictions", engine
        )
        df = df.merge(predictions, on=["date", "ticker"], how="left")
    else:
        df["prediction_score"] = float("nan")

    # ---------- Analyst upside ----------
    df["fair_value_upside"] = (
        df["target_mean_price"] - df["close"]
    ) / df["close"]

    # IL stocks with no analyst target pass through (rule waived — no free source covers them)
    il_no_target = df["ticker"].str.endswith(".TA") & df["target_mean_price"].isna()
    df["analyst_condition"] = (
        (df["fair_value_upside"] >= config.get("analyst_upside_min", 0.30)) | il_no_target
    )

    # ---------- Bollinger condition ----------
    df["bb_pct_b"] = (df["close"] - df["bb_low"]) / (df["bb_high"] - df["bb_low"])
    df["bb_condition"] = df["bb_pct_b"] < config.get("bb_pct_b_max", 0.1)

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

    # ---------- Health score gate ----------
    # Pass through if no score (new/unscored ticker), waived for IL (already have scores but safety net)
    health_min = config.get("health_score_min", 3)
    df["health_condition"] = (
        (df["health_score"] >= health_min) | df["health_score"].isna()
    )

    # ---------- ML score gate ----------
    # Waived for IL tickers (no ML model for TASE) and NULL scores (new tickers)
    ml_min = config.get("ml_score_min", 0.05)
    df["ml_condition"] = (
        (df["prediction_score"] >= ml_min)
        | df["ticker"].str.endswith(".TA")
        | df["prediction_score"].isna()
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
        & today_df["health_condition"]
        & today_df["ml_condition"]
    )

    # Suppress BUY if already in an open position (first BUY wins until SELL appears)
    if open_positions:
        already_open = today_df["ticker"].isin(open_positions.keys())
        buy_cond = buy_cond & ~already_open

    sell_cond = stop_hit | (today_df["rsi"] >= 70)

    today_df["signal"] = np.select(
        [stop_hit, buy_cond, sell_cond],
        ["SELL", "BUY", "SELL"],
        default="HOLD"
    )

    # RSI-based fallback score (always defined, always positive for BUY signals)
    today_df["score"] = 50 - today_df["rsi"]

    if "signals" in inspect(engine).get_table_names():
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM signals WHERE date = :date"),
                {"date": today}
            )

    today_df.to_sql("signals", engine, if_exists="append", index=False)

    print("Hybrid signals generated successfully")
