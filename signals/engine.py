import math
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
        "vesign_score":     "REAL",
    }
    inspector = inspect(engine)
    if "signals" in inspector.get_table_names():
        with engine.begin() as conn:
            existing = pd.read_sql("PRAGMA table_info(signals)", conn)
            for col, dtype in new_cols.items():
                if col not in existing["name"].values:
                    conn.execute(text(f"ALTER TABLE signals ADD COLUMN {col} {dtype}"))


def _get_open_positions(as_of_date=None):
    """Return {ticker: entry_price} for tickers with an open BUY (no subsequent SELL).

    If as_of_date is provided, only considers signals strictly before that date.
    """
    inspector = inspect(engine)
    if "signals" not in inspector.get_table_names():
        return {}

    if as_of_date:
        sql = """
            WITH last_sell AS (
                SELECT ticker, MAX(date) AS sell_date
                FROM signals WHERE signal = 'SELL' AND date < :dt
                GROUP BY ticker
            ),
            first_open_buy AS (
                SELECT b.ticker, MIN(b.date) AS buy_date
                FROM signals b
                LEFT JOIN last_sell ls ON b.ticker = ls.ticker
                WHERE b.signal = 'BUY' AND b.date < :dt
                AND (ls.sell_date IS NULL OR b.date > ls.sell_date)
                GROUP BY b.ticker
            )
            SELECT s.ticker, s.close AS entry_price, fob.buy_date
            FROM signals s
            JOIN first_open_buy fob ON s.ticker = fob.ticker AND s.date = fob.buy_date
        """
        params = {"dt": str(as_of_date)}
    else:
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
            SELECT s.ticker, s.close AS entry_price, fob.buy_date
            FROM signals s
            JOIN first_open_buy fob ON s.ticker = fob.ticker AND s.date = fob.buy_date
        """
        params = {}
    try:
        df = pd.read_sql(sql, engine, params=params)
        # Returns {ticker: (entry_price, buy_date)} — buy_date is ISO string
        return {r["ticker"]: (r["entry_price"], r["buy_date"]) for _, r in df.iterrows()}
    except Exception:
        return {}


def _isna(v):
    """Return True for None or float NaN."""
    if v is None:
        return True
    try:
        return math.isnan(float(v))
    except (TypeError, ValueError):
        return False


def _compute_vesign_score(row):
    """Compute a 0–100 Vesign Score showing proximity to a BUY signal.

    Works on both a pandas Series (from apply) and a plain dict.
    """
    score = 0

    # --- RSI momentum (0–30) ---
    flag = row.get("rsi_3day_flag") if isinstance(row, dict) else row["rsi_3day_flag"]
    if _isna(flag):
        flag = 0
    else:
        flag = int(flag)
    rsi = row.get("rsi") if isinstance(row, dict) else row["rsi"]

    if flag == 3 and not _isna(rsi) and float(rsi) < 25:
        score += 30
    elif flag == 3:
        score += 27
    elif flag == 2:
        score += 18
    elif flag == 1:
        score += 10
    elif not _isna(rsi) and float(rsi) < 33:
        score += 5
    elif not _isna(rsi) and float(rsi) < 37:
        score += 3

    # --- Bollinger Band (0–20) ---
    bb = row.get("bb_pct_b") if isinstance(row, dict) else row["bb_pct_b"]
    if _isna(bb):
        score += 10
    elif float(bb) < 0:
        score += 20
    elif float(bb) < 0.05:
        score += 18
    elif float(bb) < 0.10:
        score += 15
    elif float(bb) < 0.20:
        score += 10
    elif float(bb) < 0.30:
        score += 5

    # --- ML prediction (0–20) ---
    ml = row.get("prediction_score") if isinstance(row, dict) else row["prediction_score"]
    if _isna(ml):
        score += 10
    elif float(ml) >= 0.10:
        score += 20
    elif float(ml) >= 0.05:
        score += 16
    elif float(ml) >= 0.03:
        score += 10
    elif float(ml) >= 0.02:
        score += 6
    elif float(ml) >= 0.01:
        score += 3

    # --- Analyst upside (0–15) ---
    ticker = row.get("ticker") if isinstance(row, dict) else row["ticker"]
    is_tase = isinstance(ticker, str) and ticker.endswith(".TA")
    target = row.get("target_mean_price") if isinstance(row, dict) else row["target_mean_price"]
    upside = row.get("fair_value_upside") if isinstance(row, dict) else row["fair_value_upside"]
    if is_tase or _isna(target):
        score += 8
    elif _isna(upside):
        score += 8
    elif float(upside) >= 0.60:
        score += 15
    elif float(upside) >= 0.30:
        score += 12
    elif float(upside) >= 0.15:
        score += 7
    elif float(upside) >= 0.0:
        score += 3

    # --- Volume (0–5) ---
    vf = row.get("volume_flag") if isinstance(row, dict) else row["volume_flag"]
    if not _isna(vf) and vf:
        score += 5

    # --- 52-week position (0–5) ---
    w52 = row.get("week52_condition") if isinstance(row, dict) else row["week52_condition"]
    if _isna(w52) or w52:
        score += 5

    # --- Health (0–5) ---
    health = row.get("health_score") if isinstance(row, dict) else row["health_score"]
    if _isna(health):
        score += 3
    elif float(health) >= 5:
        score += 5
    elif float(health) >= 4:
        score += 4
    elif float(health) >= 3:
        score += 3

    return int(score)


def run_scoring(target_date=None):
    """Run signal scoring.

    target_date: if provided (str 'YYYY-MM-DD'), score that specific date instead of
                 the latest date in the features table. Used for backfilling missing dates.
    """
    if target_date:
        print(f"Running hybrid scoring engine for {target_date}...")
    else:
        print("Running hybrid scoring engine...")

    # ---------- Load config ----------
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(BASE_DIR, "config", "settings.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    trailing_stop_pct = config.get("trailing_stop_pct", 0.10)

    # ---------- Schema migration ----------
    _ensure_signals_columns()

    # ---------- Load open positions for trailing stop ----------
    # For backfill: only consider positions opened before target_date
    open_positions = _get_open_positions(as_of_date=target_date)

    # ---------- Load data ----------
    # Only the last 5 trading days are needed: the rolling conditions
    # (rsi_3day_flag, volume_flag) look back at most 3 rows. All other
    # indicator values (rsi, bb_*, volume_ratio, pct_from_52w_high) are
    # already stored in the features table so no additional history is needed.
    if target_date:
        # Dates are stored as 'YYYY-MM-DD HH:MM:SS.ffffff' strings in SQLite,
        # so 'YYYY-MM-DD' < 'YYYY-MM-DD 00:00:00'. Use next-day boundary to include the target.
        next_day = (pd.Timestamp(target_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        inner_filter = f"WHERE date < '{next_day}'"
        outer_filter = f"AND date < '{next_day}'"
    else:
        inner_filter = ""
        outer_filter = ""

    features = pd.read_sql(
        f"""
        SELECT * FROM features
        WHERE date >= (
            SELECT date FROM (
                SELECT DISTINCT date FROM features {inner_filter} ORDER BY date DESC LIMIT 5
            ) ORDER BY date ASC LIMIT 1
        ) {outer_filter}
        ORDER BY ticker, date
        """,
        engine
    )

    # For historical re-runs (target_date set), use the most recent analyst snapshot
    # on or before that date so signals reflect what was current then, not today.
    # For today's live run (target_date=None), always use current analyst_expectations.
    if target_date:
        analyst = pd.read_sql(
            """
            SELECT ticker, target_mean_price, target_high_price, target_low_price,
                   number_of_analysts
            FROM analyst_targets_history h1
            WHERE date = (
                SELECT MAX(date) FROM analyst_targets_history h2
                WHERE h2.ticker = h1.ticker AND h2.date <= :td
            )
            """,
            engine,
            params={"td": target_date},
        )
        if analyst.empty:
            analyst = pd.read_sql("SELECT * FROM analyst_expectations", engine)
    else:
        analyst = pd.read_sql("SELECT * FROM analyst_expectations", engine)

    if target_date:
        health = pd.read_sql(
            """
            SELECT ticker, score AS health_score
            FROM company_health_history h1
            WHERE recorded_at = (
                SELECT MAX(recorded_at) FROM company_health_history h2
                WHERE h2.ticker = h1.ticker AND DATE(h2.recorded_at) <= :td
            )
            """,
            engine,
            params={"td": target_date},
        )
        if health.empty:
            health = pd.read_sql("SELECT ticker, score AS health_score FROM company_health", engine)
    else:
        health = pd.read_sql("SELECT ticker, score AS health_score FROM company_health", engine)

    df = features.merge(analyst, on="ticker", how="left")
    df = df.merge(health, on="ticker", how="left")

    # ---------- ML prediction scores (loaded early — used as BUY gate) ----------
    # Filter to the date range of df — loading the full 2.3M-row table per call
    # made backfill take ~44s/date (predictions is ~50% of total runtime).
    if "predictions" in inspect(engine).get_table_names():
        if not df.empty:
            date_min = df["date"].min()
            date_max = df["date"].max()
            predictions = pd.read_sql(
                text(
                    "SELECT date, ticker, prediction_score FROM predictions "
                    "WHERE date >= :dmin AND date <= :dmax"
                ),
                engine, params={"dmin": str(date_min), "dmax": str(date_max)},
            )
        else:
            predictions = pd.DataFrame(columns=["date", "ticker", "prediction_score"])
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
    bb_range = df["bb_high"] - df["bb_low"]
    df["bb_pct_b"] = (df["close"] - df["bb_low"]) / bb_range.where(bb_range != 0, other=float("nan"))
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
        >= config.get("volume_ratio_threshold", 1.2)
    )

    # ---------- 52-week high distance ----------
    # Pass through if no data (NULL) — same pattern as health/ML conditions.
    df["week52_condition"] = (
        (df["pct_from_52w_high"] <= -config.get("pct_from_52w_high_min", 0.10))
        | df["pct_from_52w_high"].isna()
    )

    # ---------- Health score gate ----------
    # Pass through if no score. IL uses a lower threshold (Israeli market norms differ from US).
    health_min    = config.get("health_score_min",    3)
    health_min_il = config.get("health_score_min_il", 2)
    df["health_condition"] = (
        (df["ticker"].str.endswith(".TA")  & ((df["health_score"] >= health_min_il) | df["health_score"].isna()))
        | (~df["ticker"].str.endswith(".TA") & ((df["health_score"] >= health_min)    | df["health_score"].isna()))
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
    today = df["date"].max()  # always use the actual max date in the loaded slice
    today_df = df[df["date"] == today].copy()

    # ---------- Trailing stop (vectorized) ----------
    import numpy as np

    if open_positions:
        # open_positions: {ticker: (entry_price, buy_date)}
        entry_prices = {t: v[0] for t, v in open_positions.items()}
        buy_dates    = {t: v[1] for t, v in open_positions.items()}
        today_df["entry_price"] = today_df["ticker"].map(entry_prices)
        today_df["buy_date"]    = today_df["ticker"].map(buy_dates)
        stop_hit = (
            today_df["entry_price"].notna()
            & (today_df["close"] < today_df["entry_price"] * (1 - trailing_stop_pct))
        )
        # 365-day time-based exit — hard cap regardless of yield (bypasses ML gate)
        today_ts = pd.to_datetime(today_df["date"].max())
        days_held = (today_ts - pd.to_datetime(today_df["buy_date"])).dt.days
        time_exit = today_df["entry_price"].notna() & (days_held >= 365)
        today_df.drop(columns=["entry_price", "buy_date"], inplace=True)
    else:
        stop_hit  = pd.Series(False, index=today_df.index)
        time_exit = pd.Series(False, index=today_df.index)

    # ---------- Vectorized signal logic ----------
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

    # MASTER gate: SELL requires ML prediction to be negative (or waived: NULL / TASE).
    # Applies to trailing stop and RSI>=70. Time-based exit (365 days profitable)
    # bypasses the ML gate — it's an unconditional rule.
    ml_negative = (
        (today_df["prediction_score"] < 0)
        | today_df["prediction_score"].isna()
        | today_df["ticker"].str.endswith(".TA")
    )
    sell_cond = ((stop_hit | (today_df["rsi"] >= 70)) & ml_negative) | time_exit

    today_df["signal"] = np.select(
        [sell_cond, buy_cond],
        ["SELL", "BUY"],
        default="HOLD"
    )

    # RSI-based fallback score (always defined, always positive for BUY signals)
    today_df["score"] = 50 - today_df["rsi"]

    today_df["vesign_score"] = today_df.apply(_compute_vesign_score, axis=1)

    if "signals" in inspect(engine).get_table_names():
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM signals WHERE date = :date"),
                {"date": str(today)}
            )

    today_df.to_sql("signals", engine, if_exists="append", index=False)

    print("Hybrid signals generated successfully")
