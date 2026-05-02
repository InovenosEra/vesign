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
        "news_block_reason":"TEXT",
        "vqs":              "INTEGER",
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


def _compute_vqs(row):
    """Vesign Quality Score (V2). Sum of 9 binary contrarian conditions, 0..9.

    Treats missing values as the condition not firing (no leakage, no errors).
    """
    def _get(field):
        if isinstance(row, dict):
            return row.get(field)
        try:
            return row[field]
        except (KeyError, IndexError):
            return None

    def _ge(field, threshold):
        v = _get(field)
        if _isna(v):
            return False
        return float(v) > threshold

    def _le(field, threshold):
        v = _get(field)
        if _isna(v):
            return False
        return float(v) < threshold

    score = 0
    score += int(_ge("vix_close", 22.0))           # C1 macro stress
    score += int(_ge("vix_close", 29.0))           # C2 severe macro stress (extra weight)
    score += int(_le("mom_60d", -0.15))            # C3 deeply oversold over 60d
    score += int(_le("mom_5d",  -0.05))            # C4 recent capitulation
    score += int(_le("rsi", 35.0))                 # C5 technical oversold
    # C6: high vol — RV>0.5 OR ATR>0.04
    c6 = _ge("realized_vol_20", 0.50) or _ge("atr_14_pct", 0.04)
    score += int(c6)
    score += int(_le("log_market_cap", 22.0))      # C7 small/mid-cap
    score += int(_ge("pred_5d", 0.005))            # C8 ML positive forecast
    score += int(_le("sma_50_dist", -0.07))        # C9 far below 50-day SMA
    return score


def _compute_vesign_score(row):
    """V2-aligned 0–100 'proximity to BUY signal' score, derived from VQS.

    Mapping: vesign_score = round(vqs * 100 / 9). Examples:
      VQS=9 → 100 ('Signal active' — Strong BUY)
      VQS=8 → 89  ('Signal active' — regular BUY)
      VQS=7 → 78  ('Approaching signal')
      VQS=6 → 67  ('Watching closely')
      VQS=4 → 44  ('Early watch')

    The ResearchPage gauge labels (signal_active >=86, approaching >=71,
    watching >=51, early_watch >=31) line up cleanly with the V2 BUY
    threshold of VQS>=8.
    """
    return int(round(_compute_vqs(row) * 100 / 9))


def run_scoring(target_date=None, open_positions=None, fast_v2=False):
    """Run signal scoring.

    target_date: if provided (str 'YYYY-MM-DD'), score that specific date instead of
                 the latest date in the features table. Used for backfilling missing dates.
    open_positions: optional pre-computed {ticker: (entry_price, buy_date)} dict.
                 When provided, skips the SQL self-join on the (large, growing)
                 signals table. The full backfill loop maintains this dict in
                 memory across iterations — avoids repaying the lookup cost
                 (~11s near end of history) on every call.
    fast_v2:    if True, skip loading historical analyst + health data (those
                are V1-rule-only inputs not used by V2 BUY/SELL/vesign_score).
                Saves ~6s per date during backfill. Backfilled rows will have
                NULL for analyst_condition/health_condition/etc. (unused columns).
                The live daily pipeline runs with fast_v2=False so going forward
                those columns continue to be populated.
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
    if open_positions is None:
        open_positions = _get_open_positions(as_of_date=target_date)

    # ---------- Load data ----------
    # V2 needs 65+ trading days of price history per call to compute mom_60d
    # and the 50-day SMA. Older code loaded only 5 days; widened here.
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
                SELECT DISTINCT date FROM features {inner_filter} ORDER BY date DESC LIMIT 65
            ) ORDER BY date ASC LIMIT 1
        ) {outer_filter}
        ORDER BY ticker, date
        """,
        engine
    )

    # For historical re-runs (target_date set), use the most recent analyst snapshot
    # on or before that date so signals reflect what was current then, not today.
    # For today's live run (target_date=None), always use current analyst_expectations.
    # fast_v2 reuses values already stored in signals[date]: those were filled in
    # historically by the same as-of-D logic, so they're the right values — and
    # reading them is one fast indexed query vs the slow correlated subquery.
    if fast_v2 and target_date:
        existing = pd.read_sql(
            text(
                "SELECT ticker, target_mean_price, target_high_price, "
                "target_low_price, number_of_analysts, health_score "
                "FROM signals WHERE DATE(date) = DATE(:td)"
            ),
            engine, params={"td": target_date},
        )
        if not existing.empty:
            analyst = existing[["ticker", "target_mean_price", "target_high_price",
                              "target_low_price", "number_of_analysts"]].drop_duplicates("ticker")
            health = existing[["ticker", "health_score"]].drop_duplicates("ticker")
            # health_score is TEXT in V1 schema; coerce to numeric for the >=3 comparison
            health["health_score"] = pd.to_numeric(health["health_score"], errors="coerce")
        else:
            # First-time backfill of this date — fall back to current snapshot.
            analyst = pd.read_sql("SELECT * FROM analyst_expectations", engine)
            health = pd.read_sql("SELECT ticker, score AS health_score FROM company_health", engine)
    elif target_date:
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
        analyst = pd.read_sql("SELECT * FROM analyst_expectations", engine)
        health = pd.read_sql("SELECT ticker, score AS health_score FROM company_health", engine)

    df = features.merge(analyst, on="ticker", how="left")
    df = df.merge(health, on="ticker", how="left")

    # ---------- ML prediction scores (used by V1 ml_condition AND V2 pred_5d) ----------
    # Filter to the date range of df — loading the full 2.3M-row table per call
    # made backfill take ~44s/date (predictions is ~50% of total runtime).
    if "predictions" in inspect(engine).get_table_names():
        if not df.empty:
            date_min = df["date"].min()
            date_max = df["date"].max()
            predictions = pd.read_sql(
                text(
                    "SELECT date, ticker, pred_5d, prediction_score FROM predictions "
                    "WHERE date >= :dmin AND date <= :dmax"
                ),
                engine, params={"dmin": str(date_min), "dmax": str(date_max)},
            )
        else:
            predictions = pd.DataFrame(columns=["date", "ticker", "pred_5d", "prediction_score"])
        df = df.merge(predictions, on=["date", "ticker"], how="left")
    else:
        df["prediction_score"] = float("nan")
        df["pred_5d"] = float("nan")

    # ---------- V2 macro signal: VIX close ----------
    if not df.empty:
        date_min = df["date"].min()
        date_max = df["date"].max()
        vix_df = pd.read_sql(
            text("SELECT date, close AS vix_close FROM vix WHERE date >= :dmin AND date <= :dmax"),
            engine, params={"dmin": str(date_min), "dmax": str(date_max)},
        )
        df = df.merge(vix_df, on="date", how="left")
    else:
        df["vix_close"] = float("nan")

    # ---------- V2 size factor: market cap (as-of merge from quarterly snapshots) ----------
    if not df.empty:
        mch = pd.read_sql(
            "SELECT ticker, date AS mc_date, market_cap FROM market_cap_history "
            "WHERE date >= '2019-01-01'",
            engine,
        )
        if not mch.empty:
            mch["mc_date"] = pd.to_datetime(mch["mc_date"])
            df["date"] = pd.to_datetime(df["date"])
            df_sorted = df.sort_values(["date", "ticker"]).reset_index(drop=True)
            mch_sorted = mch.sort_values(["mc_date", "ticker"]).reset_index(drop=True)
            df = pd.merge_asof(
                df_sorted, mch_sorted,
                left_on="date", right_on="mc_date", by="ticker", direction="backward",
            ).drop(columns=["mc_date"])
        else:
            df["market_cap"] = float("nan")
        # log_market_cap (V2 condition C7 uses this)
        import numpy as _np
        df["log_market_cap"] = _np.log(df["market_cap"].replace(0, _np.nan))
    else:
        df["log_market_cap"] = float("nan")

    # ---------- Analyst upside ----------
    df["fair_value_upside"] = (
        df["target_mean_price"] - df["close"]
    ) / df["close"]

    df["analyst_condition"] = (
        df["fair_value_upside"] >= config.get("analyst_upside_min", 0.30)
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
    # Pass through if no score (new ticker without history yet).
    health_min = config.get("health_score_min", 3)
    df["health_condition"] = (df["health_score"] >= health_min) | df["health_score"].isna()

    # ---------- ML score gate ----------
    # Waived for NULL scores (new tickers without enough history for the per-sector model).
    ml_min = config.get("ml_score_min", 0.05)
    df["ml_condition"] = (df["prediction_score"] >= ml_min) | df["prediction_score"].isna()

    # ---------- V2 indicators: momentum, vol, ATR, SMA-50 distance ----------
    import numpy as _np2
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    df["mom_5d"]  = df.groupby("ticker", sort=False)["close"].transform(lambda c: c / c.shift(5)  - 1)
    df["mom_60d"] = df.groupby("ticker", sort=False)["close"].transform(lambda c: c / c.shift(60) - 1)
    df["sma_50_dist"] = df["close"] / df.groupby("ticker", sort=False)["close"].transform(
        lambda c: c.rolling(50, min_periods=20).mean()
    ) - 1

    # realized_vol_20: rolling 20d stdev of log returns, annualized
    df["log_ret_tmp"] = df.groupby("ticker", sort=False)["close"].transform(
        lambda c: _np2.log(c / c.shift(1))
    )
    df["realized_vol_20"] = df.groupby("ticker", sort=False)["log_ret_tmp"].transform(
        lambda s: s.rolling(20).std()
    ) * (252 ** 0.5)
    df = df.drop(columns=["log_ret_tmp"])

    # ATR_14: Wilder-style EWM of true range, normalized by close
    prev_close = df.groupby("ticker", sort=False)["close"].shift(1)
    df["tr_tmp"] = pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"]  - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr_14_pct"] = df.groupby("ticker", sort=False)["tr_tmp"].transform(
        lambda s: s.ewm(alpha=1/14, adjust=False).mean()
    ) / df["close"]
    df = df.drop(columns=["tr_tmp"])

    # ---------- Filter to today before signal assignment ----------
    # Rolling windows already computed above using full history.
    # Signal assignment only touches today's rows — much faster.
    today = df["date"].max()  # always use the actual max date in the loaded slice
    today_df = df[df["date"] == today].copy()

    # ---------- V2 SELL logic ----------
    # V2 SELL: (RSI >= 70 AND price > entry) OR (calendar days held >= 175,
    # which approximates 120 trading days). No trailing stop. No ML gate.
    import numpy as np

    if open_positions:
        entry_prices = {t: v[0] for t, v in open_positions.items()}
        buy_dates    = {t: v[1] for t, v in open_positions.items()}
        today_df["entry_price"] = today_df["ticker"].map(entry_prices)
        today_df["buy_date"]    = today_df["ticker"].map(buy_dates)
        today_ts = pd.to_datetime(today_df["date"].max())
        days_held = (today_ts - pd.to_datetime(today_df["buy_date"])).dt.days

        rsi_sell_profitable = (
            today_df["entry_price"].notna()
            & (today_df["rsi"] >= 70)
            & (today_df["close"] > today_df["entry_price"])
        )
        time_exit = today_df["entry_price"].notna() & (days_held >= 175)

        today_df.drop(columns=["entry_price", "buy_date"], inplace=True)
    else:
        rsi_sell_profitable = pd.Series(False, index=today_df.index)
        time_exit = pd.Series(False, index=today_df.index)

    # ---------- V2 BUY logic ----------
    # Vectorized VQS — sum of 9 binary contrarian conditions, 0..9.
    # Equivalent to _compute_vqs() but row-wise apply is ~10x slower at scale.
    # Comparisons against NaN evaluate to False (matches "not met" semantics).
    today_df["vqs"] = (
        (today_df["vix_close"] > 22.0).astype(int)
        + (today_df["vix_close"] > 29.0).astype(int)
        + (today_df["mom_60d"] < -0.15).astype(int)
        + (today_df["mom_5d"]  < -0.05).astype(int)
        + (today_df["rsi"] < 35.0).astype(int)
        + ((today_df["realized_vol_20"] > 0.50) | (today_df["atr_14_pct"] > 0.04)).astype(int)
        + (today_df["log_market_cap"] < 22.0).astype(int)
        + (today_df["pred_5d"] > 0.005).astype(int)
        + (today_df["sma_50_dist"] < -0.07).astype(int)
    )
    buy_cond = today_df["vqs"] >= 8

    # Suppress BUY if already in an open position (one position per ticker).
    if open_positions:
        already_open = today_df["ticker"].isin(open_positions.keys())
        buy_cond = buy_cond & ~already_open

    # ---------- Combine into final signal ----------
    sell_cond = rsi_sell_profitable | time_exit

    today_df["signal"] = np.select(
        [sell_cond, buy_cond],
        ["SELL", "BUY"],
        default="HOLD"
    )

    # RSI-based fallback score (kept for legacy backwards-compat — not displayed)
    today_df["score"] = 50 - today_df["rsi"]

    # vesign_score = round(vqs * 100 / 9) — vectorized, matches _compute_vesign_score()
    today_df["vesign_score"] = ((today_df["vqs"] * 100 / 9).round()).astype(int)

    # Drop transient V2 input columns that aren't part of the signals schema.
    # vqs IS persisted (column exists). pred_5d/vix_close/market_cap/etc were
    # only needed to compute vqs and aren't reused downstream.
    _transient = [
        "pred_5d", "vix_close", "market_cap", "log_market_cap",
        "mom_5d", "mom_60d", "sma_50_dist", "realized_vol_20", "atr_14_pct",
    ]
    today_df = today_df.drop(columns=[c for c in _transient if c in today_df.columns])

    if "signals" in inspect(engine).get_table_names():
        # SQLite stores dates as 'YYYY-MM-DD HH:MM:SS.ffffff' strings; match the
        # date prefix to delete cleanly regardless of microseconds.
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM signals WHERE DATE(date) = DATE(:date)"),
                {"date": str(today)}
            )

    today_df.to_sql("signals", engine, if_exists="append", index=False)

    print("Hybrid signals generated successfully")
