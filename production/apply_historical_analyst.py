"""Fetch FMP price-target-news for all tickers, build a per-date analyst consensus,
then regenerate signals + trade_log using the per-date values.

Strategy:
  1. Fetch /stable/price-target-news for each US ticker (parallel).
  2. Save individual target changes to table `analyst_target_changes`.
  3. For each (ticker, feature_date), compute consensus from targets
     published in the prior 90 days. Where no targets exist, values remain
     NULL (signal engine's analyst_condition already passes NULL gracefully).
  4. DELETE FROM signals and re-insert using backfill_all_historical_signals
     with per-date analyst data.
  5. Rebuild trade_log (applies trailing stop from config).
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, UTC

import numpy as np
import pandas as pd
import yaml
from sqlalchemy import text

from data import fmp
from data.loaders import engine


def fetch_target_news(ticker: str) -> list:
    """Return list of target-change dicts for one ticker (up to 100 most recent)."""
    data = fmp._get("price-target-news", {"symbol": ticker, "limit": 100})
    if not isinstance(data, list):
        return []
    rows = []
    for item in data:
        pd_str = item.get("publishedDate")
        pt = item.get("priceTarget") or item.get("adjPriceTarget")
        if not pd_str or pt is None:
            continue
        rows.append({
            "ticker": ticker,
            "published_date": pd_str[:10],  # normalize to YYYY-MM-DD
            "price_target": float(pt),
            "analyst_company": item.get("analystCompany", "") or "",
        })
    return rows


def fetch_all_analyst_history(tickers: list) -> pd.DataFrame:
    """Parallel fetch of price-target-news for all tickers."""
    print(f"Fetching price-target-news for {len(tickers)} tickers…")
    all_rows = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_target_news, t): t for t in tickers}
        done = 0
        for fut in as_completed(futures):
            all_rows.extend(fut.result())
            done += 1
            if done % 200 == 0:
                print(f"  {done}/{len(tickers)} tickers fetched, {len(all_rows):,} rows so far")
    print(f"  total rows: {len(all_rows):,}")
    return pd.DataFrame(all_rows)


def save_analyst_target_changes(df: pd.DataFrame) -> None:
    """Persist raw target changes into analyst_target_changes table."""
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS analyst_target_changes"))
    df.to_sql("analyst_target_changes", engine, if_exists="replace", index=False)
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX idx_atc_ticker_date ON analyst_target_changes(ticker, published_date)"))
    print(f"  saved {len(df):,} target changes")


def compute_per_date_consensus(changes: pd.DataFrame, feature_keys: pd.DataFrame, window_days: int = 365) -> pd.DataFrame:
    """Compute per-date consensus using each analyst's LATEST target within the
    lookback window. Analysts often update only every 6-12 months, so using all
    targets within the window over-weights recently-active analysts and under-
    counts the full coverage. Taking one target per analyst (their most recent)
    matches FMP's consensus behavior more closely.
    """
    print(f"Computing per-date consensus (window={window_days}d, one target per analyst, {len(feature_keys):,} rows)…")

    changes = changes.copy()
    changes["published_date"] = pd.to_datetime(changes["published_date"])
    feature_keys = feature_keys.copy()
    feature_keys["date"] = pd.to_datetime(feature_keys["date"])

    if "analyst_company" not in changes.columns:
        changes["analyst_company"] = ""
    changes["analyst_company"] = changes["analyst_company"].fillna("").astype(str)

    # Per-ticker sorted list of (published_date, analyst_company, price_target)
    ticker_to_changes: dict[str, pd.DataFrame] = {
        tk: grp[["published_date", "analyst_company", "price_target"]]
               .sort_values("published_date").reset_index(drop=True)
        for tk, grp in changes.groupby("ticker")
    }

    result_targets = []

    for ticker, group in feature_keys.groupby("ticker"):
        tc = ticker_to_changes.get(ticker)
        if tc is None or tc.empty:
            for d in group["date"]:
                result_targets.append({
                    "ticker": ticker, "date": d,
                    "target_mean_price": None, "target_high_price": None,
                    "target_low_price": None, "number_of_analysts": None,
                })
            continue

        pub_dates_arr = tc["published_date"].values.astype("datetime64[D]")
        analysts_arr = tc["analyst_company"].values
        targets_arr = tc["price_target"].values

        for d in group["date"]:
            d64 = np.datetime64(d.date(), "D")
            lo = d64 - np.timedelta64(window_days, "D")
            idx_hi = np.searchsorted(pub_dates_arr, d64, side="right")
            idx_lo = np.searchsorted(pub_dates_arr, lo, side="right")
            if idx_hi <= idx_lo:
                result_targets.append({
                    "ticker": ticker, "date": d,
                    "target_mean_price": None, "target_high_price": None,
                    "target_low_price": None, "number_of_analysts": None,
                })
                continue

            # Walk window and keep LATEST target per analyst.
            # Using dict: analyst -> price_target (overwritten since we iterate oldest→newest)
            window_analysts = analysts_arr[idx_lo:idx_hi]
            window_targets  = targets_arr[idx_lo:idx_hi]
            latest_per: dict[str, float] = {}
            for i in range(len(window_analysts)):
                name = window_analysts[i] or "_unknown"
                latest_per[name] = float(window_targets[i])

            if not latest_per:
                result_targets.append({
                    "ticker": ticker, "date": d,
                    "target_mean_price": None, "target_high_price": None,
                    "target_low_price": None, "number_of_analysts": None,
                })
                continue

            vals = list(latest_per.values())
            result_targets.append({
                "ticker": ticker, "date": d,
                "target_mean_price": float(sum(vals) / len(vals)),
                "target_high_price": float(max(vals)),
                "target_low_price": float(min(vals)),
                "number_of_analysts": int(len(vals)),
            })

    df = pd.DataFrame(result_targets)
    print(f"  computed {len(df):,} per-date rows ({df['target_mean_price'].notna().sum():,} with data)")
    return df


def backfill_signals_with_per_date_analyst(per_date_analyst: pd.DataFrame):
    """Regenerate signals table using per-date analyst data (replacing current snapshot)."""
    from sqlalchemy import text as _text

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(BASE_DIR, "config", "settings.yaml")) as f:
        config = yaml.safe_load(f)

    volume_threshold = config.get("volume_ratio_threshold", 1.5)
    pct_52w_min      = config.get("pct_from_52w_high_min", 0.10)
    bb_pct_b_max     = config.get("bb_pct_b_max", 0.10)
    analyst_upside   = config.get("analyst_upside_min", 0.30)
    ml_threshold     = config.get("ml_score_min", 0.02)

    print("Loading features (bulk)…")
    features = pd.read_sql("SELECT * FROM features ORDER BY ticker, date", engine)
    features["date"] = pd.to_datetime(features["date"])

    # Drop the stale current-snapshot analyst columns that backfill used before
    for c in ("target_mean_price", "target_high_price", "target_low_price",
              "number_of_analysts", "last_update"):
        if c in features.columns:
            features = features.drop(columns=[c])

    # Merge per-date analyst
    per_date_analyst = per_date_analyst.copy()
    per_date_analyst["date"] = pd.to_datetime(per_date_analyst["date"])
    df = features.merge(per_date_analyst, on=["ticker", "date"], how="left")

    # Predictions + health
    try:
        predictions = pd.read_sql("SELECT ticker, date, prediction_score FROM predictions", engine)
        predictions["date"] = pd.to_datetime(predictions["date"])
        df = df.merge(predictions, on=["ticker", "date"], how="left")
    except Exception:
        df["prediction_score"] = float("nan")

    # Per-year health lookup: for each (ticker, date), use the most recent
    # company_health_history snapshot at or before that date.
    try:
        health_hist = pd.read_sql(
            "SELECT ticker, score AS health_score, recorded_at FROM company_health_history",
            engine,
        )
        health_hist["date"] = pd.to_datetime(health_hist["recorded_at"].str[:10])
        # merge_asof requires BOTH sides sorted by `on` column
        health_hist = health_hist[["ticker", "date", "health_score"]].sort_values("date")
        df = df.sort_values("date")
        df_merged = pd.merge_asof(
            df, health_hist, on="date", by="ticker", direction="backward"
        )
        # Replace df with the merged version (preserves all df columns + adds health_score)
        df = df_merged
        # For dates before any snapshot, fall back to current company_health
        missing_mask = df["health_score"].isna()
        if missing_mask.any():
            current_health = pd.read_sql(
                "SELECT ticker, score AS h FROM company_health", engine
            ).set_index("ticker")["h"]
            df.loc[missing_mask, "health_score"] = df.loc[missing_mask, "ticker"].map(current_health)
    except Exception as e:
        print(f"  health history lookup failed: {e}; falling back to current company_health")
        try:
            health = pd.read_sql("SELECT ticker, score AS health_score FROM company_health", engine)
            df = df.merge(health, on="ticker", how="left")
        except Exception:
            df["health_score"] = float("nan")

    # Criteria computation (same as backfill_all_historical_signals)
    df["fair_value_upside"] = (df["target_mean_price"] - df["close"]) / df["close"]
    df["analyst_condition"] = (df["fair_value_upside"] >= analyst_upside) | df["fair_value_upside"].isna()

    bb_range = df["bb_high"] - df["bb_low"]
    df["bb_pct_b"] = (df["close"] - df["bb_low"]) / bb_range.where(bb_range != 0, other=float("nan"))
    df["bb_condition"] = df["bb_pct_b"] < bb_pct_b_max

    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    df["rsi_below_30"] = df["rsi"] < 30
    df["rsi_3day_flag"] = (
        df.groupby("ticker")["rsi_below_30"]
          .rolling(3, min_periods=3).sum().reset_index(level=0, drop=True)
    )
    df["volume_flag"] = (
        df.groupby("ticker")["volume_ratio"]
          .rolling(3, min_periods=1).max().reset_index(level=0, drop=True)
        >= volume_threshold
    )
    df["week52_condition"] = df["pct_from_52w_high"] <= -pct_52w_min
    df["health_condition"] = (df["health_score"].isna()) | (df["health_score"] >= 3)
    df["ml_condition"] = (df["prediction_score"].isna()) | (df["prediction_score"] >= ml_threshold)

    buy_cond = (
        (df["rsi_3day_flag"] == 3)
        & df["bb_condition"]
        & df["analyst_condition"]
        & df["volume_flag"]
        & df["week52_condition"]
        & df["health_condition"]
        & df["ml_condition"]
    )
    # MASTER gate: SELL requires ML < 0 (waived for NULL / TASE)
    ml_negative = (df["prediction_score"] < 0) | df["prediction_score"].isna() | df["ticker"].str.endswith(".TA")
    sell_cond = (df["rsi"] >= 70) & ml_negative

    df["signal"] = np.select([buy_cond, sell_cond], ["BUY", "SELL"], default="HOLD")
    df["score"] = 50 - df["rsi"]

    # Vesign score (0–100) — same helper used by the live engine
    from signals.engine import _compute_vesign_score
    df["vesign_score"] = df.apply(_compute_vesign_score, axis=1)

    with engine.begin() as conn:
        conn.execute(_text("DELETE FROM signals"))

    out_cols = [c for c in (
        "date", "ticker", "close", "rsi", "bb_pct_b", "fair_value_upside",
        "target_mean_price", "target_high_price", "target_low_price",
        "number_of_analysts", "volume_ratio", "pct_from_52w_high",
        "prediction_score", "health_score", "signal", "score",
        "rsi_3day_flag", "volume_flag", "week52_condition",
        "health_condition", "ml_condition", "vesign_score",
    ) if c in df.columns]
    df[out_cols].to_sql("signals", engine, if_exists="append", index=False)

    buy_n  = (df["signal"] == "BUY").sum()
    sell_n = (df["signal"] == "SELL").sum()
    hold_n = len(df) - buy_n - sell_n
    print(f"  signals: {len(df):,} total — {buy_n:,} BUY, {sell_n:,} SELL, {hold_n:,} HOLD")


def main():
    started = datetime.now()

    # 1. Load tickers
    tickers = pd.read_sql("SELECT ticker FROM companies WHERE COALESCE(market,'US') = 'US'", engine)["ticker"].tolist()
    print(f"Tickers: {len(tickers)}")

    # 2. Fetch historical analyst target changes (skip if already populated)
    try:
        existing_count = pd.read_sql("SELECT COUNT(*) AS n FROM analyst_target_changes", engine)["n"][0]
    except Exception:
        existing_count = 0
    if existing_count > 10_000:
        print(f"[SKIP fetch: analyst_target_changes has {existing_count:,} rows]")
        changes = pd.read_sql("SELECT * FROM analyst_target_changes", engine)
    else:
        changes = fetch_all_analyst_history(tickers)
        if changes.empty:
            print("No analyst data fetched — aborting.")
            return
        save_analyst_target_changes(changes)

    # 3. Build per-date consensus keyed on all feature rows
    print("Loading feature keys…")
    feature_keys = pd.read_sql("SELECT ticker, date FROM features", engine)
    per_date = compute_per_date_consensus(changes, feature_keys, window_days=365)

    # 4. Regenerate signals
    backfill_signals_with_per_date_analyst(per_date)

    # 5. Rebuild trade_log (respects current config trailing_stop_pct)
    from backtesting.engine import build_trade_log
    build_trade_log()

    # 6. Final stats
    import sqlite3
    c = sqlite3.connect("vesign.db")
    n, avg_y, wr = c.execute("SELECT COUNT(*), AVG(return_pct)*100, 100.0*SUM(CASE WHEN return_pct>0 THEN 1 ELSE 0 END)/COUNT(*) FROM trade_log").fetchone()
    print(f"\nFinal trade_log: {n} trades, avg {avg_y:.2f}%, win {wr:.1f}%")
    n, avg_y, wr = c.execute("SELECT COUNT(*), AVG(return_pct)*100, 100.0*SUM(CASE WHEN return_pct>0 THEN 1 ELSE 0 END)/COUNT(*) FROM trade_log WHERE sell_date >= '2025-04-20'").fetchone()
    print(f"  Last 12mo: {n} trades, avg {avg_y:.2f}%, win {wr:.1f}%")

    elapsed = datetime.now() - started
    print(f"\nCompleted in {elapsed}")


if __name__ == "__main__":
    main()
