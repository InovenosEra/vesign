"""One-off rebuild script for main-2.0 branch.
Rebuilds vesign.db from 2020-01-01 with US-only universe (S&P 500/400/600).

Phases:
  1. Load universe  (populates `companies` table, US-only)
  2. Prices         (FMP historical, 2020-01-01 → latest session, ~1500 tickers)
  3. VIX            (yfinance, 2020-01-01 → today)
  4. Fundamentals   (FMP company_profile + analyst targets, parallel)
  5. Price gaps     (self-heal FMP rate-limit drops)
  6. Analyst fallback (yfinance for tickers FMP doesn't cover)
  7. Features       (technical indicators — RSI/BB/MACD/etc.)
  8. Forward returns + ML retrain (XGBoost)
  9. Predictions + scoring (signals)
 10. Signal date backfill + trade_log (backtest)
 11. Ranking + allocator
 12. Company health (Claude Batches)
 13. Descriptions  (Claude Haiku)
 14. Market cap repair
 15. Validation
"""
import os
import sys

# Force US-only universe for this rebuild
os.environ["VESIGN_US_ONLY"] = "1"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gc
from datetime import date, datetime, timedelta, UTC

import exchange_calendars as xcals
import pandas as pd
import yfinance as yf

from utils.universe_loader import load_universe
from data.market_data import (
    _download_and_save,
    update_company_info,
    update_company_health_batch,
    summarize_descriptions,
)
from data.loaders import engine
from features.technical import compute_and_save_features_chunked
from features.forward_returns import compute_forward_returns
from models.train import train_factor_weights
from models.predict import run_prediction_engine
from signals.engine import run_scoring
from backtesting.engine import build_trade_log
from portfolio.ranking import run_ranking
from portfolio.allocator import run_allocator
from production.run_daily import (
    _repair_price_gaps,
    _repair_market_caps,
    _repair_analyst_targets,
    _backfill_missing_signal_dates,
    _validate_pipeline,
)

HISTORY_START = date(2020, 1, 1)


def _banner(msg: str):
    print(f"\n{'=' * 72}\n  {msg}\n{'=' * 72}")


def _row_count(table: str) -> int:
    """Return row count, or -1 if the table doesn't exist yet."""
    import sqlite3
    try:
        with engine.connect() as conn:
            from sqlalchemy import text as _text
            return conn.execute(_text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0
    except Exception:
        return -1


def backfill_all_historical_signals():
    """Generate signals for ALL (ticker, date) combos in features table.

    Adapted from production/backfill_signals.py but covers every ticker,
    not just newly-added ones. Historical backfill uses RSI>=70 for SELL
    (no trailing stop) — trailing stops are only for live positions.
    """
    import numpy as np
    import yaml
    from sqlalchemy import text as _text

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(BASE_DIR, "config", "settings.yaml")) as f:
        config = yaml.safe_load(f)

    volume_threshold = config.get("volume_ratio_threshold", 1.5)
    pct_52w_min      = config.get("pct_from_52w_high_min", 0.10)
    bb_pct_b_max     = config.get("bb_pct_b_max", 0.10)
    analyst_upside   = config.get("analyst_upside_min", 0.30)

    # Load all features + analyst + predictions + health
    print("Loading features (bulk)…")
    features = pd.read_sql("SELECT * FROM features ORDER BY ticker, date", engine)
    print(f"  features rows: {len(features):,}")

    print("Loading analyst_expectations…")
    analyst = pd.read_sql("SELECT * FROM analyst_expectations", engine)

    # Predictions (may be absent for TASE tickers)
    try:
        predictions = pd.read_sql("SELECT ticker, date, prediction_score FROM predictions", engine)
    except Exception:
        predictions = pd.DataFrame(columns=["ticker", "date", "prediction_score"])

    # Health scores (may be empty at this stage of rebuild)
    try:
        health = pd.read_sql("SELECT ticker, score AS health_score FROM company_health", engine)
    except Exception:
        health = pd.DataFrame(columns=["ticker", "health_score"])

    df = features.merge(analyst, on="ticker", how="left")
    df = df.merge(predictions, on=["ticker", "date"], how="left")
    df = df.merge(health, on="ticker", how="left")

    df["fair_value_upside"] = (df["target_mean_price"] - df["close"]) / df["close"]
    df["analyst_condition"] = df["fair_value_upside"] >= analyst_upside

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
    df["health_ok"] = (df["health_score"].isna()) | (df["health_score"] >= 3)

    # ML score passes if >= threshold OR NaN (TASE or too-early signal)
    ml_threshold = config.get("ml_threshold", 0.02)
    df["ml_ok"] = (df["prediction_score"].isna()) | (df["prediction_score"] >= ml_threshold)

    buy_cond = (
        (df["rsi_3day_flag"] == 3)
        & df["bb_condition"]
        & df["analyst_condition"]
        & df["volume_flag"]
        & df["week52_condition"]
        & df["health_ok"]
        & df["ml_ok"]
    )
    sell_cond = df["rsi"] >= 70

    df["signal"] = np.select(
        [buy_cond, sell_cond],
        ["BUY", "SELL"],
        default="HOLD",
    )
    df["score"] = 50 - df["rsi"]

    # Wipe existing signals (the 7 daily signals already generated) and insert full history
    with engine.begin() as conn:
        conn.execute(_text("DELETE FROM signals"))

    # Select columns that exist in the signals schema. Signals table created via to_sql
    # so it infers columns from dataframe. Keep the essentials only.
    out_cols = [c for c in (
        "date", "ticker", "close", "rsi", "bb_pct_b", "fair_value_upside",
        "target_mean_price", "volume_ratio", "pct_from_52w_high",
        "prediction_score", "health_score", "signal", "score"
    ) if c in df.columns]
    df[out_cols].to_sql("signals", engine, if_exists="append", index=False)

    buy_count  = (df["signal"] == "BUY").sum()
    sell_count = (df["signal"] == "SELL").sum()
    hold_count = len(df) - buy_count - sell_count
    print(f"  Inserted {len(df):,} signals — {buy_count:,} BUY, {sell_count:,} SELL, {hold_count:,} HOLD")


def _latest_nyse_session():
    today = datetime.now(UTC).date()
    nyse = xcals.get_calendar("XNYS")
    sessions = nyse.sessions_in_range(
        pd.Timestamp(today - timedelta(days=10)), pd.Timestamp(today)
    )
    return sessions[-1].date() if len(sessions) > 0 else today - timedelta(days=1)


def rebuild_prices_from_2020(tickers: list, end_date: date):
    _banner(f"Phase 2: Prices {HISTORY_START} → {end_date} ({len(tickers)} tickers)")
    _download_and_save(tickers, HISTORY_START, end_date, batch_size=20)


def rebuild_vix_from_2020():
    _banner(f"Phase 3: VIX from {HISTORY_START}")
    today = datetime.now(UTC).date()
    data = yf.download(
        "^VIX",
        start=HISTORY_START,
        end=today,
        auto_adjust=False,
        progress=False,
    )
    if data is None or data.empty:
        print("VIX download empty")
        return
    data.reset_index(inplace=True)
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] if col[1] == "" else col[0] for col in data.columns]
    data.rename(columns={"Date": "date", "Close": "close"}, inplace=True)
    data = data[["date", "close"]].dropna()
    today_ts = pd.Timestamp(today)
    data = data[data["date"] < today_ts]
    data.drop_duplicates(subset=["date"], inplace=True)
    data.to_sql("vix", engine, if_exists="append", index=False)
    print(f"VIX: saved {len(data)} rows from {data['date'].min().date()} to {data['date'].max().date()}")


def run_rebuild():
    started = datetime.now()

    # ── Phase 1: Universe ──────────────────────────────────────────────────
    if _row_count("companies") >= 1500:
        print(f"\n[SKIP Phase 1: companies has {_row_count('companies')} rows]")
        tickers = None  # will read from DB if Phase 2 needs it
    else:
        _banner("Phase 1: Loading US universe (S&P 500/400/600)")
        tickers = load_universe()

    end_date = _latest_nyse_session()

    # ── Phase 2: Historical prices from 2020-01-01 ─────────────────────────
    if _row_count("daily_prices") > 2_000_000:
        print(f"[SKIP Phase 2: daily_prices has {_row_count('daily_prices'):,} rows]")
    else:
        if tickers is None:
            tickers = pd.read_sql("SELECT ticker FROM companies", engine)["ticker"].tolist()
        print(f"Universe: {len(tickers)} US tickers, latest session: {end_date}")
        rebuild_prices_from_2020(tickers, end_date)
        gc.collect()

    # ── Phase 3: VIX ───────────────────────────────────────────────────────
    if _row_count("vix") >= 1500:
        print(f"[SKIP Phase 3: vix has {_row_count('vix')} rows]")
    else:
        rebuild_vix_from_2020()
        gc.collect()

    # ── Phase 4: Fundamentals + analyst targets ────────────────────────────
    if _row_count("fundamentals") >= 1500 and _row_count("analyst_expectations") >= 1500:
        print(f"[SKIP Phase 4: fundamentals={_row_count('fundamentals')}, analyst_expectations={_row_count('analyst_expectations')}]")
    else:
        _banner("Phase 4: Fundamentals + analyst targets (FMP)")
        update_company_info()
        gc.collect()

    # ── Phase 5: Price gap repair ──────────────────────────────────────────
    _banner("Phase 5: Price gap repair")
    _repair_price_gaps()

    # ── Phase 6: Analyst target fallback (yfinance) ────────────────────────
    _banner("Phase 6: Analyst target fallback (yfinance)")
    _repair_analyst_targets()
    gc.collect()

    # ── Phase 7: Features (~1600 trading days to cover 2020+) ──────────────
    if _row_count("features") > 2_000_000:
        print(f"[SKIP Phase 7: features has {_row_count('features'):,} rows]")
    else:
        _banner("Phase 7: Technical indicators (features)")
        compute_and_save_features_chunked(engine, days=1700, chunk_size=100)
        gc.collect()

    # ── Phase 8: Forward returns + ML retrain ──────────────────────────────
    if _row_count("forward_returns") > 2_000_000 and _row_count("factor_weights") >= 1:
        print(f"[SKIP Phase 8: forward_returns={_row_count('forward_returns'):,}, factor_weights={_row_count('factor_weights')}]")
    else:
        _banner("Phase 8: Forward returns + ML retrain (XGBoost)")
        compute_forward_returns()
        gc.collect()
        cutoff = (datetime.today() - timedelta(days=20)).strftime("%Y-%m-%d")
        train_factor_weights(train_end_date=cutoff)
        gc.collect()

    # ── Phase 9: Predictions + scoring ─────────────────────────────────────
    _banner("Phase 9: ML predictions + signal scoring")
    if _row_count("predictions") > 2_000_000:
        print(f"[SKIP 9a prediction_engine: predictions has {_row_count('predictions'):,} rows]")
    else:
        run_prediction_engine()
    run_scoring()

    # ── Phase 10: Backfill + trade_log ─────────────────────────────────────
    if _row_count("signals") > 2_000_000:
        print(f"[SKIP Phase 10a: signals has {_row_count('signals'):,} rows]")
    else:
        _banner("Phase 10a: Full historical signal backfill (2020+)")
        backfill_all_historical_signals()
        gc.collect()

    _banner("Phase 10b: Recent-dates gap backfill (today's signals)")
    _backfill_missing_signal_dates()

    _banner("Phase 10c: Trade log build")
    build_trade_log()
    gc.collect()

    # ── Phase 11: Ranking + allocator ──────────────────────────────────────
    _banner("Phase 11: Ranking + allocator")
    run_ranking()
    run_allocator()

    # ── Phase 12: Company health (Claude Batches) ──────────────────────────
    _banner("Phase 12: Company health (Claude Batches)")
    update_company_health_batch()
    gc.collect()

    # ── Phase 13: Descriptions (Claude Haiku) ──────────────────────────────
    _banner("Phase 13: Company description summaries")
    summarize_descriptions()
    gc.collect()

    # ── Phase 14: Market cap repair ────────────────────────────────────────
    _banner("Phase 14: Market cap repair")
    _repair_market_caps()

    # ── Phase 15: Validation ───────────────────────────────────────────────
    _banner("Phase 15: Pipeline validation")
    _validate_pipeline()

    elapsed = datetime.now() - started
    _banner(f"REBUILD COMPLETE in {elapsed} (started {started.strftime('%H:%M:%S')})")


if __name__ == "__main__":
    run_rebuild()
