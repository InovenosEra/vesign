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
from production.backfill_trailing_stop import backfill_all_signals

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
        # Uses the canonical engine.run_scoring() per-date loop — same path as
        # the daily 7AM pipeline. Single source of truth, no drift risk.
        backfill_all_signals()
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
