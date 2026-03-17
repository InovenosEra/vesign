import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------- Data pipelines ----------
from data.market_data import update_prices, update_vix, update_company_info

# ---------- Feature engineering ----------
from data.loaders import load_prices, save_features, engine
from features.technical import compute_features

# ---------- Modeling ----------
from models.predict import run_prediction_engine
from signals.engine import run_scoring

from backtesting.engine import build_trade_log

# ---------- Portfolio ----------
from portfolio.ranking import run_ranking
from portfolio.allocator import run_allocator

# Import repair functions from run_daily (single source of truth)
from production.run_daily import _repair_price_gaps, _repair_market_caps, _repair_analyst_targets


def run_daily_fast():
    """Fast daily pipeline — skips health scoring and description summarization.
    Those run weekly via production/run_weekly.py using the Claude Batches API.
    """
    import gc

    update_prices()
    gc.collect()

    update_vix()
    gc.collect()

    update_company_info()
    gc.collect()

    # ── Price gap repair before signal engine so no dates are skipped ────────
    _repair_price_gaps()

    prices = load_prices()
    features_df = compute_features(prices)
    save_features(features_df)
    del prices, features_df
    gc.collect()

    run_prediction_engine()
    run_scoring()

    build_trade_log()
    gc.collect()

    run_ranking()
    run_allocator()

    # ── Remaining self-healing repairs ───────────────────────────────────────
    _repair_market_caps()
    _repair_analyst_targets()


if __name__ == "__main__":
    run_daily_fast()
