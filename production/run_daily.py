import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------- Data pipelines ----------
from pipelines.daily_update import update_prices
from pipelines.fundamentals_update import update_fundamentals
from features.analyst_data import update_analyst_data

# ---------- Feature engineering ----------
from data.loaders import load_prices, save_features
from features.technical import compute_features

# ---------- Modeling ----------
from scoring.prediction_score_engine import run_prediction_engine
from scoring.scoring_engine import run_scoring

from backtesting.trade_builder import build_trade_log

# ---------- Portfolio ----------
from risk.ranking_engine import run_ranking
from portfolio.allocator import run_allocator


def run_daily():
    update_prices()
    update_fundamentals()
    update_analyst_data()

    prices = load_prices()
    features_df = compute_features(prices)
    save_features(features_df)

    run_prediction_engine()
    run_scoring()

    build_trade_log()

    run_ranking()
    run_allocator()


if __name__ == "__main__":
    run_daily()
