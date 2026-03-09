import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------- Data pipelines ----------
from data.market_data import update_prices, update_vix, update_company_info, summarize_descriptions, update_company_health

# ---------- Feature engineering ----------
from data.loaders import load_prices, save_features
from features.technical import compute_features

# ---------- Modeling ----------
from models.predict import run_prediction_engine
from signals.engine import run_scoring

from backtesting.engine import build_trade_log

# ---------- Portfolio ----------
from portfolio.ranking import run_ranking
from portfolio.allocator import run_allocator


def run_daily():
    update_prices()
    update_vix()
    update_company_info()
    summarize_descriptions()
    update_company_health()

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
