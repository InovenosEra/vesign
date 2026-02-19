import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------- Data pipelines ----------
from pipelines.daily_update import update_prices
from pipelines.fundamentals_update import update_fundamentals
from features.analyst_data import update_analyst_data

# ---------- Feature engineering ----------
from pipelines.feature_pipeline import run_feature_pipeline

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

    run_feature_pipeline()

    run_prediction_engine()
    run_scoring()

    build_trade_log()

    run_ranking()
    run_allocator()


if __name__ == "__main__":
    run_daily()
