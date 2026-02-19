import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ---------- Data pipelines ----------
from pipelines.daily_update import update_prices
from pipelines.fundamentals_update import update_fundamentals
from features.analyst_data import update_analyst_data

# ---------- Feature engineering ----------
from pipelines.feature_pipeline import run_feature_pipeline
from features.forward_returns import compute_forward_returns

# ---------- Modeling ----------
from scoring.weight_training import train_factor_weights
from scoring.prediction_score_engine import run_prediction_engine
from scoring.scoring_engine import run_scoring

from backtesting.trade_builder import build_trade_log

# ---------- Portfolio / evaluation ----------
from backtesting.backtest_engine import run_backtest
from risk.ranking_engine import run_ranking
from portfolio.allocator import run_allocator


def daily_run():
    # data updates
    update_prices()
    update_fundamentals()
    update_analyst_data()

    # features
    run_feature_pipeline()

    # prediction & signals
    run_prediction_engine()
    run_scoring()

    # trade tracking
    build_trade_log()

    # portfolio
    run_ranking()
    run_allocator()


def training_run():
    compute_forward_returns()
    train_factor_weights()
    run_backtest()


if __name__ == "__main__":
    mode = "daily"   # change to "training" when needed

    if mode == "daily":
        daily_run()
    elif mode == "training":
        training_run()
