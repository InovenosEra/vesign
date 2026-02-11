import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pipelines.daily_update import update_prices
from pipelines.feature_pipeline import run_feature_pipeline
from features.forward_returns import compute_forward_returns
from scoring.scoring_engine import run_scoring
from scoring.weight_training import train_factor_weights
from scoring.prediction_score_engine import run_prediction_engine
from backtesting.backtest_engine import run_backtest
from risk.ranking_engine import run_ranking
from portfolio.allocator import run_allocator


def main():
    update_prices()
    run_feature_pipeline()
    compute_forward_returns()

    train_factor_weights()
    run_prediction_engine()
    run_scoring()
    run_backtest()

    run_ranking()
    run_allocator()


if __name__ == "__main__":
    main()
