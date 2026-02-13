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

# ---------- Portfolio / evaluation ----------
from backtesting.backtest_engine import run_backtest
from risk.ranking_engine import run_ranking
from portfolio.allocator import run_allocator


def main():

    # ---------- Market data ----------
    update_prices()

    # ---------- Heavy daily fundamentals ----------
    update_fundamentals()
    update_analyst_data()

    # ---------- Feature pipeline ----------
    run_feature_pipeline()
    compute_forward_returns()

    # ---------- Model training / prediction ----------
    train_factor_weights()
    run_prediction_engine()

    # ---------- Signal generation ----------
    run_scoring()

    # ---------- Evaluation / allocation ----------
    run_backtest()
    run_ranking()
    run_allocator()


if __name__ == "__main__":
    main()
