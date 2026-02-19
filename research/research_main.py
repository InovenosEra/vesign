import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from features.forward_returns import compute_forward_returns
from models.train import train_factor_weights
from backtesting.backtest_engine import run_backtest


def run_training():
    compute_forward_returns()
    train_factor_weights()
    run_backtest()


if __name__ == "__main__":
    run_training()
