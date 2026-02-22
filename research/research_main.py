import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from features.forward_returns import compute_forward_returns
from models.train import train_factor_weights
from backtesting.engine import run_backtest

# ---------------------------------------------------------------------------
# Temporal split: the model is trained on data BEFORE train_end, and the
# backtest is evaluated ONLY on data from eval_start onwards.  This prevents
# the model from seeing any information from the evaluation period, removing
# look-ahead bias from the research pipeline.
#
# By default we reserve the most recent 6 months as the out-of-sample window.
# ---------------------------------------------------------------------------
_today = pd.Timestamp.today().normalize()
TRAIN_END  = (_today - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
EVAL_START = TRAIN_END


def run_training():
    print(f"Train window : up to        {TRAIN_END}  (exclusive)")
    print(f"Eval  window : from         {EVAL_START} (inclusive)")

    compute_forward_returns()
    train_factor_weights(train_end_date=TRAIN_END)
    run_backtest(eval_start_date=EVAL_START)


if __name__ == "__main__":
    run_training()
