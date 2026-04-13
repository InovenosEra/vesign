import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.market_data import summarize_descriptions, update_company_health_batch
from models.train import train_factor_weights
from features.forward_returns import compute_forward_returns
from datetime import datetime, timedelta


def run_weekly():
    """Weekly pipeline: ML retrain + summarize descriptions + batch health scoring."""
    import gc

    # Refresh forward returns (training labels) from latest prices
    print("Refreshing forward returns (ML training labels)...")
    compute_forward_returns()
    gc.collect()

    # Retrain ML models with a 20-day out-of-sample guard
    cutoff = (datetime.today() - timedelta(days=20)).strftime("%Y-%m-%d")
    print(f"Retraining ML models (train_end_date={cutoff})...")
    train_factor_weights(train_end_date=cutoff)
    gc.collect()

    summarize_descriptions()
    gc.collect()

    update_company_health_batch()
    gc.collect()


if __name__ == "__main__":
    run_weekly()
