import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.market_data import summarize_descriptions, update_company_health_batch
from features.forward_returns import compute_forward_returns
from models.walk_forward import maybe_retrain_for_today


def run_weekly():
    """Weekly pipeline: walk-forward retrain check + summarize descriptions
    + batch health scoring."""
    import gc

    # Refresh forward returns (training labels) from latest prices
    print("Refreshing forward returns (ML training labels)...")
    compute_forward_returns()
    gc.collect()

    # Walk-forward retrain check — idempotent. Only trains a new model dir at
    # quarter starts (Jan/Apr/Jul/Oct 1). On other weeks this is a no-op since
    # the current quarter's dir already exists from the daily pipeline.
    print("Walk-forward retrain check...")
    maybe_retrain_for_today()
    gc.collect()

    summarize_descriptions()
    gc.collect()

    update_company_health_batch()
    gc.collect()


if __name__ == "__main__":
    run_weekly()
