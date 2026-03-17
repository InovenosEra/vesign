import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.market_data import summarize_descriptions, update_company_health_batch


def run_weekly():
    """Weekly pipeline: summarize new descriptions + batch health scoring via Claude Batches API."""
    import gc

    summarize_descriptions()
    gc.collect()

    update_company_health_batch()
    gc.collect()


if __name__ == "__main__":
    run_weekly()
