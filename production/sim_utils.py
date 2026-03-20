"""Simulation utilities for backtesting with historical analyst targets."""

import pandas as pd


def load_features_with_analyst_history(engine) -> pd.DataFrame:
    """Load features joined with closest-available historical analyst targets.

    Uses merge_asof (backward) so each feature row gets the most recent analyst
    snapshot whose date is <= the feature date. Falls back to the current
    analyst_expectations table if analyst_targets_history doesn't exist yet.
    """
    features = pd.read_sql("SELECT * FROM features ORDER BY ticker, date", engine)
    features["date"] = pd.to_datetime(features["date"])

    try:
        history = pd.read_sql(
            "SELECT date, ticker, target_mean_price, target_high_price, "
            "target_low_price, number_of_analysts "
            "FROM analyst_targets_history ORDER BY ticker, date",
            engine,
        )
        history["date"] = pd.to_datetime(history["date"])
        return pd.merge_asof(features, history, on="date", by="ticker", direction="backward")
    except Exception:
        # Fallback: history table doesn't exist yet — use current snapshot for all dates
        analyst = pd.read_sql("SELECT * FROM analyst_expectations", engine)
        return features.merge(analyst, on="ticker", how="left")
