import os
import pickle
import pandas as pd
from data.loaders import engine
from sqlalchemy import inspect

FEATURE_COLS = ["rsi_factor", "bb_factor", "macd_factor", "trend_factor"]
ML_MODELS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml_models")


def _load_pkl(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


def run_prediction_engine():

    print("Running prediction score engine...")

    features = pd.read_sql("SELECT * FROM features", engine)

    # ---------- Try sector XGBoost pickles ----------
    global_5d = _load_pkl(os.path.join(ML_MODELS_DIR, "global_5d.pkl"))
    global_20d = _load_pkl(os.path.join(ML_MODELS_DIR, "global_20d.pkl"))

    if global_5d is not None and global_20d is not None:
        companies = pd.read_sql("SELECT ticker, sector FROM companies", engine)
        df = features.merge(companies, on="ticker", how="left")

        valid_mask = df[FEATURE_COLS].notna().all(axis=1)
        df["pred_5d"] = float("nan")
        df["pred_20d"] = float("nan")

        for sector, group_idx in df[valid_mask].groupby("sector").groups.items():
            safe_sector = str(sector).replace(" ", "_").replace("/", "_")
            m5  = _load_pkl(os.path.join(ML_MODELS_DIR, f"{safe_sector}_5d.pkl")) or global_5d
            m20 = _load_pkl(os.path.join(ML_MODELS_DIR, f"{safe_sector}_20d.pkl")) or global_20d

            X = df.loc[group_idx, FEATURE_COLS]
            df.loc[group_idx, "pred_5d"]  = m5.predict(X)
            df.loc[group_idx, "pred_20d"] = m20.predict(X)

        # Rows with no sector or missing features → use global model
        no_sector_mask = valid_mask & df["sector"].isna()
        if no_sector_mask.any():
            X_ns = df.loc[no_sector_mask, FEATURE_COLS]
            df.loc[no_sector_mask, "pred_5d"]  = global_5d.predict(X_ns)
            df.loc[no_sector_mask, "pred_20d"] = global_20d.predict(X_ns)

        df["prediction_score"] = 0.6 * df["pred_5d"] + 0.4 * df["pred_20d"]

        df[["date", "ticker", "pred_5d", "pred_20d", "prediction_score"]].to_sql(
            "predictions", engine, if_exists="replace", index=False
        )
        print("Predictions table updated (sector XGBoost models)")
        return

    # ---------- Fallback: linear weights from factor_weights table ----------
    if "factor_weights" not in inspect(engine).get_table_names():
        print("No trained weights found")
        return

    weights = pd.read_sql("SELECT * FROM factor_weights", engine)
    if weights.empty:
        print("No trained weights found")
        return

    w = weights.iloc[0]

    features["pred_5d"] = (
        features["rsi_factor"]   * w["short_rsi_factor"] +
        features["bb_factor"]    * w["short_bb_factor"] +
        features["macd_factor"]  * w["short_macd_factor"] +
        features["trend_factor"] * w["short_trend_factor"]
    )

    features["pred_20d"] = (
        features["rsi_factor"]   * w["med_rsi_factor"] +
        features["bb_factor"]    * w["med_bb_factor"] +
        features["macd_factor"]  * w["med_macd_factor"] +
        features["trend_factor"] * w["med_trend_factor"]
    )

    features["prediction_score"] = (
        0.6 * features["pred_5d"] +
        0.4 * features["pred_20d"]
    )

    features[["date", "ticker", "pred_5d", "pred_20d", "prediction_score"]].to_sql(
        "predictions", engine, if_exists="replace", index=False
    )

    print("Predictions table updated (linear weights fallback)")
