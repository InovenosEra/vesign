import gc
import os
import pickle
import pandas as pd
from data.loaders import engine
from sqlalchemy import inspect, text

FEATURE_COLS = ["rsi_factor", "bb_factor", "macd_factor", "trend_factor"]
ML_MODELS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml_models")


def _load_pkl(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


def _pending_dates():
    """Dates present in features but not yet in predictions (US tickers only)."""
    insp = inspect(engine)
    with engine.connect() as c:
        all_dates = {r[0] for r in c.execute(text(
            "SELECT DISTINCT DATE(date) FROM features WHERE ticker NOT LIKE '%.TA'"
        ))}
        if "predictions" in insp.get_table_names():
            existing = {r[0] for r in c.execute(text(
                "SELECT DISTINCT DATE(date) FROM predictions WHERE ticker NOT LIKE '%.TA'"
            ))}
        else:
            existing = set()
    return sorted(all_dates - existing)


def run_prediction_engine():
    """Incrementally compute predictions for any date present in features
    but missing from predictions. Processes one date at a time to keep memory
    bounded — the original full-table rebuild used ~900MB on the 1GB server.
    """
    print("Running prediction score engine...")

    pending = _pending_dates()
    if not pending:
        print("Predictions already up-to-date")
        return

    print(f"Computing predictions for {len(pending)} date(s): {pending[0]}..{pending[-1]}")

    global_5d  = _load_pkl(os.path.join(ML_MODELS_DIR, "global_5d.pkl"))
    global_20d = _load_pkl(os.path.join(ML_MODELS_DIR, "global_20d.pkl"))

    if global_5d is not None and global_20d is not None:
        companies = pd.read_sql("SELECT ticker, sector FROM companies", engine)
        sector_models_cache = {}

        def _get_sector_models(sector):
            if sector in sector_models_cache:
                return sector_models_cache[sector]
            safe = str(sector).replace(" ", "_").replace("/", "_")
            m5  = _load_pkl(os.path.join(ML_MODELS_DIR, f"{safe}_5d.pkl"))  or global_5d
            m20 = _load_pkl(os.path.join(ML_MODELS_DIR, f"{safe}_20d.pkl")) or global_20d
            sector_models_cache[sector] = (m5, m20)
            return m5, m20

        for d in pending:
            features = pd.read_sql(
                text("SELECT * FROM features WHERE DATE(date) = :d AND ticker NOT LIKE '%.TA'"),
                engine, params={"d": d},
            )
            if features.empty:
                continue
            df = features.merge(companies, on="ticker", how="left")
            valid_mask = df[FEATURE_COLS].notna().all(axis=1)
            df["pred_5d"]  = float("nan")
            df["pred_20d"] = float("nan")

            for sector, group_idx in df[valid_mask].groupby("sector").groups.items():
                m5, m20 = _get_sector_models(sector)
                X = df.loc[group_idx, FEATURE_COLS]
                df.loc[group_idx, "pred_5d"]  = m5.predict(X)
                df.loc[group_idx, "pred_20d"] = m20.predict(X)

            no_sector_mask = valid_mask & df["sector"].isna()
            if no_sector_mask.any():
                X_ns = df.loc[no_sector_mask, FEATURE_COLS]
                df.loc[no_sector_mask, "pred_5d"]  = global_5d.predict(X_ns)
                df.loc[no_sector_mask, "pred_20d"] = global_20d.predict(X_ns)

            df["prediction_score"] = 0.6 * df["pred_5d"] + 0.4 * df["pred_20d"]

            with engine.begin() as c:
                c.execute(text("DELETE FROM predictions WHERE DATE(date) = :d"), {"d": d})
            df[["date", "ticker", "pred_5d", "pred_20d", "prediction_score"]].to_sql(
                "predictions", engine, if_exists="append", index=False,
            )
            del features, df
            gc.collect()

        print(f"Predictions updated for {len(pending)} date(s) (sector XGBoost models)")
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

    for d in pending:
        features = pd.read_sql(
            text("SELECT * FROM features WHERE DATE(date) = :d AND ticker NOT LIKE '%.TA'"),
            engine, params={"d": d},
        )
        if features.empty:
            continue
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
        features["prediction_score"] = 0.6 * features["pred_5d"] + 0.4 * features["pred_20d"]

        with engine.begin() as c:
            c.execute(text("DELETE FROM predictions WHERE DATE(date) = :d"), {"d": d})
        features[["date", "ticker", "pred_5d", "pred_20d", "prediction_score"]].to_sql(
            "predictions", engine, if_exists="append", index=False,
        )
        del features
        gc.collect()

    print(f"Predictions updated for {len(pending)} date(s) (linear weights fallback)")
