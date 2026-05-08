import gc
import os
import pickle
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect, text

from data.loaders import engine

FEATURE_COLS = ["rsi_factor", "bb_factor", "macd_factor", "trend_factor"]
ML_MODELS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml_models")
ML_WALK_DIR = Path(ML_MODELS_DIR) / "walk"


def _load_pkl(path):
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


def _walk_dirs():
    """Sorted list of walk-forward model dirs (ISO dates sort lexicographically)."""
    if not ML_WALK_DIR.exists():
        return []
    return sorted(p for p in ML_WALK_DIR.iterdir() if p.is_dir() and not p.name.startswith("_"))


def _models_for_date(date_str: str):
    """For a prediction date, return (dir, cutoff_str) of the latest walk-forward
    model whose cutoff <= date_str.

    Returns (None, None) if no valid model exists for the date — caller should
    SKIP rather than predict, otherwise we'd produce a leak. This case happens
    when features extend earlier than the earliest walk-forward cutoff (e.g.
    2018-2019 features exist as ML training warmup but the earliest walk dir
    is 2020-01-01 — we must NOT predict 2018-2019 with a 2020 model).

    Falls back to legacy ML_MODELS_DIR with empty cutoff string only when there
    are no walk dirs at all (emergency fallback for first-run installs).
    """
    dirs = _walk_dirs()
    if not dirs:
        return Path(ML_MODELS_DIR), ""
    valid = [d for d in dirs if d.name <= date_str]
    if not valid:
        return None, None
    return valid[-1], valid[-1].name


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


def _has_model_cutoff_column() -> bool:
    insp = inspect(engine)
    if "predictions" not in insp.get_table_names():
        return False
    return any(c["name"] == "model_cutoff" for c in insp.get_columns("predictions"))


def run_prediction_engine():
    """Incrementally compute predictions for any date present in features
    but missing from predictions. Each date's prediction uses the latest
    walk-forward model whose cutoff <= that date — leak-free by construction.
    """
    print("Running prediction score engine...")

    pending = _pending_dates()
    if not pending:
        print("Predictions already up-to-date")
        return

    print(f"Computing predictions for {len(pending)} date(s): {pending[0]}..{pending[-1]}")

    has_cutoff_col = _has_model_cutoff_column()
    insert_cols = ["date", "ticker", "pred_5d", "pred_20d", "prediction_score"]
    if has_cutoff_col:
        insert_cols.append("model_cutoff")

    companies = pd.read_sql("SELECT ticker, sector FROM companies", engine)

    # Cache loaded models per (dir_path, sector) so per-date lookup doesn't
    # re-pickle.
    model_cache: dict = {}

    def _load_models_for_dir(mdir: Path):
        key = ("global", str(mdir))
        if key not in model_cache:
            model_cache[key] = (
                _load_pkl(os.path.join(str(mdir), "global_5d.pkl")),
                _load_pkl(os.path.join(str(mdir), "global_20d.pkl")),
            )
        return model_cache[key]

    def _sector_models(mdir: Path, sector, fallback_5d, fallback_20d):
        key = (str(mdir), sector)
        if key in model_cache:
            return model_cache[key]
        safe = str(sector).replace(" ", "_").replace("/", "_")
        m5 = _load_pkl(os.path.join(str(mdir), f"{safe}_5d.pkl")) or fallback_5d
        m20 = _load_pkl(os.path.join(str(mdir), f"{safe}_20d.pkl")) or fallback_20d
        model_cache[key] = (m5, m20)
        return m5, m20

    # ---------- Walk-forward / legacy XGBoost path ----------
    skipped = 0
    for d in pending:
        mdir, cutoff_str = _models_for_date(d)
        if mdir is None:
            # Date predates the earliest walk-forward cutoff — skip to avoid leak.
            skipped += 1
            continue
        global_5d, global_20d = _load_models_for_dir(mdir)
        if global_5d is None or global_20d is None:
            # No XGBoost models found at all — fall through to linear weights below
            break

        features = pd.read_sql(
            text("SELECT * FROM features WHERE DATE(date) = :d AND ticker NOT LIKE '%.TA'"),
            engine, params={"d": d},
        )
        if features.empty:
            continue
        df = features.merge(companies, on="ticker", how="left")
        valid_mask = df[FEATURE_COLS].notna().all(axis=1)
        df["pred_5d"] = float("nan")
        df["pred_20d"] = float("nan")

        for sector, group_idx in df[valid_mask].groupby("sector").groups.items():
            m5, m20 = _sector_models(mdir, sector, global_5d, global_20d)
            X = df.loc[group_idx, FEATURE_COLS]
            df.loc[group_idx, "pred_5d"] = m5.predict(X)
            df.loc[group_idx, "pred_20d"] = m20.predict(X)

        no_sector_mask = valid_mask & df["sector"].isna()
        if no_sector_mask.any():
            X_ns = df.loc[no_sector_mask, FEATURE_COLS]
            df.loc[no_sector_mask, "pred_5d"] = global_5d.predict(X_ns)
            df.loc[no_sector_mask, "pred_20d"] = global_20d.predict(X_ns)

        df["prediction_score"] = 0.6 * df["pred_5d"] + 0.4 * df["pred_20d"]
        if has_cutoff_col:
            df["model_cutoff"] = cutoff_str

        with engine.begin() as c:
            c.execute(text("DELETE FROM predictions WHERE DATE(date) = :d"), {"d": d})
        df[insert_cols].to_sql(
            "predictions", engine, if_exists="append", index=False,
        )
        del features, df
        gc.collect()
    else:
        n_done = len(pending) - skipped
        msg = f"Predictions updated for {n_done} date(s) (XGBoost)"
        if skipped:
            msg += f" — skipped {skipped} pre-walk-forward dates (would be leaks)"
        print(msg)
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
        _, cutoff_str = _models_for_date(d)
        features = pd.read_sql(
            text("SELECT * FROM features WHERE DATE(date) = :d AND ticker NOT LIKE '%.TA'"),
            engine, params={"d": d},
        )
        if features.empty:
            continue
        features["pred_5d"] = (
            features["rsi_factor"] * w["short_rsi_factor"]
            + features["bb_factor"] * w["short_bb_factor"]
            + features["macd_factor"] * w["short_macd_factor"]
            + features["trend_factor"] * w["short_trend_factor"]
        )
        features["pred_20d"] = (
            features["rsi_factor"] * w["med_rsi_factor"]
            + features["bb_factor"] * w["med_bb_factor"]
            + features["macd_factor"] * w["med_macd_factor"]
            + features["trend_factor"] * w["med_trend_factor"]
        )
        features["prediction_score"] = 0.6 * features["pred_5d"] + 0.4 * features["pred_20d"]
        if has_cutoff_col:
            features["model_cutoff"] = cutoff_str

        with engine.begin() as c:
            c.execute(text("DELETE FROM predictions WHERE DATE(date) = :d"), {"d": d})
        features[insert_cols].to_sql(
            "predictions", engine, if_exists="append", index=False,
        )
        del features
        gc.collect()

    print(f"Predictions updated for {len(pending)} date(s) (linear weights fallback)")
