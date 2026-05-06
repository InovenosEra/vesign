"""Walk-forward training + prediction.

Quarterly cadence: at each Jan/Apr/Jul/Oct 1st cutoff, retrain a new set of
sector + global XGBoost models using `train_factor_weights(train_end_date=cutoff)`.
The trained pickles are stored under `ml_models/walk/{YYYY-MM-DD}/`.

For predicting any date d, we use the model from the LATEST cutoff <= d.
Concretely: predictions for [Q_n, Q_{n+1}) use models from cutoff Q_n.

This module is the only place predictions get written to predictions_walk.
The output rows carry the `model_cutoff` audit field so verify_no_leak.py can
prove that no row was predicted by a model trained on data >= the row's date.
"""
from __future__ import annotations

import os
import gc
import pickle
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from data.loaders import engine

ML_WALK_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / "ml_models" / "walk"
PREDICTIONS_TABLE = "predictions_walk"
FEATURE_COLS = ["rsi_factor", "bb_factor", "macd_factor", "trend_factor"]


class NoLeakError(AssertionError):
    pass


def quarterly_cutoffs(start: date, end: date) -> list[date]:
    """Return the list of quarter-start dates that fall within [start, end].

    A quarter-start is the 1st of Jan, Apr, Jul, Oct. The first cutoff is the
    smallest quarter-start >= `start`. The last cutoff is the largest
    quarter-start <= `end`.
    """
    if end < start:
        return []
    QUARTER_MONTHS = (1, 4, 7, 10)
    cuts: list[date] = []
    y = start.year
    while True:
        for m in QUARTER_MONTHS:
            d = date(y, m, 1)
            if d < start:
                continue
            if d > end:
                return cuts
            cuts.append(d)
        y += 1


def models_dir_for(cutoff: date) -> Path:
    return ML_WALK_DIR / cutoff.isoformat()


def train_for_cutoff(cutoff: date, force: bool = False) -> Path:
    """Train sector + global XGBoost models with train_end_date=cutoff.

    Saves pickles to ml_models/walk/{cutoff}/. If the directory already exists
    and force=False, returns the existing path without retraining.
    """
    dest = models_dir_for(cutoff)
    if dest.exists() and not force:
        return dest

    import models.train as train_mod
    original_dir = train_mod.ML_MODELS_DIR
    staging = ML_WALK_DIR / f"_staging_{cutoff.isoformat()}"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    train_mod.ML_MODELS_DIR = str(staging)
    try:
        train_mod.train_factor_weights(train_end_date=cutoff.isoformat())
    finally:
        train_mod.ML_MODELS_DIR = original_dir

    if dest.exists():
        shutil.rmtree(dest)
    staging.rename(dest)
    return dest


def _load_model(path: Path):
    if not path.exists():
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


def predict_period(cutoff: date, period_start: date, period_end_excl: date) -> int:
    """Predict for dates in [period_start, period_end_excl) using models at cutoff.

    Writes rows into PREDICTIONS_TABLE with model_cutoff=cutoff.isoformat().
    Returns the number of rows written. Idempotent per-date (deletes existing
    rows in the staging table for any date about to be re-predicted).
    """
    if cutoff > period_start:
        raise NoLeakError(
            f"Refusing to predict period [{period_start}, {period_end_excl}) "
            f"with cutoff {cutoff} > period_start {period_start}. Caller bug."
        )

    mdir = models_dir_for(cutoff)
    global_5d = _load_model(mdir / "global_5d.pkl")
    global_20d = _load_model(mdir / "global_20d.pkl")
    if global_5d is None or global_20d is None:
        raise FileNotFoundError(f"Global models missing for cutoff {cutoff}: {mdir}")

    companies = pd.read_sql("SELECT ticker, sector FROM companies", engine)
    sector_cache: dict = {}

    def _sector_models(sector):
        if sector in sector_cache:
            return sector_cache[sector]
        safe = str(sector).replace(" ", "_").replace("/", "_")
        m5 = _load_model(mdir / f"{safe}_5d.pkl") or global_5d
        m20 = _load_model(mdir / f"{safe}_20d.pkl") or global_20d
        sector_cache[sector] = (m5, m20)
        return m5, m20

    written = 0
    cur = period_start
    while cur < period_end_excl:
        feats = pd.read_sql(
            text("SELECT * FROM features WHERE DATE(date) = :d AND ticker NOT LIKE '%.TA'"),
            engine, params={"d": cur.isoformat()},
        )
        if feats.empty:
            cur = cur + timedelta(days=1)
            continue
        df = feats.merge(companies, on="ticker", how="left")
        valid = df[FEATURE_COLS].notna().all(axis=1)
        df["pred_5d"] = float("nan")
        df["pred_20d"] = float("nan")
        for sector, idx in df[valid].groupby("sector").groups.items():
            m5, m20 = _sector_models(sector)
            X = df.loc[idx, FEATURE_COLS]
            df.loc[idx, "pred_5d"] = m5.predict(X)
            df.loc[idx, "pred_20d"] = m20.predict(X)
        no_sec = valid & df["sector"].isna()
        if no_sec.any():
            X_ns = df.loc[no_sec, FEATURE_COLS]
            df.loc[no_sec, "pred_5d"] = global_5d.predict(X_ns)
            df.loc[no_sec, "pred_20d"] = global_20d.predict(X_ns)

        df["prediction_score"] = 0.6 * df["pred_5d"] + 0.4 * df["pred_20d"]
        df["model_cutoff"] = cutoff.isoformat()

        with engine.begin() as c:
            c.execute(text(f"DELETE FROM {PREDICTIONS_TABLE} WHERE DATE(date) = :d"), {"d": cur.isoformat()})
        df[["date", "ticker", "pred_5d", "pred_20d", "prediction_score", "model_cutoff"]].to_sql(
            PREDICTIONS_TABLE, engine, if_exists="append", index=False,
        )
        written += len(df)
        del feats, df
        gc.collect()
        cur = cur + timedelta(days=1)

    return written


def ensure_predictions_walk_table() -> None:
    """Create the staging table with the same shape as predictions plus model_cutoff."""
    with engine.begin() as c:
        c.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {PREDICTIONS_TABLE} (
                date TEXT, ticker TEXT,
                pred_5d FLOAT, pred_20d FLOAT, prediction_score FLOAT,
                model_cutoff TEXT NOT NULL
            )
        """))
        c.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{PREDICTIONS_TABLE}_date ON {PREDICTIONS_TABLE}(date)"))
        c.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{PREDICTIONS_TABLE}_ticker ON {PREDICTIONS_TABLE}(ticker)"))


def maybe_retrain_for_today() -> Optional[Path]:
    """For production: if today's quarter-start has no walk model yet, train one.

    Idempotent — safe to call daily. Returns the model dir for today's cutoff,
    or None if no retrain was needed.
    """
    today = date.today()
    qstart_month = ((today.month - 1) // 3) * 3 + 1
    cutoff = date(today.year, qstart_month, 1)
    dest = models_dir_for(cutoff)
    if dest.exists():
        return None
    return train_for_cutoff(cutoff)
