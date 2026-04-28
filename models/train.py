import os
import pickle
import pandas as pd
from sklearn.linear_model import LinearRegression
from data.loaders import engine

FEATURE_COLS = ["rsi_factor", "bb_factor", "macd_factor", "trend_factor"]
ML_MODELS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ml_models")


def _save_pkl(model, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(model, f)


def train_factor_weights(train_end_date=None):
    """
    Train per-sector XGBoost models for multi-horizon return prediction.

    For each sector with >= 30 samples, trains an XGBRegressor for fwd_5d and
    fwd_20d. Sectors with < 30 samples fall back to a global model trained on
    all data. Pickles are saved to ml_models/{sector}_5d.pkl etc.

    Also writes the factor_weights table as a backward-compatible fallback.

    Parameters
    ----------
    train_end_date : str or None
        Exclusive upper bound for training data (format "YYYY-MM-DD").
    """

    try:
        from xgboost import XGBRegressor
        use_xgb = True
    except ImportError:
        print("xgboost not installed, falling back to LinearRegression")
        use_xgb = False

    print("Training factor weights (per-sector XGBoost)..." if use_xgb
          else "Training rolling factor weights (multi-horizon)...")

    # Memory: filter at SQL level. Loading the full features table (2.3M rows)
    # OOM-killed the daily pipeline on a 2GB server (Apr 24-28 2026). Training
    # only uses the last 730 days anyway — load 800 days for a small buffer.
    if train_end_date is not None:
        train_end_ts = pd.Timestamp(train_end_date)
    else:
        train_end_ts = pd.Timestamp.utcnow().normalize() + pd.Timedelta(days=1)
    sql_start = (train_end_ts - pd.Timedelta(days=800)).strftime("%Y-%m-%d")
    sql_end = train_end_ts.strftime("%Y-%m-%d")

    features = pd.read_sql(
        f"SELECT * FROM features WHERE date >= '{sql_start}' AND date < '{sql_end}'",
        engine,
    )
    fwd = pd.read_sql(
        f"SELECT date,ticker,fwd_5d,fwd_20d FROM forward_returns "
        f"WHERE date >= '{sql_start}' AND date < '{sql_end}'",
        engine,
    )

    # Load sector info
    companies = pd.read_sql("SELECT ticker, sector FROM companies", engine)

    df = features.merge(fwd, on=["date", "ticker"]).dropna(subset=FEATURE_COLS + ["fwd_5d", "fwd_20d"])
    del features, fwd
    df = df.merge(companies, on="ticker", how="left")
    df["date"] = pd.to_datetime(df["date"])

    if train_end_date is not None:
        print(f"Training on data before {train_end_ts.date()} (out-of-sample guard)")

    cutoff = df["date"].max() - pd.Timedelta(days=730)
    train_df = df[df["date"] >= cutoff].copy()
    del df

    X_all = train_df[FEATURE_COLS]

    # ---------- Global fallback models ----------
    if use_xgb:
        global_5d = XGBRegressor(n_estimators=100, max_depth=3, learning_rate=0.1,
                                  random_state=42, verbosity=0)
        global_20d = XGBRegressor(n_estimators=100, max_depth=3, learning_rate=0.1,
                                   random_state=42, verbosity=0)
    else:
        global_5d = LinearRegression()
        global_20d = LinearRegression()

    global_5d.fit(X_all, train_df["fwd_5d"])
    global_20d.fit(X_all, train_df["fwd_20d"])

    _save_pkl(global_5d, os.path.join(ML_MODELS_DIR, "global_5d.pkl"))
    _save_pkl(global_20d, os.path.join(ML_MODELS_DIR, "global_20d.pkl"))
    print(f"Global models saved ({len(train_df)} samples)")

    # ---------- Per-sector models ----------
    MIN_SAMPLES = 30
    sectors = train_df["sector"].dropna().unique()

    for sector in sectors:
        sector_df = train_df[train_df["sector"] == sector]
        if len(sector_df) < MIN_SAMPLES:
            print(f"  {sector}: {len(sector_df)} samples < {MIN_SAMPLES}, using global model")
            continue

        X_sec = sector_df[FEATURE_COLS]

        if use_xgb:
            m5 = XGBRegressor(n_estimators=100, max_depth=3, learning_rate=0.1,
                               random_state=42, verbosity=0)
            m20 = XGBRegressor(n_estimators=100, max_depth=3, learning_rate=0.1,
                                random_state=42, verbosity=0)
        else:
            m5 = LinearRegression()
            m20 = LinearRegression()

        m5.fit(X_sec, sector_df["fwd_5d"])
        m20.fit(X_sec, sector_df["fwd_20d"])

        safe_sector = sector.replace(" ", "_").replace("/", "_")
        _save_pkl(m5,  os.path.join(ML_MODELS_DIR, f"{safe_sector}_5d.pkl"))
        _save_pkl(m20, os.path.join(ML_MODELS_DIR, f"{safe_sector}_20d.pkl"))
        print(f"  {sector}: {len(sector_df)} samples — sector model saved")

    # ---------- Backward-compat: write factor_weights table ----------
    model_short = LinearRegression()
    model_short.fit(X_all, train_df["fwd_5d"])
    model_med = LinearRegression()
    model_med.fit(X_all, train_df["fwd_20d"])

    short_weights = dict(zip(FEATURE_COLS, model_short.coef_))
    med_weights = dict(zip(FEATURE_COLS, model_med.coef_))

    weights_df = pd.DataFrame([{
        **{f"short_{k}": v for k, v in short_weights.items()},
        **{f"med_{k}": v for k, v in med_weights.items()}
    }])

    weights_df.to_sql("factor_weights", engine, if_exists="replace", index=False)

    print("Multi-horizon weights trained")
