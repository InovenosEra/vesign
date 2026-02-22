import pandas as pd
from sklearn.linear_model import LinearRegression
from data.loaders import engine


def train_factor_weights(train_end_date=None):
    """
    Train linear regression weights for multi-horizon return prediction.

    Parameters
    ----------
    train_end_date : str or None
        Exclusive upper bound for training data (format "YYYY-MM-DD").
        Pass this in the research pipeline to prevent the model from
        seeing data from the evaluation period (look-ahead bias).
        Defaults to None, which uses all available data (production mode).
    """

    print("Training rolling factor weights (multi-horizon)...")

    features = pd.read_sql("SELECT * FROM features", engine)
    fwd = pd.read_sql("SELECT date,ticker,fwd_5d,fwd_20d FROM forward_returns", engine)

    df = features.merge(fwd, on=["date", "ticker"]).dropna()
    df["date"] = pd.to_datetime(df["date"])

    if train_end_date is not None:
        train_end_date = pd.Timestamp(train_end_date)
        df = df[df["date"] < train_end_date]
        print(f"Training on data before {train_end_date.date()} (out-of-sample guard)")

    cutoff = df["date"].max() - pd.Timedelta(days=730)
    train_df = df[df["date"] >= cutoff]

    X = train_df[["rsi_factor", "bb_factor", "macd_factor", "trend_factor"]]

    # ---------- Short horizon ----------
    model_short = LinearRegression()
    model_short.fit(X, train_df["fwd_5d"])
    short_weights = dict(zip(X.columns, model_short.coef_))

    # ---------- Medium horizon ----------
    model_med = LinearRegression()
    model_med.fit(X, train_df["fwd_20d"])
    med_weights = dict(zip(X.columns, model_med.coef_))

    weights_df = pd.DataFrame([{
        **{f"short_{k}": v for k, v in short_weights.items()},
        **{f"med_{k}": v for k, v in med_weights.items()}
    }])

    weights_df.to_sql("factor_weights", engine,
                      if_exists="replace", index=False)

    print("Multi-horizon weights trained")
