import pandas as pd
from data.loaders import engine


def run_prediction_engine():

    print("Running prediction score engine...")

    features = pd.read_sql("SELECT * FROM features", engine)
    weights = pd.read_sql("SELECT * FROM factor_weights", engine)

    if weights.empty:
        print("No trained weights found")
        return

    w = weights.iloc[0]

    # ---------- Short horizon prediction ----------
    features["pred_5d"] = (
        features["rsi_factor"]   * w["short_rsi_factor"] +
        features["bb_factor"]    * w["short_bb_factor"] +
        features["macd_factor"]  * w["short_macd_factor"] +
        features["trend_factor"] * w["short_trend_factor"]
    )

    # ---------- Medium horizon prediction ----------
    features["pred_20d"] = (
        features["rsi_factor"]   * w["med_rsi_factor"] +
        features["bb_factor"]    * w["med_bb_factor"] +
        features["macd_factor"]  * w["med_macd_factor"] +
        features["trend_factor"] * w["med_trend_factor"]
    )

    # ---------- Combined prediction score ----------
    features["prediction_score"] = (
        0.6 * features["pred_5d"] +
        0.4 * features["pred_20d"]
    )

    features[[
        "date",
        "ticker",
        "pred_5d",
        "pred_20d",
        "prediction_score"
    ]].to_sql("predictions", engine,
              if_exists="replace", index=False)

    print("Predictions table updated")
