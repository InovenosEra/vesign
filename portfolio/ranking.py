import pandas as pd
from data.loaders import engine


def run_ranking():

    print("Running ranking engine...")

    ranked = pd.read_sql("SELECT * FROM signals WHERE signal='BUY'", engine)

    if ranked.empty:
        print("No BUY signals to rank")
        return

    # ---------- rank by ML prediction score ----------
    # Use prediction_score (ML model output) when available.
    # Fall back to RSI-based score for rows where predictions are missing
    # (e.g. model not yet trained, or ticker not in predictions table).
    if "prediction_score" in ranked.columns:
        ranked["_rank_score"] = ranked["prediction_score"].fillna(ranked["score"])
    else:
        ranked["_rank_score"] = ranked["score"]

    ranked = ranked.sort_values(
        ["date", "_rank_score"],
        ascending=[True, False]
    )

    # ---------- create rank per day ----------
    ranked["rank"] = (
        ranked.groupby("date")["_rank_score"]
        .rank(method="first", ascending=False)
    )

    ranked = ranked.drop(columns=["_rank_score"])

    ranked.to_sql("daily_ranked", engine,
                  if_exists="replace", index=False)

    print("Ranking completed")
