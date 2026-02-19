import pandas as pd
from data.loaders import engine


def run_ranking():

    print("Running ranking engine...")

    signals = pd.read_sql("SELECT * FROM signals", engine)

    # ---------- keep only BUY candidates ----------
    ranked = signals[signals["signal"] == "BUY"].copy()

    if ranked.empty:
        print("No BUY signals to rank")
        ranked.to_sql("daily_ranked", engine,
                      if_exists="replace", index=False)
        return

    # ---------- rank by prediction score ----------
    ranked = ranked.sort_values(
        ["date", "score"],
        ascending=[True, False]
    )

    # ---------- create rank per day ----------
    ranked["rank"] = (
        ranked.groupby("date")["score"]
        .rank(method="first", ascending=False)
    )

    ranked.to_sql("daily_ranked", engine,
                  if_exists="replace", index=False)

    print("Ranking completed")
