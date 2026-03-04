import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, UTC
from data.loaders import engine
from utils.update_guard import should_run, mark_run


def update_analyst_data():

    # ---------- run only once per day ----------
    if not should_run("analyst_update", 24):
        return

    print("Downloading analyst expectations...")

    tickers = pd.read_sql(
        "SELECT ticker FROM companies",
        engine
    )["ticker"].tolist()

    now = datetime.now(UTC)

    def _fetch(t):
        try:
            info = yf.Ticker(t).info
            return {
                "ticker": t,
                "target_mean_price": info.get("targetMeanPrice"),
                "target_high_price": info.get("targetHighPrice"),
                "target_low_price": info.get("targetLowPrice"),
                "number_of_analysts": info.get("numberOfAnalystOpinions"),
                "last_update": now,
            }
        except Exception:
            return None

    rows = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch, t): t for t in tickers}
        done = 0
        for f in as_completed(futures):
            done += 1
            result = f.result()
            if result:
                rows.append(result)
            if done % 100 == 0:
                print(f"  Analyst: {done}/{len(tickers)} done, {len(rows)} fetched")

    if len(rows) == 0:
        print("No analyst data downloaded")
        return

    df = pd.DataFrame(rows)
    df.to_sql("analyst_expectations", engine, if_exists="replace", index=False)

    mark_run("analyst_update")

    print(f"Analyst expectations updated successfully ({len(rows)} tickers)")
