import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, UTC
from utils.universe_loader import load_universe
from database.db_connection import engine


def update_prices():

    print("Updating prices incrementally...")

    tickers = load_universe()

    end_date = datetime.now(UTC).date()

    # ---------- detect last stored date ----------
    try:
        existing = pd.read_sql("SELECT MAX(date) as last_date FROM daily_prices", engine)
        last_date = pd.to_datetime(existing["last_date"][0]).date()

        # start from next day
        start_date = last_date + timedelta(days=1)

    except Exception:
        # first run
        start_date = end_date - timedelta(days=3 * 365)

    if start_date >= end_date:
        print("Database already up to date")
        return

    print(f"Downloading missing data from {start_date} to {end_date}")

    data = yf.download(
        tickers,
        start=start_date,
        end=end_date,
        group_by="ticker",
        auto_adjust=False,
        progress=True
    )

    all_frames = []

    for ticker in tickers:

        try:
            df = data[ticker].copy()
            df.reset_index(inplace=True)

            df["ticker"] = ticker

            df.rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            }, inplace=True)

            # remove today's incomplete bar
            today = pd.Timestamp(datetime.now(UTC).date())
            df = df[df["date"] < today]

            all_frames.append(df)

        except Exception:
            continue

    if len(all_frames) == 0:
        print("No new data downloaded")
        return

    final_df = pd.concat(all_frames)

    # append instead of replace
    final_df.to_sql("daily_prices", engine, if_exists="append", index=False)

    print("Prices incrementally updated successfully")
