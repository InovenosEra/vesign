import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, UTC
from utils.universe_loader import load_universe
from database.db_connection import engine
import pandas_market_calendars as mcal


def update_prices():

    print("Updating prices incrementally...")

    tickers = load_universe()

    today = datetime.now(UTC).date()

    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=today - timedelta(days=10), end_date=today)

    end_date = schedule.index[-1].date()

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
            if ticker not in data.columns.get_level_values(0):
                print(f"{ticker} download failed - retrying single download")
                retry = yf.download(
                    ticker,
                    start=start_date,
                    end=end_date,
                    auto_adjust=False,
                    progress=False
                )

                if retry is None or retry.empty:
                    print(f"{ticker} retry failed - skipping")
                    continue
                df = retry.copy()
            else:
                df = data[ticker].copy()

            if df is None or df.empty:
                print(f"{ticker} returned empty data - skipping")
                continue

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

        except Exception as e:
            print(f"{ticker} failed: {e}")
            continue

    if len(all_frames) == 0:
        print("No new data downloaded")
        return

    final_df = pd.concat(all_frames)

    final_df.drop_duplicates(subset=["date", "ticker"], inplace=True)

    # append instead of replace
    final_df.to_sql("daily_prices", engine, if_exists="append", index=False)

    print("Prices incrementally updated successfully")
