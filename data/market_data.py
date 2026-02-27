import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, UTC
from utils.universe_loader import load_universe
from data.loaders import engine
from sqlalchemy import text
import pandas_market_calendars as mcal
from utils.update_guard import should_run, mark_run


def update_prices():

    print("Updating prices incrementally...")

    tickers = load_universe()

    today = datetime.now(UTC).date()

    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=today - timedelta(days=10), end_date=today)

    end_date = schedule.index[-1].date()

    # ---------- detect last stored date ----------
    # Use MIN(MAX(date) per ticker) so that if any single ticker has more
    # recent data than the rest (e.g. a manually added custom ticker), the
    # other tickers still get updated rather than being silently skipped.
    try:
        existing = pd.read_sql(
            "SELECT MIN(max_date) as last_date FROM "
            "(SELECT ticker, MAX(date) as max_date FROM daily_prices GROUP BY ticker)",
            engine
        )
        last_date = pd.to_datetime(existing["last_date"][0]).date()

        start_date = last_date + timedelta(days=1)

    except Exception:
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

    # Delete any existing rows for the same date/ticker before inserting
    # (guards against re-runs on the same day producing duplicates or
    # overwriting NULL-close rows that were inserted before market open)
    dates = final_df["date"].dt.strftime("%Y-%m-%d").unique().tolist()
    if dates:
        placeholders = ",".join(f"'{d}'" for d in dates)
        with engine.begin() as conn:
            conn.execute(text(
                f"DELETE FROM daily_prices WHERE date(date) IN ({placeholders})"
            ))

    final_df.to_sql("daily_prices", engine, if_exists="append", index=False)

    print("Prices incrementally updated successfully")





def update_vix():

    print("Updating VIX data incrementally...")

    try:
        existing = pd.read_sql("SELECT MAX(date) as last_date FROM vix", engine)
        last_date = pd.to_datetime(existing["last_date"][0]).date()
        start_date = last_date + timedelta(days=1)
    except Exception:
        start_date = datetime.now(UTC).date() - timedelta(days=3 * 365)

    today = datetime.now(UTC).date()

    if start_date >= today:
        print("VIX already up to date")
        return

    print(f"Downloading VIX from {start_date} to {today}")

    try:
        data = yf.download(
            "^VIX",
            start=start_date,
            end=today,
            auto_adjust=False,
            progress=False
        )

        if data is None or data.empty:
            print("VIX download returned empty data")
            return

        data.reset_index(inplace=True)

        # Handle MultiIndex columns from yfinance
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [col[0] if col[1] == "" else col[0] for col in data.columns]

        data.rename(columns={"Date": "date", "Close": "close"}, inplace=True)
        data = data[["date", "close"]].dropna()

        today_ts = pd.Timestamp(datetime.now(UTC).date())
        data = data[data["date"] < today_ts]

        data.drop_duplicates(subset=["date"], inplace=True)
        data.to_sql("vix", engine, if_exists="append", index=False)
        print("VIX updated successfully")

    except Exception as e:
        print(f"VIX update failed: {e}")


def update_fundamentals():

    # ---------- run only if needed ----------
    if not should_run("fundamentals_update", 24):
        return

    print("Updating fundamentals...")

    tickers = pd.read_sql(
        "SELECT ticker FROM companies",
        engine
    )["ticker"].tolist()

    rows = []

    for t in tickers:
        try:
            info = yf.Ticker(t).info

            rows.append({
                "ticker": t,
                "market_cap": info.get("marketCap")
            })

        except Exception:
            continue

    if len(rows) == 0:
        print("No fundamentals downloaded")
        return

    df = pd.DataFrame(rows)

    df.to_sql(
        "fundamentals",
        engine,
        if_exists="replace",
        index=False
    )

    mark_run("fundamentals_update")

    print("Fundamentals updated successfully")
