import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, UTC
from utils.universe_loader import load_universe
from data.loaders import engine
from sqlalchemy import text
import pandas_market_calendars as mcal
from utils.update_guard import should_run, mark_run

# Batch size for backfilling large numbers of new tickers (avoids memory/timeout issues)
_BACKFILL_BATCH = 200


def _build_ticker_df(raw, ticker, start_date, end_date, single=False):
    """
    Extract and clean one ticker's DataFrame from a yfinance download result.
    Returns a clean DataFrame or None if the ticker had no usable data.
    """
    try:
        df = raw.copy() if single else raw[ticker].copy()
    except (KeyError, TypeError):
        return None

    if df is None or df.empty:
        return None

    # Flatten MultiIndex columns (yfinance returns these for single-ticker downloads too)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    df = df.reset_index()
    df["ticker"] = ticker
    df.rename(columns={
        "Date": "date", "Open": "open", "High": "high",
        "Low": "low", "Close": "close", "Volume": "volume",
    }, inplace=True)

    today_ts = pd.Timestamp(datetime.now(UTC).date())
    df = df[df["date"] < today_ts]

    # Keep only the standard columns that exist
    keep = [c for c in ("date", "ticker", "open", "high", "low", "close", "volume") if c in df.columns]
    return df[keep]


def _download_and_save(tickers: list, start_date, end_date, batch_size: int = 0):
    """
    Download price data for *tickers* between *start_date* and *end_date*,
    then upsert into daily_prices.

    If batch_size > 0 the list is split into chunks of that size (used for
    large backfills to avoid memory / rate-limit issues).
    """
    if not tickers:
        return

    batches = (
        [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
        if batch_size > 0
        else [tickers]
    )

    all_frames = []

    for b_idx, batch in enumerate(batches):
        if len(batches) > 1:
            print(f"  Batch {b_idx + 1}/{len(batches)} ({len(batch)} tickers)…")

        query = batch[0] if len(batch) == 1 else batch
        try:
            data = yf.download(
                query,
                start=start_date,
                end=end_date,
                group_by="ticker",
                auto_adjust=False,
                progress=len(batches) == 1,   # show bar only for single large batch
            )
        except Exception as e:
            print(f"  Batch download failed: {e}")
            continue

        single = len(batch) == 1

        for ticker in batch:
            # Try extracting from the bulk download first
            df = _build_ticker_df(data, ticker, start_date, end_date, single=single)

            # If missing from bulk result, retry individually
            if df is None and not single:
                try:
                    retry = yf.download(
                        ticker, start=start_date, end=end_date,
                        auto_adjust=False, progress=False,
                    )
                    df = _build_ticker_df(retry, ticker, start_date, end_date, single=True)
                except Exception:
                    pass

            if df is not None and not df.empty:
                all_frames.append(df)

    if not all_frames:
        print("  No data downloaded.")
        return

    final_df = pd.concat(all_frames, ignore_index=True)
    final_df.drop_duplicates(subset=["date", "ticker"], inplace=True)

    # Delete any conflicting rows for the same dates before inserting
    dates = final_df["date"].dt.strftime("%Y-%m-%d").unique().tolist()
    if dates:
        placeholders = ",".join(f"'{d}'" for d in dates)
        with engine.begin() as conn:
            conn.execute(text(
                f"DELETE FROM daily_prices WHERE date(date) IN ({placeholders})"
            ))

    final_df.to_sql("daily_prices", engine, if_exists="append", index=False)
    print(f"  Saved {len(final_df):,} rows for {final_df['ticker'].nunique():,} tickers.")


def update_prices():

    print("Updating prices…")

    tickers = load_universe()

    today    = datetime.now(UTC).date()
    nyse     = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=today - timedelta(days=10), end_date=today)
    end_date = schedule.index[-1].date()

    # ── Step 1: backfill any tickers that have never been downloaded ──────────
    # New tickers (e.g. newly added NASDAQ stocks) won't appear in daily_prices
    # at all, so the MIN(MAX(date)) logic below would only give them a few days
    # of data.  We backfill them separately with 3 years of history first.
    try:
        initialized = set(
            pd.read_sql("SELECT DISTINCT ticker FROM daily_prices", engine)["ticker"].tolist()
        )
    except Exception:
        initialized = set()

    new_tickers = [t for t in tickers if t not in initialized]

    if new_tickers:
        backfill_start = end_date - timedelta(days=3 * 365)
        print(f"Backfilling {len(new_tickers):,} new tickers from {backfill_start} to {end_date}…")
        _download_and_save(new_tickers, backfill_start, end_date, batch_size=_BACKFILL_BATCH)

    # ── Step 2: incremental update for all tickers ────────────────────────────
    # Use MIN(MAX(date) per ticker) so that even if one ticker is slightly ahead,
    # the others still get refreshed.
    try:
        existing = pd.read_sql(
            "SELECT MIN(max_date) as last_date FROM "
            "(SELECT ticker, MAX(date) as max_date FROM daily_prices GROUP BY ticker)",
            engine
        )
        last_date  = pd.to_datetime(existing["last_date"][0]).date()
        start_date = last_date + timedelta(days=1)
    except Exception:
        start_date = end_date - timedelta(days=3 * 365)

    if start_date >= end_date:
        print("Database already up to date.")
        return

    print(f"Incremental update: {start_date} → {end_date} ({len(tickers):,} tickers)")
    _download_and_save(tickers, start_date, end_date)

    print("Prices updated successfully.")


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
