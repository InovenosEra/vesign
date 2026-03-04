import os
import yfinance as yf
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    df = df[keep]

    # Drop rows with no close price — these are non-trading days or incomplete data
    # that would corrupt feature computation downstream.
    if "close" in df.columns:
        df = df[df["close"].notna()]

    return df if not df.empty else None


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

    # Delete only the rows for the specific tickers being saved (not all tickers on those dates,
    # which would wipe unrelated markets' price history when doing a partial backfill).
    dates = final_df["date"].dt.strftime("%Y-%m-%d").unique().tolist()
    batch_tickers = final_df["ticker"].unique().tolist()
    if dates and batch_tickers:
        date_placeholders   = ",".join(f"'{d}'" for d in dates)
        ticker_placeholders = "','".join(batch_tickers)
        with engine.begin() as conn:
            conn.execute(text(
                f"DELETE FROM daily_prices "
                f"WHERE date(date) IN ({date_placeholders}) "
                f"AND ticker IN ('{ticker_placeholders}')"
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


def update_company_info():
    """Fetch fundamentals + analyst targets in a single parallel pass (one .info call per ticker)."""

    needs_fundamentals = should_run("fundamentals_update", 168)   # weekly
    needs_analyst      = should_run("analyst_update", 24)         # daily

    if not needs_fundamentals and not needs_analyst:
        return

    print("Updating company info (fundamentals + analyst)...")

    tickers = pd.read_sql("SELECT ticker FROM companies", engine)["ticker"].tolist()
    now = datetime.now(UTC)

    def _fetch(t):
        try:
            info = yf.Ticker(t).info
            return {
                "ticker":               t,
                "market_cap":           info.get("marketCap"),
                "industry":             info.get("industry"),
                "description":          info.get("longBusinessSummary"),
                "target_mean_price":    info.get("targetMeanPrice"),
                "target_high_price":    info.get("targetHighPrice"),
                "target_low_price":     info.get("targetLowPrice"),
                "number_of_analysts":   info.get("numberOfAnalystOpinions"),
                "last_update":          now,
            }
        except Exception:
            return None

    # Warm up the yfinance session (crumb) before threading to avoid race conditions
    try:
        yf.Ticker(tickers[0]).info
    except Exception:
        pass

    rows = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(_fetch, t): t for t in tickers}
        done = 0
        for f in as_completed(futures):
            done += 1
            result = f.result()
            if result:
                rows.append(result)
            if done % 200 == 0:
                print(f"  {done}/{len(tickers)} done, {len(rows)} fetched")

    if not rows:
        print("No company info downloaded")
        return

    df = pd.DataFrame(rows)

    if needs_fundamentals:
        fund_df = df[["ticker", "market_cap"]]
        fetched = fund_df["ticker"].tolist()
        with engine.begin() as conn:
            placeholders = ",".join(f"'{t}'" for t in fetched)
            conn.execute(text(f"DELETE FROM fundamentals WHERE ticker IN ({placeholders})"))
        fund_df.to_sql("fundamentals", engine, if_exists="append", index=False)
        # Also update industry + description in companies table
        with engine.begin() as conn:
            for _, row in df[["ticker", "industry", "description"]].iterrows():
                if row["industry"] or row["description"]:
                    conn.execute(text(
                        "UPDATE companies SET industry = :ind, description = :desc WHERE ticker = :t"
                    ), {"ind": row["industry"], "desc": row["description"], "t": row["ticker"]})
        mark_run("fundamentals_update")

    if needs_analyst:
        analyst_df = df[["ticker", "target_mean_price", "target_high_price",
                          "target_low_price", "number_of_analysts", "last_update"]]
        fetched = analyst_df["ticker"].tolist()
        with engine.begin() as conn:
            placeholders = ",".join(f"'{t}'" for t in fetched)
            conn.execute(text(f"DELETE FROM analyst_expectations WHERE ticker IN ({placeholders})"))
        analyst_df.to_sql("analyst_expectations", engine, if_exists="append", index=False)
        mark_run("analyst_update")

    print(f"Company info updated ({len(rows)} tickers)")


def update_company_health():
    """Score company financial health (1-5) using yfinance fundamentals + news via Claude Haiku.
    Runs weekly; only re-scores tickers whose score is missing or older than 7 days.
    """
    import json as _json
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set — skipping health scoring.")
        return

    try:
        import anthropic
    except ImportError:
        print("anthropic package not installed — skipping.")
        return

    all_tickers = pd.read_sql(
        "SELECT ticker FROM companies", engine
    )["ticker"].tolist()

    try:
        existing = pd.read_sql("SELECT ticker, last_update FROM company_health", engine)
        existing["last_update"] = pd.to_datetime(existing["last_update"])
        cutoff = pd.Timestamp.now() - pd.Timedelta(days=7)
        fresh = set(existing[existing["last_update"] >= cutoff]["ticker"].tolist())
    except Exception:
        fresh = set()

    pending = [t for t in all_tickers if t not in fresh]
    if not pending:
        print("Company health scores are up to date.")
        return

    print(f"Scoring health for {len(pending)} companies…")
    client = anthropic.Anthropic(api_key=api_key)
    now = datetime.now(UTC)

    _SYSTEM = (
        "You are a strict financial analyst rating company health on a FULL 1-5 scale. "
        "Use the ENTIRE range — do NOT cluster scores around 2-3.\n\n"
        "Scale definition (use each level freely):\n"
        "  1 = Weak:      Negative or near-zero margins, heavy debt load, negative/weak cash flow, "
        "shrinking revenue, or near-distress signals.\n"
        "  2 = Fair:      Below-average profitability, elevated leverage, modest or inconsistent cash flow, "
        "slow/flat growth. Survivable but uninspiring.\n"
        "  3 = Good:      Solid, average performance for the industry. Profitable, manageable debt, "
        "positive cash flow, stable growth.\n"
        "  4 = Great:     Above-average margins, strong free cash flow, low-to-moderate debt, "
        "healthy revenue/earnings growth. Financially sound.\n"
        "  5 = Excellent: Exceptional across ALL metrics — industry-leading margins, minimal debt, "
        "strong growing free cash flow, consistent double-digit growth.\n\n"
        "Rules:\n"
        "- If debtToEquity > 200 or profitMargins < 0, lean toward 1-2.\n"
        "- If freeCashflow < 0 and revenueGrowth < 0, that is a 1 or 2.\n"
        "- If profitMargins > 0.20 and debtToEquity < 50 and revenueGrowth > 0.10, lean toward 4-5.\n"
        "- Score 5 requires excellence in ALL dimensions simultaneously.\n"
        "- Context matters: benchmark within the company's industry.\n\n"
        "Respond with ONLY valid JSON: {\"score\": <integer 1-5>, \"reason\": \"<one concise sentence>\"}"
    )

    def _score(ticker):
        try:
            info = yf.Ticker(ticker).info
            metrics = {k: info.get(k) for k in (
                "profitMargins", "operatingMargins", "grossMargins",
                "returnOnEquity", "returnOnAssets",
                "currentRatio", "quickRatio",
                "debtToEquity", "totalDebt", "totalCash",
                "freeCashflow", "operatingCashflow",
                "revenueGrowth", "earningsGrowth",
            )}
            metrics = {k: v for k, v in metrics.items() if v is not None}

            try:
                headlines = [n["title"] for n in (yf.Ticker(ticker).news or [])[:5] if n.get("title")]
            except Exception:
                headlines = []

            prompt = (
                f"Company: {info.get('longName') or ticker} ({ticker})\n"
                f"Industry: {info.get('industry') or 'Unknown'}\n\n"
            )
            if metrics:
                prompt += "Financial metrics:\n"
                for k, v in metrics.items():
                    prompt += f"  {k}: {round(v, 4) if isinstance(v, float) else f'{v:,}'}\n"
            if headlines:
                prompt += "\nRecent news:\n" + "".join(f"  - {h}\n" for h in headlines)

            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=120,
                system=_SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = msg.content[0].text.strip()
            # Strip markdown code block if present
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            result = _json.loads(raw.strip())
            return {
                "ticker":      ticker,
                "score":       max(1, min(5, int(result["score"]))),
                "reason":      result["reason"],
                "last_update": now,
            }
        except Exception:
            return None

    # Warmup crumb before threading
    try:
        yf.Ticker(pending[0]).info
    except Exception:
        pass

    done = 0
    success = 0
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(_score, t): t for t in pending}
        for f in as_completed(futures):
            done += 1
            r = f.result()
            if r:
                success += 1
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT OR REPLACE INTO company_health (ticker, score, reason, last_update)
                        VALUES (:ticker, :score, :reason, :last_update)
                    """), r)
            if done % 100 == 0:
                print(f"  {done}/{len(pending)} scored ({success} successful)")

    print(f"Health scoring complete: {success}/{len(pending)} scored.")


def summarize_descriptions():
    """Use Claude Haiku to generate short (2-sentence) summaries for company descriptions.
    Only processes rows that have a raw description but no summary yet.
    """
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set — skipping description summarization.")
        return

    try:
        import anthropic
    except ImportError:
        print("anthropic package not installed — skipping.")
        return

    pending = pd.read_sql(
        "SELECT ticker, description FROM companies "
        "WHERE description IS NOT NULL AND description != '' "
        "AND (description_short IS NULL OR description_short = '')",
        engine
    )

    if pending.empty:
        print("All descriptions already summarized.")
        return

    print(f"Summarizing {len(pending)} company descriptions…")
    client = anthropic.Anthropic(api_key=api_key)

    updated = 0
    for _, row in pending.iterrows():
        try:
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=80,
                messages=[{
                    "role": "user",
                    "content": (
                        f"Summarize the following company description in exactly 1-2 short sentences. "
                        f"Be factual and concise. No fluff.\n\n{row['description']}"
                    ),
                }],
            )
            summary = msg.content[0].text.strip()
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE companies SET description_short = :s WHERE ticker = :t"
                ), {"s": summary, "t": row["ticker"]})
            updated += 1
        except Exception as e:
            print(f"  {row['ticker']}: {e}")

    print(f"Summarized {updated}/{len(pending)} descriptions.")
